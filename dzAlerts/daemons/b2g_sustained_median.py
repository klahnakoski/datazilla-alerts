# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import unicode_literals
from datetime import datetime, timedelta
from dzAlerts.daemons.util import significant_difference

from dzAlerts.util.collections import MIN, MAX
from dzAlerts.util.env.elasticsearch import ElasticSearch
from dzAlerts.util.env.files import File
from dzAlerts.util.queries.es_query import ESQuery
from dzAlerts.util.env import startup
from dzAlerts.util.queries.db_query import DBQuery, esfilter2sqlwhere
from dzAlerts.daemons.util.median_test import median_test
from dzAlerts.util.cnv import CNV
from dzAlerts.util.maths import Math
from dzAlerts.util.queries import windows
from dzAlerts.util.struct import nvl
from dzAlerts.util.sql.db import SQL
from dzAlerts.util.env.logs import Log
from dzAlerts.util.struct import Struct
from dzAlerts.util.queries import Q
from dzAlerts.util.sql.db import DB


SEVERITY = 0.8              # THERE ARE MANY FALSE POSITIVES (0.99 == positive indicator, 0.5==not an indicator, 0.01 == negative indicator)
# MIN_CONFIDENCE = 0.9999
REASON = "b2g_alert_sustained_median"     # name of the reason in alert_reason
MAX_AGE = timedelta(days=90)

TEMPLATE = """<div><h3>{{score}} - {{reason}}</h3><br>
On page {{page_url}}<br>
<a href=\"https://tbpl.mozilla.org/?tree={{branch}}&rev={{revision}}\">TBPL</a><br>
<a href=\"https://hg.mozilla.org/rev/{{revision}}\">Mercurial</a><br>
<a href=\"https://bugzilla.mozilla.org/show_bug.cgi?id={{bug_id}}\">Bugzilla - {{bug_description}}</a><br>
<a href=\"https://datazilla.mozilla.org/?start={{push_date_min}}&stop={{push_date_max}}&product={{product}}&repository={{branch}}&os={{operating_system_name}}&os_version={{operating_system_version}}&test={{test_name}}&graph_search={{revision}}&error_bars=false&project=talos\">Datazilla</a><br>
<a href=\"http://people.mozilla.com/~klahnakoski/test/es/DZ-ShowPage.html#page={{page_url}}&sampleMax={{push_date}}000&sampleMin={{push_date_min}}000&branch={{branch}}\">Kyle's ES</a><br>
Raw data:  {{details}}
</div>"""

DEBUG = True


def alert_sustained_median(settings, qb, alerts_db):
    """
    find single points that deviate from the trend
    """
    # OBJECTSTORE = settings.objectstore.schema + ".objectstore"
    # TDAD = settings.perftest.schema + ".test_data_all_dimensions"
    TDAD = settings.query["from"]
    PUSH_DATE = "datazilla.date_loaded"

    debug = nvl(settings.param.debug, DEBUG)

    query = settings.query

    new_test_points = qb.query({
        "from": TDAD,
        "select": {"name": "min_push_date", "value": PUSH_DATE, "aggregate": "min"},
        "edges": query.edges,
        "where": {"and": [
            {"missing": {"field": "processed_sustained_median"}}
            #FOR DEBUGGING SPECIFIC SERIES
            # {"term": {"test_machine.type": "hamachi"}},
            # {"term": {"testrun.suite": "browser"}},
            # {"term": {"result.test_name": "fps"}}
        ]}
    })

    #BRING IN ALL NEEDED DATA
    if debug:
        Log.note("Pull all data for {{num}} groups:\n{{groups.name}}", {
            "num": len(new_test_points),
            "groups": query.edges
        })

    # all_min_date = Null
    all_touched = set()
    re_alert = set()
    alerts = []   # PUT ALL THE EXCEPTION ITEMS HERE
    for g, test_points in Q.groupby(new_test_points, query.edges):
        if not test_points.min_push_date:
            continue
        try:
            first_sample = MAX(MIN(test_points.min_push_date), CNV.datetime2milli(datetime.utcnow() - MAX_AGE))
            # FOR THIS g, HOW FAR BACK IN TIME MUST WE GO TO COVER OUR WINDOW_SIZE?
            first_in_window = qb.query({
                "select": {"name": "min_date", "value": "push_date", "aggregate": "min"},
                "from": {
                    "from": TDAD,
                    "select": {"name": "push_date", "value": PUSH_DATE},
                    "where": {"and": [
                        {"term": g},
                        {"range": {PUSH_DATE: {"lt": first_sample}}}
                    ]},
                    "sort": {"field": PUSH_DATE, "sort": -1},
                    "limit": settings.param.sustained_median.window_size + 1
                }})

            min_date = MIN(first_sample, first_in_window.min_date)

            #LOAD TEST RESULTS FROM DATABASE
            test_results = qb.query({
                "from": {
                    "from": "b2g_alerts",
                    "select": [{"name": "push_date", "value": PUSH_DATE}] +
                              query.select +
                              query.edges,
                    "where": {"and": [
                        {"term": g},
                        {"range": {PUSH_DATE: {"gte": min_date}}}
                    ]},
                },
                "sort": "push_date"
            })

            Log.note("{{num}} test results found for\n{{group}}", {
                "num": len(test_results),
                "group": g
            })

            # if g.test_name not in ALLOWED_TESTS:
            #     if debug:
            #         Log.note("Skipping sustained_median exceptions (test is known multimodal)")
            #     all_touched.update(Q.select(test_results, "test_run_id"))
            #     continue

            if debug:
                Log.note("Find sustained_median exceptions")

            #APPLY WINDOW FUNCTIONS
            stats = Q.run({
                "from": test_results,
                "window": [
                    {
                        # SO WE CAN SHOW A DATAZILLA WINDOW
                        "name": "push_date_min",
                        "value": lambda r: r.push_date,
                        # "edges": query.edges,
                        "sort": "push_date",
                        "aggregate": windows.Min,
                        "range": {"min": -settings.param.sustained_median.window_size, "max": 0}
                    }, {
                        "name": "past_stats",
                        "value": lambda r: r.value,
                        # "edges": query.edges,
                        "sort": "push_date",
                        "aggregate": windows.Stats(middle=0.60),
                        "range": {"min": -settings.param.sustained_median.window_size, "max": 0}
                    }, {
                        "name": "future_stats",
                        "value": lambda r: r.value,
                        # "edges": query.edges,
                        "sort": "push_date",
                        "aggregate": windows.Stats(middle=0.60),
                        "range": {"min": 0, "max": settings.param.sustained_median.window_size}
                    }, {
                        "name": "result",
                        "value": lambda r, i, rows: median_test(
                            rows[-settings.param.sustained_median.window_size + i:i:].value,
                            rows[i:settings.param.sustained_median.window_size + i:].value,
                            interpolate=False
                        ),
                        "sort": "push_date"
                    }, {
                        "name": "is_diff",
                        "value": lambda r: True if settings.param.sustained_median.trigger < r.result.confidence else False
                    }, {
                        "name": "diff",
                        "value": lambda r: Math.abs(r.future_stats.mean - r.past_stats.mean) / r.past_stats.mean
                    }
                ]
            })

            #PICK THE BEST SCORE FOR EACH is_diff==True REGION
            for g2, data in Q.groupby(stats, "is_diff", contiguous=True):
                if g2.is_diff:
                    best = Q.sort(data, ["result.confidence", "diff"]).last()
                    best["pass"] = True

            all_touched.update(Q.select(test_results, ["test_run_id", "B2G.Test"]))

            # TESTS THAT HAVE BEEN (RE)EVALUATED GIVEN THE NEW INFORMATION
            re_alert.update(Q.select(test_results, ["test_run_id", "B2G.Test"]))

            #FOR DEBUGGING
            # Q.select(stats[0:400:], ["test_build.gaia_revision","push_date", "is_diff", "result.confidence"])

            File("test_values.txt").write(CNV.list2tab(Q.select(stats, [
                {"name": "push_date", "value": lambda x: CNV.datetime2string(CNV.milli2datetime(x.push_date), "%d-%b-%Y %H:%M:%S")},
                "value",
                {"name": "gaia", "value": "B2G.Revision.gaia"},
                {"name": "gecko", "value": "B2G.Revision.gecko"},
                {"name": "confidence", "value": "result.confidence"},
                "pass"
            ])))

            #TESTS THAT HAVE SHOWN THEMSELVES TO BE EXCEPTIONAL
            new_exceptions = Q.filter(stats, {"term": {"pass": True}})
            for v in new_exceptions:
                alert = Struct(
                    status="new",
                    create_time=CNV.milli2datetime(v.push_date),
                    tdad_id={"test_run_id": v.test_run_id, "B2G": {"Test": v.B2G.Test}},
                    reason=REASON,
                    revision=v.B2G.Revision,
                    details=v,
                    severity=SEVERITY,
                    confidence=v.result.confidence
                )
                alerts.append(alert)

            if debug:
                Log.note("{{num}} new exceptions found", {"num": len(new_exceptions)})

        except Exception, e:
            Log.warning("Problem with alert identification, continue to log existing alerts and stop cleanly", e)

        # break  # DEBUGGING ONLY

    if debug:
        Log.note("Get Current Alerts")

    #CHECK THE CURRENT ALERTS
    if not re_alert:
        current_alerts = []
    else:
        current_alerts = DBQuery(alerts_db).query({
            "from": "alerts",
            "select": [
                "id",
                "tdad_id",
                "status",
                "last_updated",
                "severity",
                "confidence",
                "details",
                "solution"
            ],
            "where": {"and": [
                {"terms": {"tdad_id":  re_alert}},
                {"term": {"reason": REASON}}
            ]}
        })

    found_alerts = Q.unique_index(alerts, "tdad_id")
    current_alerts = Q.unique_index(current_alerts, "tdad_id")

    new_alerts = found_alerts - current_alerts
    changed_alerts = current_alerts & found_alerts
    obsolete_alerts = current_alerts - found_alerts

    if debug:
        Log.note("Update Alerts: ({{num_new}} new, {{num_change}} changed, {{num_delete}} obsoleted)", {
            "num_new": len(new_alerts),
            "num_change": len(changed_alerts),
            "num_delete": len(obsolete_alerts)
        })

    if new_alerts:
        for a in new_alerts:
            a.id = SQL("util_newid()")
            a.last_updated = datetime.utcnow()
        try:
            alerts_db.insert_list("alerts", new_alerts)
        except Exception, e:
            Log.error("problem with insert", e)

    for curr in changed_alerts:
        if len(nvl(curr.solution, "").strip()) != 0:
            continue  # DO NOT TOUCH SOLVED ALERTS

        a = found_alerts[(curr.tdad_id,)]

        if significant_difference(curr.severity, a.severity) or \
                significant_difference(curr.confidence, a.confidence) or \
                        curr.reason != a.reason:
            curr.last_updated = datetime.utcnow()
            alerts_db.update("alerts", {"id": curr.id}, a)

    #OBSOLETE THE ALERTS THAT ARE NO LONGER VALID
    if obsolete_alerts:
        alerts_db.execute("UPDATE alerts SET status='obsolete' WHERE {{where}}", {
            "where": esfilter2sqlwhere(alerts_db, {"terms": {"id": Q.select(obsolete_alerts, "id")}})
        })

    alerts_db.execute("UPDATE alert_reasons SET last_run={{now}} WHERE {{where}}", {
        "now": datetime.utcnow(),
        "where": esfilter2sqlwhere(alerts_db, {"term": {"code": REASON}})
    })

    alerts_db.flush()

    if debug:
        Log.note("Marking {{num}} test_run_id as 'done'", {"num": len(all_touched)})

    for g, t in Q.groupby(all_touched, "B2G.Test"):
        qb.update({
            "set": {"processed_sustained_median": "done"},
            "where": {"and": [
                {"terms": {"test_run_id": t.test_run_id}},
                {"term": {"B2G.Test": g.B2G.Test}}
            ]}
        })


def main():
    settings = startup.read_settings()
    Log.start(settings.debug)
    try:
        Log.note("Finding exceptions in schema {{schema}}", {"schema": settings.perftest.schema})

        qb = ESQuery(ElasticSearch(settings.query["from"]))
        qb.addDimension(CNV.JSON2object(File(settings.dimension.filename).read()))

        with DB(settings.alerts) as alerts_db:
            alert_sustained_median(
                settings,
                qb,
                alerts_db
            )
    except Exception, e:
        Log.warning("Failure to find sustained_median exceptions", e)
    finally:
        Log.stop()


if __name__ == '__main__':
    main()