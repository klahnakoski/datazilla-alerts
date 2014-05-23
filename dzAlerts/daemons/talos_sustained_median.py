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

from dzAlerts.util.collections import MIN, MAX, AND, OR
from dzAlerts.util.env.elasticsearch import ElasticSearch
from dzAlerts.util.env.files import File
from dzAlerts.util.maths import Math
from dzAlerts.util.queries.es_query import ESQuery
from dzAlerts.util.env import startup
from dzAlerts.util.queries.db_query import DBQuery, esfilter2sqlwhere
from dzAlerts.daemons.util.median_test import median_test
from dzAlerts.util.cnv import CNV
from dzAlerts.util.queries import windows
from dzAlerts.util.queries.query import Query
from dzAlerts.util.struct import nvl, StructList, literal_field, unwrap
from dzAlerts.util.sql.db import SQL
from dzAlerts.util.env.logs import Log
from dzAlerts.util.struct import Struct, set_default
from dzAlerts.util.queries import Q
from dzAlerts.util.sql.db import DB
from dzAlerts.util.times.timer import Timer


SEVERITY = 0.8              # THERE ARE MANY FALSE POSITIVES (0.99 == positive indicator, 0.5==not an indicator, 0.01 == negative indicator)
# MIN_CONFIDENCE = 0.9999
REASON = "talos_alert_sustained_median"     # name of the reason in alert_reason
NOW = datetime.utcnow()
MAX_AGE = timedelta(days=90)
OLDEST_TS = CNV.datetime2milli(NOW - MAX_AGE)

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

    def is_bad(r):
        if settings.param.sustained_median.trigger < r.result.confidence:
            test_param = set_default(
                settings.param.suite[literal_field(r.Talos.Test.suite)],
                settings.param.test[literal_field(r.Talos.Test.name)],
                settings.param.default
            )

            if test_param.disable:
                return False

            if test_param.better == "higher":
                diff = -r.diff
            elif test_param.better == "lower":
                diff = r.diff
            else:
                diff = abs(r.diff)  # DEFAULT = ANY DIRECTION IS BAD

            if test_param.min_regression:
                if unicode(test_param.min_regression.strip()[-1]) == "%":
                    min_diff = Math.abs(r.past_stats.mean * float(test_param.min_regression.strip()[:-1]) / 100.0)
                else:
                    min_diff = Math.abs(float(test_param.min_regression))
            else:
                min_diff = Math.abs(r.past_stats.mean * 0.01)

            if diff > min_diff:
                return True

        return False

    with Timer("pull combinations"):
        disabled_suites = [s for s, p in settings.param.suite.items() if p.disable]
        disabled_tests = [t for t, p in settings.param.test.items() if p.disable]
        disabled_branches = [t for t, p in settings.param.branch.items() if p.disable]

        temp = Query({
            "from": TDAD,
            "select": {"name": "min_push_date", "value": PUSH_DATE, "aggregate": "min"},
            "edges": query.edges,
            "where": {"and": [
                True if settings.args.restart else {"missing": {"field": settings.param.mark_complete}},
                {"exists": {"field": "result.test_name"}},
                {"range": {PUSH_DATE: {"gte": OLDEST_TS}}},
                {"not": {"terms": {"Talos.Test.fields.suite": disabled_suites}}},
                {"not": {"terms": {"Talos.Branch": disabled_branches}}},
                {"not": {"terms": {"Talos.Test.fields.name": disabled_tests}}}
                # {"term": {"testrun.suite": "cart"}},
                # {"term": {"result.test_name": "1-customize-enter.error.TART"}},
                # {"term": {"test_machine.osversion": "OS X 10.8"}}
                #FOR DEBUGGING SPECIFIC SERIES
                # {"term": {"test_machine.type": "hamachi"}},
                # {"term": {"test_machine.platform": "Gonk"}},
                # {"term": {"test_machine.os": "Firefox OS"}},
                # {"term": {"test_build.branch": "master"}},
                # {"term": {"testrun.suite": "communications/ftu"}},
                # {"term": {"result.test_name": "startup_time"}}
            ]},
            "limit": nvl(settings.param.combo_limit, 1000)
        }, qb)

        new_test_points = qb.query(temp)

    #BRING IN ALL NEEDED DATA
    if debug:
        Log.note("Pull all data for {{num}} groups:\n{{groups.name}}", {
            "num": len(new_test_points),
            "groups": query.edges
        })

    # all_min_date = Null
    all_touched = set()
    evaled_tests = set()
    alerts = []   # PUT ALL THE EXCEPTION ITEMS HERE
    for g, test_points in Q.groupby(new_test_points, query.edges):
        if not test_points.min_push_date:
            continue
        try:
            if settings.args.restart:
                first_sample = OLDEST_TS
            else:
                first_sample = MAX(MIN(test_points.min_push_date), OLDEST_TS)
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
                    "limit": settings.param.sustained_median.window_size * 2
                }
            })
            if len(first_in_window) > settings.param.sustained_median.window_size * 2:
                do_all = False
            else:
                do_all = True

            min_date = MIN(first_sample, first_in_window.min_date)

            #LOAD TEST RESULTS FROM DATABASE
            test_results = qb.query({
                "from": {
                    "from": "talos",
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

            Log.note("{{num}} test results found for {{group}} dating back no further than {{start_date}}", {
                "num": len(test_results),
                "group": g,
                "start_date": CNV.milli2datetime(min_date)
            })

            if debug:
                Log.note("Find sustained_median exceptions")

            #APPLY WINDOW FUNCTIONS
            stats = Q.run({
                "from": {
                    "from": test_results,
                    "where": {"exists": {"field": "value"}}
                },
                "window": [
                    {
                        # WE DO NOT WANT TO CONSIDER THE POINTS BEFORE FULL WINDOW SIZE
                        "name": "ignored",
                        "value": lambda r, i: False if do_all or i > settings.param.sustained_median.window_size else True
                    }, {
                        # SO WE CAN SHOW A DATAZILLA WINDOW
                        "name": "push_date_min",
                        "value": lambda r: r.push_date,
                        "sort": "push_date",
                        "aggregate": windows.Min,
                        "range": {"min": -settings.param.sustained_median.window_size, "max": 0}
                    }, {
                        # SO WE CAN SHOW A DATAZILLA WINDOW
                        "name": "push_date_max",
                        "value": lambda r: r.push_date,
                        "sort": "push_date",
                        "aggregate": windows.Max,
                        "range": {"min": 0, "max": settings.param.sustained_median.window_size}
                    }, {
                        "name": "past_revision",
                        "value": lambda r, i, rows: rows[i - 1].Talos.Revision,
                        "sort": "push_date"
                    }, {
                        "name": "past_stats",
                        "value": lambda r: r.value,
                        "sort": "push_date",
                        "aggregate": windows.Stats(middle=0.60),
                        "range": {"min": -settings.param.sustained_median.window_size, "max": 0}
                    }, {
                        "name": "future_stats",
                        "value": lambda r: r.value,
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
                        "name": "diff",
                        "value": lambda r: r.future_stats.mean - r.past_stats.mean
                    }, {
                        "name": "diff_percent",
                        "value": lambda r: (r.future_stats.mean - r.past_stats.mean) / r.past_stats.mean
                    }, {
                        "name": "is_diff",
                        "value": is_bad
                    }, {
                        #USE THIS TO FILL CONFIDENCE HOLES
                        #WE CAN MARK IT is_diff KNOWING THERE IS A HIGHER CONFIDENCE
                        "name": "future_is_diff",
                        "value": lambda r, i, rows: rows[i - 1].is_diff and r.result.confidence < rows[i - 1].result.confidence,
                        "sort": "push_date"
                    }, {
                        #WE CAN MARK IT is_diff KNOWING THERE IS A HIGHER CONFIDENCE
                        "name": "past_is_diff",
                        "value": lambda r, i, rows: rows[i - 1].is_diff and r.result.confidence < rows[i - 1].result.confidence,
                        "sort": {"value": "push_date", "sort": -1}
                    },
                ]
            })

            #PICK THE BEST SCORE FOR EACH is_diff==True REGION
            for g2, data in Q.groupby(stats, "is_diff", contiguous=True):
                if g2.is_diff:
                    best = Q.sort(data, ["result.confidence", "diff"]).last()
                    best["pass"] = True

            all_touched.update(Q.select(test_results, ["test_run_id", "Talos.Test"]))

            # TESTS THAT HAVE BEEN (RE)EVALUATED GIVEN THE NEW INFORMATION
            evaled_tests.update(Q.run({
                "from": test_results,
                "select": ["test_run_id", "Talos.Test"],
                "where": {"term": {"ignored": False}}
            }))

            File("test_values.txt").write(CNV.list2tab(Q.select(stats, [
                {"name": "push_date", "value": lambda x: CNV.datetime2string(CNV.milli2datetime(x.push_date), "%d-%b-%Y %H:%M:%S")},
                "value",
                {"name": "revision", "value": "Talos.Revision"},
                {"name": "confidence", "value": "result.confidence"},
                "pass"
            ])))

            #TESTS THAT HAVE SHOWN THEMSELVES TO BE EXCEPTIONAL
            new_exceptions = Q.filter(stats, {"term": {"pass": True}})
            for v in new_exceptions:
                if v.ignored:
                    continue
                alert = Struct(
                    status="new",
                    create_time=CNV.milli2datetime(v.push_date),
                    tdad_id={"test_run_id": v.test_run_id, "Talos": {"Test": v.Talos.Test}},
                    reason=REASON,
                    revision=v.Talos.Revision,
                    details=v,
                    severity=SEVERITY,
                    confidence=v.result.confidence
                )
                alerts.append(alert)

            if debug:
                Log.note("{{num}} new exceptions found", {"num": len(new_exceptions)})

        except Exception, e:
            Log.warning("Problem with alert identification, continue to log existing alerts and stop cleanly", e)

    if debug:
        Log.note("Get Current Alerts")

    #CHECK THE CURRENT ALERTS
    if not evaled_tests:
        current_alerts = StructList.EMPTY
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
                {"terms": {"tdad_id": evaled_tests}},
                {"term": {"reason": REASON}}
            ]}
        })

    found_alerts = Q.unique_index(alerts, "tdad_id")
    current_alerts = Q.unique_index(current_alerts, "tdad_id")

    new_alerts = found_alerts - current_alerts
    changed_alerts = current_alerts & found_alerts
    obsolete_alerts = Q.filter(current_alerts - found_alerts, {"not": {"term": {"status": "obsolete"}}})

    if debug:
        Log.note("Update Alerts: ({{num_new}} new, {{num_change}} changed, {{num_delete}} obsoleted)", {
            "num_new": len(new_alerts),
            "num_change": len(changed_alerts),
            "num_delete": len(obsolete_alerts)
        })

    if new_alerts:
        for a in new_alerts:
            a.id = SQL("util.newid()")
            a.last_updated = NOW
        try:
            alerts_db.insert_list("alerts", new_alerts)
        except Exception, e:
            Log.error("problem with insert", e)

    for curr in changed_alerts:
        if len(nvl(curr.solution, "").strip()) != 0:
            continue  # DO NOT TOUCH SOLVED ALERTS

        a = found_alerts[(curr.tdad_id, )]

        if a == None:
            Log.error("Programmer error, changed_alerts must have {{key_value}}", {"key_value": curr.tdad.id})

        if significant_difference(curr.severity, a.severity) or \
                significant_difference(curr.confidence, a.confidence) or \
                        curr.reason != a.reason:
            curr.last_updated = NOW
            alerts_db.update("alerts", {"id": curr.id}, a)

    #OBSOLETE THE ALERTS THAT ARE NO LONGER VALID
    if obsolete_alerts:
        alerts_db.execute("UPDATE alerts SET status='obsolete' WHERE {{where}}", {
            "where": esfilter2sqlwhere(
                alerts_db,
                {"and": [
                    {"terms": {"id": obsolete_alerts.id}},
                    {"not": {"term": {"status": "obsolete"}}}
                ]}
            )
        })

    alerts_db.execute("UPDATE reasons SET last_run={{now}} WHERE {{where}}", {
        "now": NOW,
        "where": esfilter2sqlwhere(alerts_db, {"term": {"code": REASON}})
    })

    alerts_db.flush()

    if debug:
        Log.note("Marking {{num}} test_run_id as 'done'", {"num": len(all_touched)})

    for g, t in Q.groupby(all_touched, "Talos.Test"):
        qb.update({
            "set": {settings.param.mark_complete: "done"},
            "where": {"and": [
                {"terms": {"datazilla.test_run_id": t.test_run_id}},
                {"term": {"Talos.Test": g.Talos.Test}},
                {"missing": {"field": settings.param.mark_complete}}
            ]}
        })


def main():
    settings = startup.read_settings(defs=[{
        "name": ["--restart", "--reset", "--redo"],
        "help": "use this to recalc alerts",
        "action": "store_true",
        "dest": "restart"
    }])
    Log.start(settings.debug)
    try:
        with startup.SingleInstance(flavor_id=settings.args.filename):
            Log.note("Finding exceptions in index {{index_name}}", {"index_name": settings.query["from"].name})

            with ESQuery(ElasticSearch(settings.query["from"])) as qb:
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




