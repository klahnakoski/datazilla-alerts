# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import unicode_literals
from datetime import datetime
import scipy
from scipy import stats
from dzAlerts.util.env import startup
from dzAlerts.util.queries.db_query import DBQuery, esfilter2sqlwhere

scipy.stats = stats  # I WANT TO REFER TO "scipy.stats" TO BE EXPLICIT

from dzAlerts.daemons.util.median_test import median_test
from dzAlerts.util import struct
from dzAlerts.util.cnv import CNV
from dzAlerts.util.maths import Math
from dzAlerts.util.queries import windows

from dzAlerts.daemons.alert import update_h0_rejected, significant_difference
from dzAlerts.util.struct import nvl
from dzAlerts.util.sql.db import SQL
from dzAlerts.util.env.logs import Log
from dzAlerts.util.struct import Struct, Null
from dzAlerts.util.queries import Q
from dzAlerts.util.sql.db import DB


SEVERITY = 0.8              # THERE ARE MANY FALSE POSITIVES (0.99 == positive indicator, 0.5==not an indicator, 0.01 == negative indicator)
# MIN_CONFIDENCE = 0.9999
REASON = "alert_sustained_median"     # name of the reason in alert_reason

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

ALLOWED_TESTS = [
    "tp5o",
    "ts_paint",
    "tsvgx",
    "tsvgr_opacity",
    "tart",
    "tscrollx",
    "tpaint",
    "tresize",
    "a11yr",
    "kraken",
    "tcanvasmark",
    "v8_7"
]


def alert_sustained_median(settings, db):
    """
    find single points that deviate from the trend
    """
    OBJECTSTORE = settings.objectstore.schema + ".objectstore"
    TDAD = settings.perftest.schema + ".test_data_all_dimensions"

    debug = nvl(settings.param.debug, DEBUG)
    db.debug = debug


    # THE EVENTUAL GOAL IS TO PARAMETRIZE THE SQL, WHICH REALLY IS
    # SIMPLE EDGE SETS
    query = struct.wrap({
        "from": TDAD,
        "select": [
            {"value": "id", "name": "tdad_id"},
            "test_run_id",
            "revision",
            {"value": "n_replicates", "name": "count"},
            "mean",
            "std"
        ],
        "edges": [
            "test_name",
            "product",
            "branch",
            "operating_system_name",
            "operating_system_version",
            "processor",
            "page_url"
        ],
        "sort": {"name": "push_date", "value": "push_date"}
    })

    #FIND NEW POINTS IN CUBE TO TEST
    if debug:
        Log.note("Find tests that need sustained_median regression detection")

    qb = DBQuery(db)

    records_to_process = set(qb.query({
        "from": OBJECTSTORE,
        "select": "test_run_id",
        "where": {"and": [
            {"term": {"processed_sustained_median": 'ready'}},
            {"term": {"processed_cube": "done"}}
        ]},
        "limit": settings.param.sustained_median.max_test_results_per_run
    }))

    #TODO: BE SURE WE CAUGHT https://datazilla.mozilla.org/?start=1385895839&stop=1388674396&product=Firefox&repository=Try-Non-PGO&test=tsvgx&page=hixie-003.xml&graph_search=331ddc9661bc&tr_id=3877458&graph=win%206.1.7601&x86_64=true&error_bars=false&project=talos

    # TODO: Turn into tests
    records_to_process = set(qb.query({
        "from": TDAD,
        "select": "test_run_id",
        "where": {"term": {
            "branch": "Birch",
            "operating_system_name": "mac",
            "operating_system_version": "OS X 10.6.8",
            "page_url": "Asteroids - Vectors",
            "processor": "x86_64",
            "product": "Firefox",
            "test_name": "tcanvasmark"
        }},
        "limit": settings.param.sustained_median.max_test_results_per_run
    }))

    new_test_points = qb.query({
        "from": TDAD,
        "select": {"name": "min_push_date", "value": "push_date", "aggregate": "min"},
        "edges": query.edges,
        "where": {"and": [
            {"terms": {"test_run_id": records_to_process}},
        ]}
    })

    #BRING IN ALL NEEDED DATA
    if debug:
        Log.note("Pull all data for {{num}} groups:\n{{groups}}", {
            "num": len(new_test_points),
            "groups": new_test_points.edges
        })

    all_min_date = Null
    all_touched = set()
    re_alert = set()
    alerts = []   # PUT ALL THE EXCEPTION ITEMS HERE
    for g, points in Q.groupby(new_test_points, query.edges):
        try:
            min_date = Math.min(points)
            all_min_date = Math.min(all_min_date, min_date)

            # FOR THIS g, HOW FAR BACK IN TIME MUST WE GO TO COVER OUR WINDOW_SIZE?
            first_in_window = qb.query({
                "select": {"name": "min_date", "value": "push_date", "aggregate": "min"},
                "from": {
                    "from": TDAD,
                    "select": "push_date",
                    "where": {"and": [
                        {"term": g},
                        {"exists": "n_replicates"},
                        {"range": {"push_date": {"lt": min_date}}}
                    ]},
                    "sort": {"value": "push_date", "sort": -1},
                    "limit": settings.param.sustained_median.window_size + 1
                }})

            all_min_date = Math.min(all_min_date, first_in_window)

            #LOAD TEST RESULTS FROM DATABASE
            test_results = qb.query({
                "from": TDAD,
                "select": [
                    "revision",
                    "push_date"
                ] + query.select + query.edges,
                "where": {"and": [
                    {"term": g},
                    {"exists": "n_replicates"},
                    {"range": {"push_date": {"gte": Math.min([min_date, first_in_window.min_date])}}}
                ]},
                "sort": "push_date"
            })

            Log.note("{{num}} test results found for\n{{group}}", {
                "num": len(test_results),
                "group": g
            })

            if g.test_name not in ALLOWED_TESTS:
                if debug:
                    Log.note("Skipping sustained_median exceptions (test is known multimodal)")
                all_touched.update(Q.select(test_results, "test_run_id"))
                continue

            if debug:
                Log.note("Find sustained_median exceptions")

            #APPLY WINDOW FUNCTIONS
            stats = Q.run({
                "from": test_results.data,
                "window": [
                    {
                        "name": "push_date_min",
                        "value": lambda r: r.push_date,
                        "edges": query.edges,
                        "sort": "push_date",
                        "aggregate": windows.Min,
                        "range": {"min": -settings.param.sustained_median.window_size, "max": 0}
                    }, {
                        "name": "past_stats",
                        "value": lambda r: r.mean,
                        "edges": query.edges,
                        "sort": "push_date",
                        "aggregate": windows.Stats(middle=0.60),
                        "range": {"min": -settings.param.sustained_median.window_size, "max": 0}
                    }, {
                        "name": "future_stats",
                        "value": lambda r: r.mean,
                        "edges": query.edges,
                        "sort": "push_date",
                        "aggregate": windows.Stats(middle=0.60),
                        "range": {"min": 0, "max": settings.param.sustained_median.window_size}
                    }, {
                        "name": "result",
                        "value": lambda r, i, rows: median_test(rows[-settings.param.sustained_median.window_size + i:i:].mean, rows[i:settings.param.sustained_median.window_size + i:].mean,
                            interpolate=False),
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

            all_touched.update(Q.select(test_results, "test_run_id"))

            # TESTS THAT HAVE BEEN (RE)EVALUATED GIVEN THE NEW INFORMATION
            re_alert.update(Q.select(test_results, "tdad_id"))

            #FOR DEBUGGING
            # Q.select(stats, ["revision", "is_diff", "result.confidence", "past_stats", "future_stats"])

            if g.page_url == "Asteroids - Vectors":
                #https://datazilla.mozilla.org/?start=1375989662&stop=1391541662&product=Firefox&repository=Birch&os=mac&os_version=OS%20X%2010.6.8&test=tcanvasmark&project=talos
                Log.debug()

            #TESTS THAT HAVE SHOWN THEMSELVES TO BE EXCEPTIONAL
            new_exceptions = Q.filter(stats, {"term": {"pass": True}})

            for v in new_exceptions:
                alert = Struct(
                    status="new",
                    create_time=CNV.unix2datetime(v.push_date),
                    tdad_id=v.tdad_id,
                    reason=REASON,
                    revision=v.revision,
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
    if not re_alert:
        current_alerts = []
    else:
        current_alerts = qb.query({
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
                {"terms": {"tdad_id": re_alert}},
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

    for a in new_alerts:
        a.id = SQL("util_newid()")
        a.last_updated = datetime.utcnow()
    try:
        db.insert_list("alerts", new_alerts)
    except Exception, e:
        test = found_alerts - current_alerts
        Log.error("problem with insert", e)

    for curr in changed_alerts:
        if len(nvl(curr.solution, "").strip()) != 0:
            continue  # DO NOT TOUCH SOLVED ALERTS

        a = found_alerts[curr.tdad_id]

        if significant_difference(curr.severity, a.severity) or \
                significant_difference(curr.confidence, a.confidence) or \
                        curr.reason != a.reason:
            curr.last_updated = datetime.utcnow()
            db.update("alerts", {"id": curr.id}, a)

    #OBSOLETE THE ALERTS THAT ARE NO LONGER VALID
    db.execute("UPDATE alerts SET status='obsolete' WHERE {{where}}", {
        "where": esfilter2sqlwhere(db, {"terms": {"id": Q.select(obsolete_alerts, "id")}})
    })

    db.execute("UPDATE alert_reasons SET last_run={{now}} WHERE {{where}}", {
        "now": datetime.utcnow(),
        "where": esfilter2sqlwhere(db, {"term": {"code": REASON}})
    })

    if debug:
        Log.note("Reviewing h0")

    update_h0_rejected(db, all_min_date, set(Q.select(current_alerts, "tdad_id")) | set(Q.select(found_alerts, "tdad_id")))

    if debug:
        Log.note("Marking {{num}} test_run_id as 'done'", {"num": len(all_touched | records_to_process)})
    db.execute("""
        UPDATE {{objectstore}}.objectstore
        SET processed_sustained_median='done'
        WHERE {{where}}
    """, {
        "objectstore": db.quote_column(settings.objectstore.schema),
        "where": esfilter2sqlwhere(db, {"terms": {"test_run_id": all_touched | records_to_process}})
    })
    db.flush()


def main():
    settings = startup.read_settings()
    Log.start(settings.debug)
    try:
        Log.note("Finding exceptions in schema {{schema}}", {"schema": settings.perftest.schema})
        with DB(settings.perftest) as db:
            #TEMP FIX UNTIL IMPORT DOES IT FOR US
            db.execute("update test_data_all_dimensions set push_date=date_received where push_date is null")
            #TODO: REMOVE, LEAVE IN DB
            db.execute("update alert_reasons set email_template={{template}} where code={{reason}}", {
                "template": TEMPLATE,
                "reason": REASON
            })
            db.flush()
            alert_sustained_median(
                settings,
                db
            )
    except Exception, e:
        Log.warning("Failure to find sustained_median exceptions", cause=e)
    finally:
        Log.stop()


if __name__ == '__main__':
    main()