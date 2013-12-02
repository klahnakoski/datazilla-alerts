################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## Author: Kyle Lahnakoski (kyle@lahnakoski.com)
################################################################################


from datetime import datetime

import scipy
from scipy import stats
from dzAlerts.daemons.alert_exception import single_ttest
from dzAlerts.util import struct
from dzAlerts.util.cnv import CNV
from dzAlerts.util.maths import Math
from dzAlerts.util.queries import windows

scipy.stats = stats  # I WANT TO REFER TO "scipy.stats" TO BE EXPLICIT

from dzAlerts.daemons.alert import update_h0_rejected, significant_difference
from dzAlerts.util.struct import nvl
from dzAlerts.util.db import SQL
from dzAlerts.util.logs import Log
from dzAlerts.util.struct import Struct, Null
from dzAlerts.util.queries import Q
from dzAlerts.util.stats import Stats
from dzAlerts.util.db import DB
from dzAlerts.util import startup


SEVERITY = 0.6              # THERE ARE MANY FALSE POSITIVES (0.99 == positive indicator, 0.5==not an indicator, 0.01 == negative indicator)
MIN_CONFIDENCE = 0.99
REASON = "alert_exception"     # name of the reason in alert_reason
WINDOW_SIZE = 10
SAMPLE_LIMIT_FOR_DEBUGGING = 10         # FOR LIMITING NUMBER OF TESTS IN SINGLE PULL, SET TO one million IN PROD
DEBUG = True


def alert_exception(settings, db):
    """
    find single points that deviate from the trend
    """

    debug = nvl(settings.param.debug, DEBUG)
    db.debug = debug

    # THE EVENTUAL GOAL IS TO PARAMETRIZE THE SQL, WHICH REALLY IS
    # SIMPLE EDGE SETS
    query = struct.wrap({
        "from": "test_data_all_dimensions",
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
            "branch_version",
            "operating_system_name",
            "operating_system_version",
            "processor",
            "page_url"
        ],
        "sort": {"name": "push_date", "value": "push_date"}
    })

    #FIND NEW POINTS IN CUBE TO TEST
    if debug:
        Log.note("Find tests that need summary statistics")

    new_test_points = db.query("""
        SELECT
            {{edges}},
            min(push_date) min_push_date
        FROM
            {{objectstore}}.objectstore o
        JOIN
            {{perftest}}.test_data_all_dimensions t
        ON
            t.test_run_id = o.test_run_id
        WHERE
            {{where}}
        GROUP BY
            {{edges}}
        LIMIT
            {{sample_limit}}
    """, {
        "perftest": db.quote_column(settings.perftest.schema),
        "objectstore": db.quote_column(settings.objectstore.schema),
        "edges": db.quote_column(query.edges, table="t"),
        "sample_limit": SQL(SAMPLE_LIMIT_FOR_DEBUGGING),
        "where": db.esfilter2sqlwhere({"and": [
            {"not": {"term": {"o.processed_flag": "summary_complete"}}},
            {"exists": "t.n_replicates"}
        ]})
    })

    #BRING IN ALL NEEDED DATA
    if debug:
        Log.note("Pull all data for {{num}} groups:\n{{groups}}", {
            "num": len(new_test_points),
            "groups": new_test_points
        })

    all_min_date = Null
    all_touched = set()
    re_alert = []
    alerts = []   # PUT ALL THE EXCEPTION ITEMS HERE

    for g, points in Q.groupby(new_test_points, query.edges):
        min_date = Math.min(Q.select(points, "min_push_date"))
        all_min_date = Math.min([all_min_date, min_date])

        # FOR THIS g, HOW FAR BACK IN TIME MUST WE GO TO COVER OUR WINDOW_SIZE?
        first_in_window = db.query("""
            SELECT
                min(push_date) min_date
            FROM (
                SELECT
                    push_date
                FROM
                    {{from}} t
                WHERE
                    {{where}}
                ORDER BY
                    push_date DESC
                LIMIT
                    {{window_size}}
            ) t
        """, {
            "from": db.quote_column(query["from"]),
            "edges": db.quote_column(query.edges),
            "where": db.esfilter2sqlwhere({"and": [
                {"term": g},
                {"exists": "n_replicates"},
                {"range": {"push_date": {"lt": min_date}}}
            ]}),
            "window_size": WINDOW_SIZE + 1
        })[0]

        all_min_date = Math.min([all_min_date, first_in_window.min_date])

        #LOAD TEST RESULTS FROM DATABASE
        test_results = db.query("""
            SELECT
                revision,
                {{edges}},
                {{sort}},
                {{select}}
            FROM
                {{from}} t
            WHERE
                {{where}}
            """, {
            "from": db.quote_column(query["from"]),
            "sort": db.quote_column(query.sort),
            "select": db.quote_column(query.select),
            "edges": db.quote_column(query.edges),
            "where": db.esfilter2sqlwhere({"and": [
                {"term": g},
                {"exists": "n_replicates"},
                {"range": {"push_date": {"gte": Math.min([min_date, first_in_window.min_date])}}}
            ]})
        })

        Log.note("{{num}} test results found for\n{{group}}", {
            "num": len(test_results),
            "group": g
        })

        if debug:
            Log.note("Find exceptions")

        #APPLY WINDOW FUNCTIONS
        stats = Q.run({
            "from": test_results,
            "window": [
                {
                    "name": "push_date_min",
                    "value": lambda (r): r.push_date,
                    "edges": query.edges,
                    "sort": "push_date",
                    "aggregate": windows.Min,
                    "range": {"min": -WINDOW_SIZE, "max": 0}
                }, {
                    "name": "past_stats",
                    "value": lambda (r): Stats(count=1, mean=r.mean),
                    "edges": query.edges,
                    "sort": "push_date",
                    "aggregate": windows.Stats,
                    "range": {"min": -WINDOW_SIZE, "max": 0}
                }, {
                    "name": "result",
                    "value": lambda (r): single_ttest(r.mean, r.past_stats, min_variance=1.0 / 12.0)
                }, {
                    "name": "pass",
                    "value": lambda (r): True if MIN_CONFIDENCE < r.result.confidence and r.result.diff > 0 else False
                },

            ]
        })

        all_touched.update(Q.select(test_results, "test_run_id"))

        # TESTS THAT HAVE BEEN (RE)EVALUATED GIVEN THE NEW INFORMATION
        re_alert.extend(Q.run({
            "from": stats,
            "select": "tdad_id",
            "where": lambda (r): r.past_stats.count == WINDOW_SIZE
        }))

        #TESTS THAT HAVE SHOWN THEMSELVES TO BE EXCEPTIONAL
        new_exceptions = [e for e in stats if e["pass"]]

        for v in new_exceptions:
            v.diff = v.result.diff
            v.confidence = v.result.confidence
            v.result = Null

            alert = Struct(
                status="new",
                create_time=datetime.utcnow(),
                tdad_id=v.tdad_id,
                reason=REASON,
                details=v,
                severity=SEVERITY,
                confidence=v.confidence
            )
            alerts.append(alert)

            # Log.debug(CNV.object2JSON(alert))

        if debug:
            Log.note("{{num}} new exceptions found", {"num": len(new_exceptions)})


    # if debug: Log.note(
    #     "Testing {{num_tests}} samples, {{num_alerts}} alerts, on group {{key}}",
    #     {"key": edges, "num_tests": len(values), "num_alerts": num_new}
    # )

    if debug:
        Log.note("Get Current Alerts")

    #CHECK THE CURRENT ALERTS
    if len(re_alert) == 0:
        current_alerts = []
    else:
        current_alerts = db.query("""
            SELECT
                a.id,
                a.tdad_id,
                a.status,
                a.last_updated,
                a.severity,
                a.confidence,
                a.details,
                a.solution
            FROM
                alerts a
            WHERE
                a.tdad_id in {{re_alerts}} AND
                reason={{type}}
            """, {
            "re_alerts": SQL(re_alert),
            "type": REASON
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
        db.insert("alerts", a)

    for curr in changed_alerts:
        if len(nvl(curr.solution, "").strip()) != 0:
            continue  # DO NOT TOUCH SOLVED ALERTS

        a = found_alerts[curr.tdad_id]

        if significant_difference(curr.severity, a.severity) or \
                significant_difference(curr.confidence, a.confidence) or \
                        curr.reason != a.reason \
            :
            curr.last_updated = datetime.utcnow()
            db.update("alerts", {"id": a.id}, a)

    #OBSOLETE THE ALERTS THAT ARE NO LONGER VALID
    for a in obsolete_alerts:
        if a.status != "obsolete":
            a.status = "obsolete"
            a.last_updated = datetime.utcnow()
            db.update("alerts", {"id": a.id}, a)

    if debug:
        Log.note("Reviewing h0")

    update_h0_rejected(db, all_min_date)

    if len(all_touched) > 0:
        if debug:
            Log.note("Marking {{num}} test_run_id as 'summary_complete'", {"num": len(all_touched)})
        db.execute("""
            UPDATE {{objectstore}}.objectstore
            SET processed_flag='summary_complete'
            WHERE {{where}}
        """, {
            "objectstore": db.quote_column(settings.objectstore.schema),
            "where": db.esfilter2sqlwhere({"term": {"test_run_id": all_touched}})
        })


def main():
    settings = startup.read_settings()
    Log.start(settings.debug)
    try:
        Log.note("Finding exceptions in schema {{schema}}", {"schema": settings.perftest.schema})
        with DB(settings.perftest) as db:
            #TEMP FIX UNTIL IMPORT DOES IT FOR US
            db.execute("""update test_data_all_dimensions set push_date=date_received where push_date is null""")

            alert_exception(
                settings,
                db
            )
    except Exception, e:
        Log.warning("Failure to find exceptions", cause=e)
    finally:
        Log.stop()


if __name__ == '__main__':
    main()
