# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from datetime import datetime
from math import sqrt

import scipy
from scipy import stats
from dzAlerts.util import struct
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
# MIN_CONFIDENCE = 0.9999
REASON = "alert_exception"     # name of the reason in alert_reason

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

    #SPLIT INTO TWO BECAUSE MYSQL GOES PATHOLOGICALLY SLOW ON SIMPLE QUERY
    #   SELECT
    # 	    `t`.`test_name`, `t`.`product`, `t`.`branch`, `t`.`branch_version`, `t`.`operating_system_name`, `t`.`operating_system_version`, `t`.`processor`, `t`.`page_url`,
    # 	    min(push_date) min_push_date
    # 	FROM
    # 	    `ekyle_objectstore_1`.objectstore o
    # 	JOIN
    # 	    `ekyle_perftest_1`.test_data_all_dimensions t
    # 	ON
    # 	    t.test_run_id = o.test_run_id
    # 	WHERE
    # 	    NOT (`o`.`processed_exception`='summary_complete')
    # 	LIMIT
    # 	    1000
    records_to_process = set(Q.select(db.query("""
            SELECT
                o.test_run_id
            FROM
                {{objectstore}}.objectstore o
            WHERE
                {{where}}
            LIMIT
                {{sample_limit}}
        """, {
        "objectstore": db.quote_column(settings.objectstore.schema),
        "sample_limit": SQL(settings.param.exception.max_test_results_per_run),
        "where": db.esfilter2sqlwhere({"and": [
            {"term": {"o.processed_exception": 'ready'}},
            {"term": {"o.processed_cube": "done"}}
        ]})
    }), "test_run_id"))

    new_test_points = db.query("""
        SELECT
            {{edges}},
            min(push_date) min_push_date
        FROM
            {{perftest}}.test_data_all_dimensions t
        WHERE
            {{where}}
        GROUP BY
            {{edges}}
    """, {
        "perftest": db.quote_column(settings.perftest.schema),
        "edges": db.quote_column(query.edges, table="t"),
        "where": db.esfilter2sqlwhere({"terms": {"t.test_run_id": records_to_process}})
    })

    #BRING IN ALL NEEDED DATA
    if debug:
        Log.note("Pull all data for {{num}} groups:\n{{groups}}", {
            "num": len(new_test_points),
            "groups": new_test_points
        })

    all_min_date = Null
    all_touched = set()
    re_alert = set()
    alerts = []   # PUT ALL THE EXCEPTION ITEMS HERE
    for g, points in Q.groupby(new_test_points, query.edges):
        try:
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
                "window_size": settings.param.exception.window_size + 1
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
                        "range": {"min": -settings.param.exception.window_size, "max": 0}
                    }, {
                        "name": "past_stats",
                        "value": lambda (r): Stats(count=1, mean=r.mean),
                        "edges": query.edges,
                        "sort": "push_date",
                        "aggregate": windows.Stats,
                        "range": {"min": -settings.param.exception.window_size, "max": 0}
                    }, {
                        "name": "result",
                        "value": lambda (r): single_ttest(r.mean, r.past_stats, min_variance=1.0 / 12.0) #VARIANCE OF STANDARD UNIFORM DISTRIBUTION
                    }, {
                        "name": "pass",
                        "value": lambda (r): True if settings.param.exception.min_confidence < r.result.confidence else False
                    },

                ]
            })

            all_touched.update(Q.select(test_results, "test_run_id"))

            # TESTS THAT HAVE BEEN (RE)EVALUATED GIVEN THE NEW INFORMATION
            re_alert.update(Q.select(test_results, "tdad_id"))

            #TESTS THAT HAVE SHOWN THEMSELVES TO BE EXCEPTIONAL
            new_exceptions = Q.filter(stats, {"term": {"pass": True}})

            for v in new_exceptions:
                v.diff = v.result.diff
                v.confidence = v.result.confidence
                v.result = None

                alert = Struct(
                    status="new",
                    create_time=v.push_date,
                    tdad_id=v.tdad_id,
                    reason=REASON,
                    revision=v.revision,
                    details=v,
                    severity=SEVERITY,
                    confidence=v.confidence
                )
                alerts.append(alert)

            if debug:
                Log.note("{{num}} new exceptions found", {"num": len(new_exceptions)})

        except Exception, e:
            Log.warning("Problem with alert identification, continue to log existing alerts and stop cleanly", e)

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
                {{where}}
        """, {
            "where": db.esfilter2sqlwhere({"and": [
                {"terms": {"a.tdad_id": re_alert}},
                {"term": {"reason": REASON}}
            ]})
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
        "where": db.esfilter2sqlwhere({"terms": {"id": Q.select(obsolete_alerts, "id")}})
    })

    db.execute("UPDATE alert_reasons SET last_run={{now}} WHERE {{where}}", {
        "now": datetime.utcnow(),
        "where": db.esfilter2sqlwhere({"term": {"code": REASON}})
    })

    if debug:
        Log.note("Reviewing h0")

    update_h0_rejected(db, all_min_date, set(Q.select(current_alerts, "tdad_id")) | set(Q.select(found_alerts, "tdad_id")))

    if debug:
        Log.note("Marking {{num}} test_run_id as 'summary_complete'", {"num": len(all_touched | records_to_process)})
    db.execute("""
        UPDATE {{objectstore}}.objectstore
        SET processed_exception='done'
        WHERE {{where}}
    """, {
        "objectstore": db.quote_column(settings.objectstore.schema),
        "where": db.esfilter2sqlwhere({"terms": {"test_run_id": all_touched | records_to_process}})
    })
    db.flush()


def single_ttest(point, stats, min_variance=0):
    n1 = stats.count
    m1 = stats.mean
    v1 = stats.variance

    if n1 < 2:
        return {"confidence": 0, "diff": 0}

    try:
        tt = (point - m1) / sqrt(max(min_variance, v1))    #WE WILL IGNORE UNUSUALLY GOOD TIMINGS
        t_distribution = scipy.stats.distributions.t(n1 - 1)
        confidence = t_distribution.cdf(tt)
        return {"confidence": confidence, "diff": tt}
    except Exception, e:
        Log.error("error with t-test", e)


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
