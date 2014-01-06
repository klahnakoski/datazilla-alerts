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
from math import sqrt
import numpy
import scipy
from scipy import stats
from dzAlerts import util
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


SEVERITY = 0.8              # THERE ARE MANY FALSE POSITIVES (0.99 == positive indicator, 0.5==not an indicator, 0.01 == negative indicator)
# MIN_CONFIDENCE = 0.9999
REASON = "alert_sustained"     # name of the reason in alert_reason

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


def alert_sustained(settings, db):
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
            "operating_system_name",
            "operating_system_version",
            "processor",
            "page_url"
        ],
        "sort": {"name": "push_date", "value": "push_date"}
    })

    #FIND NEW POINTS IN CUBE TO TEST
    if debug:
        Log.note("Find tests that need sustained regression detection")

    records_to_process = set(Q.select(db.query("""
            SELECT
                o.test_run_id
            FROM
                {{objectstore}}.objectstore o
            WHERE
                {{where}}
            ORDER BY
                o.test_run_id
            LIMIT
                {{sample_limit}}
        """, {
        "objectstore": db.quote_column(settings.objectstore.schema),
        "sample_limit": SQL(settings.param.sustained.max_test_results_per_run),
        "where": db.esfilter2sqlwhere({"and": [
            {"term": {"o.processed_sustained": 'ready'}},
            {"term": {"o.processed_cube": "done"}}
        ]})
    }), "test_run_id"))

    #TODO: BE SURE WE CAUGHT https://datazilla.mozilla.org/?start=1385895839&stop=1388674396&product=Firefox&repository=Try-Non-PGO&test=tsvgx&page=hixie-003.xml&graph_search=331ddc9661bc&tr_id=3877458&graph=win%206.1.7601&x86_64=true&error_bars=false&project=talos


    # TODO: Turn into tests
    # records_to_process = set(Q.select(db.query("""
    #     SELECT
    #         test_run_id
    #     FROM
    #         ekyle_perftest_1.test_data_all_dimensions t
    #     WHERE
    #         # TEST C
    #         branch='Try-Non-PGO' AND
    #         branch_version='29.0a1' AND
    #         operating_system_version='6.1.7601' AND
    #         test_name='tsvgx' and
    #         page_url='hixie-003.xml'
    #         # TEST B
    #         # branch='Mozilla-Esr17' AND
    #         # operating_system_version='fedora 12' AND
    #         # processor='x86_64' and
    #         # test_name='dromaeo_css' and
    #         # page_url='jquery.html'
    #         # TEST A
    #         # branch='Mozilla-Inbound' AND
    #         # operating_system_version='OS X 10.8' AND
    #         # processor='x86_64' and
    #         # test_name='tp5o' and
    #         # page_url='bbc.co.uk'
    # """), "test_run_id"))

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
        "where": db.esfilter2sqlwhere({"and": [
            {"terms": {"t.test_run_id": records_to_process}},
            # PART OF TEST A
            # {"term": {"page_url": "store.apple.com"}},
            # {"term": {"branch_version": "28.0a1"}}
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
                "window_size": settings.param.sustained.window_size + 1
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
                ORDER BY
                    push_date
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
                Log.note("Find sustained exceptions")

            #APPLY WINDOW FUNCTIONS
            stats = Q.run({
                "from": test_results,
                "window": [
                    {
                        "name": "push_date_min",
                        "value": lambda r: r.push_date,
                        "edges": query.edges,
                        "sort": "push_date",
                        "aggregate": windows.Min,
                        "range": {"min": -settings.param.sustained.window_size, "max": 0}
                    }, {
                        "name": "past_stats",
                        "value": lambda r: r.mean,
                        "edges": query.edges,
                        "sort": "push_date",
                        "aggregate": windows.Stats(middle=0.60),
                        "range": {"min": -settings.param.sustained.window_size, "max": 0}
                    }, {
                        "name": "future_stats",
                        "value": lambda r: r.mean,
                        "edges": query.edges,
                        "sort": "push_date",
                        "aggregate": windows.Stats(middle=0.60),
                        "range": {"min": 0, "max": settings.param.sustained.window_size}
                    }, {
                        "name": "sustained_result",
                        "value": lambda r: welchs_ttest(r.past_stats, r.future_stats)
                    }, {
                        "name": "is_diff",
                        "value": lambda r: True if settings.param.sustained.trigger < r.sustained_result.confidence else False
                    }

                ]
            })

            #PICK THE BEST SCORE FOR EACH is_diff==True REGION
            for g, data in Q.groupby(stats, "is_diff", contiguous=True):
                if g.is_diff:
                    best = Q.sort(data, ["sustained_result.confidence", "sustained_result.diff"]).last()
                    best["pass"] = True

            all_touched.update(Q.select(test_results, "test_run_id"))

            # TESTS THAT HAVE BEEN (RE)EVALUATED GIVEN THE NEW INFORMATION
            re_alert.update(Q.select(test_results, "tdad_id"))

            #TESTS THAT HAVE SHOWN THEMSELVES TO BE EXCEPTIONAL
            Q.select(stats, ["revision", "is_diff", "sustained_result.confidence", "past_stats", "future_stats"])
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
                    confidence=v.sustained_result.confidence
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
        Log.note("Marking {{num}} test_run_id as 'done'", {"num": len(all_touched | records_to_process)})
    db.execute("""
        UPDATE {{objectstore}}.objectstore
        SET processed_sustained='done'
        WHERE {{where}}
    """, {
        "objectstore": db.quote_column(settings.objectstore.schema),
        "where": db.esfilter2sqlwhere({"terms": {"test_run_id": all_touched | records_to_process}})
    })
    db.flush()


def welchs_ttest(stats1, stats2):
    """
    SNAGGED FROM https://github.com/mozilla/datazilla-metrics/blob/master/dzmetrics/ttest.py#L56
    Execute TWO-sided Welch's t-test given pre-calculated means and stddevs.

    Accepts summary data (N, stddev, and mean) for two datasets and performs
    one-sided Welch's t-test, returning p-value.
    """
    n1 = stats1.count
    m1 = stats1.mean
    v1 = max(stats1.variance, 1.0/12.0)

    n2 = stats2.count
    m2 = stats2.mean
    v2 = max(stats2.variance, 1.0/12.0)

    if n1 < 2 or n2 < 2:
        return {"confidence": 0, "diff": 0}

    vpooled = v1 / n1 + v2 / n2
    # 1/12 == STD OF STANDARD UNIFORM DISTRIBUTION
    # We assume test replicates (xi) are actually rounded results from
    # actual measurements somewhere in the range of (xi - 0.5, xi + 0.5),
    # which has a variance of 1/12
    tt = abs(m1 - m2) / sqrt(vpooled)

    df_numerator = vpooled ** 2
    df_denominator = ((v1 / n1) ** 2) / (n1 - 1) + ((v2 / n2) ** 2) / (n2 - 1)
    df = df_numerator / df_denominator

    # abs(x - 0.5)*2 IS AN ATTEMPT TO GIVE HIGH NUMBERS TO EITHER TAIL OF THE cdf
    return {"confidence": abs(stats.t(df).cdf(tt) - 0.5) * 2, "diff": tt}


def median_test(samples1, samples2):
    if len(samples1) < 3 or len(samples2) < 3:
        return {"diff": 0, "confidence": 0}
    median = util.stats.median(samples1 + samples2)

    above1, below1 = count_partition(samples1, median)
    above2, below2 = count_partition(samples2, median)

    result = stats.chisquare(
        numpy.array([above1, below1, above2, below2]),
        f_exp=numpy.array([float(len(samples1)) / 2, float(len(samples1)) / 2, float(len(samples2)) / 2, float(len(samples2)) / 2])
    )
    return {"diff": result[0], "confidence": result[1]}


def count_partition(samples, cut_value, resolution=1.0):
    """
    COMPARE SAMPLES TO cut_value AND COUNT IF GREATER OR LESSER
    """
    a = 0
    b = 0
    min_cut = cut_value - resolution/2
    max_cut = cut_value + resolution/2
    for v in samples:
        if v > max_cut:
            b += 1
        elif v < min_cut:
            a += 1
        else:
            a += (max_cut - v) / resolution
            b += (v - min_cut) / resolution
    return a, b


def pick_besk(past, present, future):
    """
    LOOKING AT THESE THREE, WHICH ONE SHOULD BE MARKED AS THE REGRESSION?
    """
    if not present.is_diff:
        return False

    if past.is_diff:
        if future.sustained_result.confidence > present.sustained_result.confidence:
            return False
        else:
            return True
    else:
        if future.sustained_result.confidence > present.sustained_result.confidence:
            return False
        else:
            return True




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
            alert_sustained(
                settings,
                db
            )
    except Exception, e:
        Log.warning("Failure to find sustained exceptions", cause=e)
    finally:
        Log.stop()


if __name__ == '__main__':
    main()
