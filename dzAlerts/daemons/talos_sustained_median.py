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
from dzAlerts.util.maths import Math
from dzAlerts.util.queries.es_query import ESQuery
from dzAlerts.util.env import startup
from dzAlerts.util.queries.db_query import DBQuery, esfilter2sqlwhere
from dzAlerts.daemons.util.median_test import median_test
from dzAlerts.daemons.util.welchs_ttest import welchs_ttest
from dzAlerts.util.cnv import CNV
from dzAlerts.util.queries import windows
from dzAlerts.util.queries.query import Query
from dzAlerts.util.struct import nvl, StructList, literal_field, wrap_dot
from dzAlerts.util.sql.db import SQL
from dzAlerts.util.env.logs import Log
from dzAlerts.util.struct import Struct, set_default
from dzAlerts.util.queries import Q
from dzAlerts.util.sql.db import DB
from dzAlerts.util.times.timer import Timer


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

    debug = nvl(settings.param.debug, DEBUG)
    query = settings.query

    def is_bad(r, test_param):
        if test_param.min_confidence < r.result.confidence:
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
            "from": settings.query["from"],
            "select": {"name": "min_push_date", "value": settings.param.default.sort.value, "aggregate": "min"},
            "edges": query.edges,
            "where": {"and": [
                # True if settings.args.restart else {"missing": {"field": settings.param.mark_complete}},
                {"exists": {"field": "result.test_name"}},
                {"range": {settings.param.default.sort.value: {"gte": OLDEST_TS}}},
                {"not": {"terms": {settings.param.test_dimension+".fields.suite": disabled_suites}}},
                {"not": {"terms": {settings.param.test_dimension+".fields.name": disabled_tests}}},
                {"not": {"terms": {settings.param.branch_dimension: disabled_branches}}},
                #FOR DEBUGGING SPECIFIC SERIES
                {"term": {"testrun.suite": "dromaeo_css"}},
                # {"term": {"result.test_name": "alipay.com"}},
                # {"term": {"test_machine.osversion": "Ubuntu 12.04"}},
                # {"term": {"test_machine.platform": "x86_64"}}
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
            test_param = set_default(
                settings.param.test[literal_field(g[settings.param.test_dimension].name)],
                settings.param.suite[literal_field(g[settings.param.test_dimension].suite)],
                settings.param.branch[literal_field(g[settings.param.branch_dimension])],
                settings.param.default
            )

            if settings.args.restart:
                first_sample = OLDEST_TS
            else:
                first_sample = MAX(MIN(test_points.min_push_date), OLDEST_TS)
            # FOR THIS g, HOW FAR BACK IN TIME MUST WE GO TO COVER OUR WINDOW_SIZE?
            first_in_window = qb.query({
                "select": {"name": "min_date", "value": test_param.sort.name, "aggregate": "min"},
                "from": {
                    "from": settings.query["from"],
                    "select": test_param.sort,
                    "where": {"and": [
                        {"term": g},
                        {"range": {test_param.sort.value: {"lt": first_sample}}}
                    ]},
                    "sort": {"field": test_param.sort.value, "sort": -1},
                    "limit": test_param.window_size * 2
                }
            })
            if len(first_in_window) > test_param.window_size * 2:
                do_all = False
            else:
                do_all = True

            min_date = MIN(first_sample, first_in_window.min_date)

            #LOAD TEST RESULTS FROM DATABASE
            test_results = qb.query({
                "from": {
                    "from": settings.query["from"],
                    "select": [
                        test_param.sort,
                        test_param.select.datazilla,
                        test_param.select.repo,
                        test_param.select.value
                        ] +
                        query.edges,
                    "where": {"and": [
                        {"term": g},
                        {"range": {test_param.sort.value: {"gte": min_date}}}
                    ]},
                },
                "sort": test_param.sort.name
            })

            #REMOVE ALL TESTS EXCEPT MOST RECENT FOR EACH REVISION
            test_results = Q.run({
                "from": test_results,
                "window": [
                    {
                        "name": "redundant",
                        "value": lambda r, i, rows: True if r[settings.param.revision_dimension] == rows[i + 1][settings.param.revision_dimension] else None
                    }
                ],
                "where": {"missing": {"field": "redundant"}}
            })

            Log.note("{{num}} test results found for {{group}} dating back no further than {{start_date}}", {
                "num": len(test_results),
                "group": g,
                "start_date": CNV.milli2datetime(min_date)
            })

            if debug:
                Log.note("Find sustained_median exceptions")

            def diff_by_association(r, i, rows):
                #MARK IT DIFF IF IT IS IN THE T-TEST MOUNTAIN OF HIGH CONFIDENCE
                if rows[i - 1].is_diff and r.ttest_result.confidence > test_param.min_confidence:
                    r.is_diff = True
                return None

            #APPLY WINDOW FUNCTIONS
            stats = Q.run({
                "from": {
                    "from": test_results,
                    "where": {"exists": {"field": test_param.sort.name}},  # FOR THE RARE CASE WHEN THIS ATTRIBUTE IS MISSING
                    "sort": test_param.sort.name
                },
                "window": [
                    {
                        # WE DO NOT WANT TO CONSIDER THE POINTS BEFORE FULL WINDOW SIZE
                        "name": "ignored",
                        "value": lambda r, i: False if do_all or i > test_param.window_size else True
                    }, {
                        # SO WE CAN SHOW A DATAZILLA WINDOW
                        "name": "push_date_min",
                        "value": lambda r: r[test_param.sort.name],
                        "sort": test_param.sort.name,
                        "aggregate": windows.Min,
                        "range": {"min": -test_param.window_size, "max": 0}
                    }, {
                        # SO WE CAN SHOW A DATAZILLA WINDOW
                        "name": "push_date_max",
                        "value": lambda r: r[test_param.sort.name],
                        "sort": test_param.sort.name,
                        "aggregate": windows.Max,
                        "range": {"min": 0, "max": test_param.window_size}
                    }, {
                        "name": "past_revision",
                        "value": lambda r, i, rows: rows[i - 1][test_param.select.repo],
                        "sort": test_param.sort.name
                    }, {
                        "name": "past_stats",
                        "value": lambda r: r.value,
                        "sort": test_param.sort.name,
                        "aggregate": windows.Stats(middle=0.60),
                        "range": {"min": -test_param.window_size, "max": 0}
                    }, {
                        "name": "future_stats",
                        "value": lambda r: r.value,
                        "sort": test_param.sort.name,
                        "aggregate": windows.Stats(middle=0.60),
                        "range": {"min": 0, "max": test_param.window_size}
                    }, {
                        "name": "ttest_result",
                        "value": lambda r, i, rows:  welchs_ttest(
                            rows[-test_param.window_size + i:i:].value,
                            rows[ i:test_param.window_size + i:].value
                        ),
                        "sort": test_param.sort.name
                    }, {
                        "name": "result",
                        "value": lambda r, i, rows: median_test(
                            rows[-test_param.window_size + i:i:].value,
                            rows[ i:test_param.window_size + i:].value,
                            interpolate=False
                        ),
                        "sort": test_param.sort.name
                    }, {
                        "name": "diff",
                        "value": lambda r: r.future_stats.mean - r.past_stats.mean
                    }, {
                        "name": "diff_percent",
                        "value": lambda r: (r.future_stats.mean - r.past_stats.mean) / r.past_stats.mean
                    }, {
                        "name": "is_diff",
                        "value": lambda r: is_bad(r, test_param)
                    }, {
                        #USE THIS TO FILL CONFIDENCE HOLES
                        #WE CAN MARK IT is_diff KNOWING THERE IS A HIGHER CONFIDENCE
                        "name": "future_is_diff",
                        "value": diff_by_association,
                        "sort": test_param.sort.name
                    }, {
                        #WE CAN MARK IT is_diff KNOWING THERE IS A HIGHER CONFIDENCE
                        "name": "past_is_diff",
                        "value": diff_by_association,
                        "sort": {"value": test_param.sort.name, "sort": -1}
                    }
                ]
            })

            #PICK THE BEST SCORE FOR EACH is_diff==True REGION
            for g2, data in Q.groupby(stats, "is_diff", contiguous=True):
                if g2.is_diff:
                    best = Q.sort(data, ["ttest_result.confidence", "diff"]).last()
                    best["pass"] = True

            all_touched.update(Q.select(test_results, [test_param.select.datazilla.name, settings.param.test_dimension]))

            # TESTS THAT HAVE BEEN (RE)EVALUATED GIVEN THE NEW INFORMATION
            evaled_tests.update(Q.run({
                "from": test_results,
                "select": [test_param.select.datazilla.name, settings.param.test_dimension],
                "where": {"term": {"ignored": False}}
            }))

            File("test_values.txt").write(CNV.list2tab(Q.run({
                "from": stats,
                "select": [
                    {"name": test_param.sort.name, "value": lambda x: CNV.datetime2string(CNV.milli2datetime(x[test_param.sort.name]), "%d-%b-%Y %H:%M:%S")},
                    "value",
                    {"name": "revision", "value": settings.param.revision_dimension},
                    {"name": "mtest_confidence", "value": "result.confidence"},
                    {"name": "ttest_confidence", "value": "ttest_result.confidence"},
                    "is_diff",
                    "pass"
                ],
                "sort": test_param.sort.name
            })))

            #TESTS THAT HAVE SHOWN THEMSELVES TO BE EXCEPTIONAL
            new_exceptions = Q.filter(stats, {"term": {"pass": True}})
            for v in new_exceptions:
                if v.ignored:
                    continue
                alert = Struct(
                    status="new",
                    create_time=CNV.milli2datetime(v[test_param.sort.name]),
                    tdad_id=wrap_dot({
                        test_param.select.datazilla.name: v[test_param.select.datazilla.name],
                        settings.param.test_dimension: v[settings.param.test_dimension]
                    }),
                    reason=settings.param.reason,
                    revision=v[settings.param.revision_dimension],
                    details=v,
                    severity=settings.param.severity,
                    confidence=v.result.confidence
                )
                alerts.append(alert)

            if debug:
                Log.note("{{num}} new exceptions found", {"num": len(new_exceptions)})

        except Exception, e:
            Log.warning("Problem with alert identification, continue to log existing alerts and stop cleanly", e)

        break

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
                {"term": {"reason": settings.param.reason}}
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
        "where": esfilter2sqlwhere(alerts_db, {"term": {"code": settings.param.reason}})
    })

    alerts_db.flush()

    if debug:
        Log.note("Marking {{num}} {{ids}} as 'done'", {
            "num": len(all_touched),
            "ids": settings.param.default.select.datazilla.name
        })

    for g, t in Q.groupby(all_touched, settings.param.test_dimension):
        try:
            qb.update({
                "set": {settings.param.mark_complete: "done"},
                "where": {"and": [
                    {"terms": {test_param.select.datazilla.value: Q.select(t, test_param.select.datazilla.name)}},
                    {"term": {settings.param.test_dimension: g[settings.param.test_dimension]}},
                    {"missing": {"field": settings.param.mark_complete}}
                ]}
            })
        except Exception, e:

            Log.warning("Can not mark as done", e)


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
            #MORE SETTINGS
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




