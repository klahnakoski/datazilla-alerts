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
from dzAlerts.daemons.util import update_alert_status

from dzAlerts.util.collections import MIN, MAX
from dzAlerts.util.env.elasticsearch import ElasticSearch
from dzAlerts.util.env.files import File
from dzAlerts.util.maths import Math
from dzAlerts.util.queries.es_query import ESQuery
from dzAlerts.util.env import startup
from dzAlerts.util.queries.db_query import DBQuery
from dzAlerts.daemons.util.median_test import median_test
from dzAlerts.daemons.util.welchs_ttest import welchs_ttest
from dzAlerts.util.cnv import CNV
from dzAlerts.util.queries import windows
from dzAlerts.util.queries.query import Query
from dzAlerts.util.struct import nvl, StructList, literal_field, split_field
from dzAlerts.util.env.logs import Log
from dzAlerts.util.struct import Struct, set_default
from dzAlerts.util.queries import Q
from dzAlerts.util.sql.db import DB
from dzAlerts.util.structs.wraps import wrap_dot, listwrap
from dzAlerts.util.thread.threads import Thread
from dzAlerts.util.times.durations import Duration
from dzAlerts.util.times.timer import Timer


NOW = datetime.utcnow()
MAX_AGE = Duration(days=90)

TEMPLATE = """<div><h3>{{score}} - {{reason}}</h3><br>
On page {{page_url}}<br>
<a href=\"https://tbpl.mozilla.org/?tree={{branch}}&rev={{revision}}\">TBPL</a><br>
<a href=\"https://hg.mozilla.org/rev/{{revision}}\">Mercurial</a><br>
<a href=\"https://bugzilla.mozilla.org/show_bug.cgi?id={{bug_id}}\">Bugzilla - {{bug_description}}</a><br>
<a href=\"https://datazilla.mozilla.org/?start={{push_date_min}}&stop={{push_date_max}}&product={{product}}&repository={{branch}}&os={{operating_system_name}}&os_version={{operating_system_version}}&test={{test_name}}&graph_search={{revision}}&error_bars=false&project=talos\">Datazilla</a><br>
<a href=\"http://people.mozilla.com/~klahnakoski/test/es/DZ-ShowPage.html# page={{page_url}}&sampleMax={{push_date}}000&sampleMin={{push_date_min}}000&branch={{branch}}\">Kyle's ES</a><br>
Raw data:  {{details}}
</div>"""

VERBOSE = True
DEBUG = True  # SETTINGS CAN TURN OFF DEBUGGING
DEBUG_TOUCH_ALL_ALERTS = False  # True IF ALERTS WILL BE UPDATED, EVEN IF THE QUALITY IS NO DIFFERENT


def alert_sustained_median(settings, qb, alerts_db):
    """
    find single points that deviate from the trend
    """

    # OBJECTSTORE = settings.objectstore.schema + ".objectstore"
    oldest_ts = CNV.datetime2milli(NOW - MAX_AGE)
    verbose = nvl(settings.param.verbose, VERBOSE)
    debug = True if settings.param.debug is False else DEBUG or DEBUG_TOUCH_ALL_ALERTS  # SETTINGS CAN TURN OFF DEBUGGING
    if debug:
        Log.warning("Debugging is ON")
    query = settings.query

    def is_bad(r, test_param):
        if test_param.min_mscore < r.result.score:
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

            if diff > min_diff:  # THIS MAY NEVER HAPPEN GIVEN THE MEDIAN TEST CAN MISS THE DISCONTINUITY
                return True

        return False

    with Timer("pull combinations"):
        # THIS COMPLICATED CODE IS SIMPLY USING THE
        # SETTINGS TO MAP branch, suite, test OVERRIDES
        # TO THE default SETTINGS
        fields = qb.normalize_edges(settings.param.test_dimension)

        disabled = []
        exists = []
        for f in fields:
            k = nvl(f.name, "test")
            k = split_field(k)[-1]
            k = "test" if k=="name" else k

            disabled.append(
                {"not": {"terms": {f.value: [s for s, p in settings.param[k].items() if p.disable]}}}
            )
            exists.append(
                {"exists": {"field": f.value}}
            )
        disabled.append({"not": {"terms": {qb.normalize_edges(settings.param.branch_dimension)[0].value: [t for t, p in settings.param.branch.items() if p.disable]}}})

        source_ref = qb.normalize_edges(settings.param.source_ref)


        temp = Query({
            "from": settings.query["from"],
            "select": {"name": "min_push_date", "value": settings.param.default.sort.value, "aggregate": "min"},
            "edges": query.edges,
            "where": {"and": [
                True if debug or settings.args.restart else {"or": [
                    {"missing": {"field": settings.param.mark_complete}},
                    {"not": {"term": {settings.param.mark_complete: "done"}}}
                ]},
                {"range": {settings.param.default.sort.value: {"gte": oldest_ts}}},
                {"and": exists},
                {"and": disabled},
                {"or": [
                    {"not": debug},
                    {"and": [
                        # FOR DEBUGGING SPECIFIC SERIES
                        # {"term": {"metadata.device": "flame"}},
                        # {"term": {"metadata.test": "b2g-gallery-startup"}},
                        # {"exists": {"field": "metadata.value"}}
                        # {"term": {"result.test_name": "fps"}}
                        # {"term": {"test_machine.type": "flame"}},
                        # {"term": {"metadata.app": "b2g-nightly"}}
                        # {"term":{"metadata.branch":"mozilla-central"}},
                        # {"term": {"metadata.test": "imgur"}},
                        # {"term": {"metadata.device": "samsung-gn"}},
                        # {"term": {"metadata.app": "nightly"}},
                        # {"term": {"testrun.suite": "tcanvasmark"}},
                        # {"term": {"result.test_name": "canvasmark"}},
                        # {"term": {"test_machine.platform": "x86_64"}},
                        # {"term": {"test_build.name": "Firefox"}},
                        # {"term": {"test_machine.osversion": "6.2.9200"}},
                        # {"term": {"test_build.branch": "Mozilla-Aurora"}}

                    ]}
                ]}
            ]},
            "limit": nvl(settings.param.combo_limit, 1000)
        }, qb)

        new_test_points = qb.query(temp)

    # BRING IN ALL NEEDED DATA
    if verbose:
        Log.note("Pull all data for {{num}} groups:\n{{groups}}", {
            "num": len(new_test_points),
            "groups": query.edges
        })

    all_touched = set()
    evaled_tests = set()
    alerts = []   # PUT ALL THE EXCEPTION ITEMS HERE
    for g, min_push_date in Q.groupby(new_test_points, query.edges):
        if not min_push_date:
            continue
        try:
            # FIND SPECIFIC PARAMETERS FOR THIS SLICE
            lookup = []
            for f in listwrap(qb.edges[settings.param.test_dimension].fields):
                if isinstance(f, basestring):
                    lookup.append(settings.param.test[literal_field(g[settings.param.test_dimension])])
                else:
                    for k, v in f.items():
                        lookup.append(settings.param['test' if k=='name' else k][literal_field(g[settings.param.test_dimension][k])])
            lookup.append(settings.param.branch[literal_field(g[settings.param.branch_dimension])])
            lookup.append(settings.param.default)
            test_param = set_default(*lookup)

            if settings.args.restart:
                first_sample = oldest_ts
            else:
                first_sample = MAX(MIN(min_push_date), oldest_ts)
            # FOR THIS g, HOW FAR BACK IN TIME MUST WE GO TO COVER OUR WINDOW_SIZE?
            first_in_window = qb.query({
                "select": {"name": "min_date", "value": test_param.sort.name, "aggregate": "min"},
                "from": {
                    "from": settings.query["from"],
                    "select": test_param.sort,
                    "where": {"and": [
                        {"term": g},
                        {"exists": {"field": test_param.select.value.value}},
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

            # LOAD TEST RESULTS FROM DATABASE
            all_test_results = qb.query({
                "from": {
                    "from": settings.query["from"],
                    "select": [
                        test_param.sort,
                        test_param.select.repo,
                        test_param.select.value,
                    ]+
                        [s for s in source_ref if not s.name.startswith("Talos.Test") and not s.name.startswith("B2G.Test")] +  # BIG HACK!  WE SHOULD HAVE A WAY TO UNION() THE SELECT CLAUSE
                        query.edges,
                    "where": {"and": [
                        {"term": g},
                        {"range": {test_param.sort.value: {"gte": min_date}}}
                    ]},
                },
                "sort": test_param.sort.name
            })

            all_touched.update(Q.select(all_test_results, source_ref.name))

            # REMOVE ALL TESTS EXCEPT MOST RECENT FOR EACH REVISION
            # REMOVE TESTS MISSING A VALUE
            # THIS MAKES THE PAST WINDOW TOO SMALL, THEN FORCING A RECALC.
            # THIS CONVERGES TO 'DONE' EVENTUALLY
            test_results = Q.run({
                "from": all_test_results,
                "window": [
                    {
                        "name": "redundant",
                        "value": lambda r, i, rows: True if r[settings.param.revision_dimension] == rows[i + 1][settings.param.revision_dimension] else None
                    }
                ],
                "where": {"and": [
                    {"missing": {"field": "redundant"}},
                    {"exists": {"field": test_param.select.value.name}}
                ]}
            })

            Log.note("{{num}} unique test results found for {{group}} dating back no further than {{start_date}}", {
                "num": len(test_results),
                "group": g,
                "start_date": CNV.milli2datetime(min_date)
            })

            if verbose:
                Log.note("Find sustained_median exceptions")

            def diff_by_association(r, i, rows):
                # MARK IT DIFF IF IT IS IN THE T-TEST MOUNTAIN OF HIGH SCORE
                if rows[i - 1].is_diff and r.ttest_result.score > test_param.min_mscore:
                    r.is_diff = True
                return None

            # APPLY WINDOW FUNCTIONS
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
                            interpolate=False,
                            resolution=test_param.resolution
                        ),
                        "sort": test_param.sort.name
                    }, {
                        "name": "diff",
                        "value": lambda r: r.future_stats.mean - r.past_stats.mean
                    }, {
                        "name": "diff_percent",
                        "value": lambda r: 1 if r.past_stats.mean==0 else (r.future_stats.mean - r.past_stats.mean) / r.past_stats.mean
                    }, {
                        "name": "is_diff",
                        "value": lambda r: is_bad(r, test_param)
                    }, {
                        # USE THIS TO FILL SCORE HOLES
                        # WE CAN MARK IT is_diff KNOWING THERE IS A HIGHER SCORE
                        "name": "future_is_diff",
                        "value": diff_by_association,
                        "sort": test_param.sort.name
                    }, {
                        # WE CAN MARK IT is_diff KNOWING THERE IS A HIGHER SCORE
                        "name": "past_is_diff",
                        "value": diff_by_association,
                        "sort": {"value": test_param.sort.name, "sort": -1}
                    }
                ]
            })

            # PICK THE BEST SCORE FOR EACH is_diff==True REGION
            for g2, data in Q.groupby(stats, "is_diff", contiguous=True):
                if g2.is_diff:
                    best = Q.sort(data, ["ttest_result.score", "diff"]).last()
                    if best.ttest_result.score > test_param.min_tscore:
                        best["pass"] = True

            # TESTS THAT HAVE BEEN (RE)EVALUATED GIVEN THE NEW INFORMATION
            evaled_tests.update(Q.run({
                "from": test_results,
                "select": source_ref.name,
                "where": {"term": {"ignored": False}}
            }))

            if debug:
                File("test_values.txt").write(CNV.list2tab(Q.run({
                    "from": stats,
                    "select": [
                        {"name": test_param.sort.name, "value": lambda x: CNV.datetime2string(CNV.milli2datetime(x[test_param.sort.name]), "%d-%b-%Y %H:%M:%S")},
                        "value",
                        {"name": "revision", "value": settings.param.revision_dimension},
                        {"name": "mtest_score", "value": "result.score"},
                        {"name": "ttest_score", "value": "ttest_result.score"},
                        "is_diff",
                        "pass"
                    ],
                    "sort": test_param.sort.name
                })))

            # TESTS THAT HAVE SHOWN THEMSELVES TO BE EXCEPTIONAL
            new_exceptions = Q.filter(stats, {"term": {"pass": True}})
            for v in new_exceptions:
                if v.ignored:
                    continue
                alert = Struct(
                    status="new",
                    create_time=CNV.milli2datetime(v[test_param.sort.name]),
                    tdad_id=wrap_dot({
                        s.name: v[s.name] for s in source_ref
                    }),
                    reason=settings.param.reason,
                    revision=v[settings.param.revision_dimension],
                    details=v,
                    severity=settings.param.severity,
                    confidence=v.ttest_result.score
                )
                alerts.append(alert)

            if verbose:
                Log.note("{{num}} new exceptions found", {"num": len(new_exceptions)})

        except Exception, e:
            Log.warning("Problem with alert identification.  Will continue with rest of other alerts and stop cleanly", e)

    if verbose:
        Log.note("Get Current Alerts")

    # CHECK THE CURRENT ALERTS
    if not evaled_tests:
        old_alerts = StructList.EMPTY
    else:
        old_alerts = DBQuery(alerts_db).query({
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

    update_alert_status(settings, alerts_db, alerts, old_alerts)

    if verbose:
        Log.note("Marking {{num}} {{ids}} as 'done'", {
            "num": len(all_touched),
            "ids": source_ref.name
        })

    for g, t in Q.groupby(all_touched, source_ref.leftBut(1).name):
        try:
            qb.update({
                "set": {settings.param.mark_complete: "done"},
                "where": {"and": [
                    {"and": [
                        {"terms": {source_ref.last().value: Q.select(t, source_ref.last().name)}}
                    ]},
                    {"term": g},
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
            # MORE SETTINGS
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
        Thread.sleep(seconds=2)
    finally:
        Log.stop()


if __name__ == '__main__':
    main()




