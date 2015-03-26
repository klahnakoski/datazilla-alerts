# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import unicode_literals
from __future__ import division

from dzAlerts.daemons.util import update_alert_status
from dzAlerts.daemons.util.median_test import median_test
from dzAlerts.daemons.util.welchs_ttest import welchs_ttest
from pyLibrary.collections import MIN, MAX
from pyLibrary.env.files import File
from pyLibrary.maths import Math
from pyLibrary.queries.qb_usingES import FromES
from pyLibrary.debugs import startup, constants
from pyLibrary import convert, queries
from pyLibrary.queries import windows
from pyLibrary.queries.query import Query
from pyLibrary.debugs.logs import Log
from pyLibrary.queries import qb
from pyLibrary.sql.mysql import MySQL
from pyLibrary.dot import Null, split_field, literal_field, set_default, Dict, nvl, wrap_dot, listwrap
from pyLibrary.thread.threads import Thread
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import Duration
from pyLibrary.times.timer import Timer


NOW = Date.today()
MAX_AGE = Duration(days=90)

TEMPLATE = """<div><h3>{{score}} - {{reason}}</h3><br>
On page {{page_url}}<br>
<a href=\"https://treeherder.mozilla.org/#/jobs?repo={{repo}}&revision={{revision}}\">Treeherder</a><br>
<a href=\"https://hg.mozilla.org/rev/{{revision}}\">Mercurial</a><br>
<a href=\"https://bugzilla.mozilla.org/show_bug.cgi?id={{bug_id}}\">Bugzilla - {{bug_description}}</a><br>
<a href=\"https://datazilla.mozilla.org/?start={{push_date_min}}&stop={{push_date_max}}&product={{product}}&repository={{branch}}&os={{operating_system_name}}&os_version={{operating_system_version}}&test={{test_name}}&graph_search={{revision}}&error_bars=false&project=talos\">Datazilla</a><br>
<a href=\"http://people.mozilla.com/~klahnakoski/test/es/DZ-ShowPage.html# page={{page_url}}&sampleMax={{push_date}}000&sampleMin={{push_date_min}}000&branch={{branch}}\">Kyle's ES</a><br>
Raw data:  {{details}}
</div>"""

VERBOSE = True
DEBUG = False  # SETTINGS CAN TURN ON/OFF DEBUGGING
DEBUG_TOUCH_ALL_ALERTS = False  # True IF ALERTS WILL BE UPDATED, EVEN IF THE QUALITY IS NO DIFFERENT


def diff_percent(r):
    try:
        if r.past_stats.mean==0:
            return 1
        else:
            if r.past_stats.mean==None:
                return Null
            return (r.future_stats.mean - r.past_stats.mean) / r.past_stats.mean
    except Exception, e:
        Log.error("" + str(Null / Null), e)



def get_settings_pre(settings, qb):
    # THIS COMPLICATED CODE IS SIMPLY USING THE
    # SETTINGS TO MAP branch, suite, test OVERRIDES
    # TO THE default SETTINGS
    fields = qb.normalize_edges(settings.param.test_dimension)
    disabled = []
    exists = []
    for f in fields:
        k = nvl(f.name, "test")
        k = split_field(k)[-1]
        k = "test" if k == "name" else k

        disabled.append(
            {"not": {"terms": {f.value: [s for s, p in settings.param[k].items() if p.disable]}}}
        )
        exists.append(
            {"exists": {"field": f.value}}
        )
    disabled.append({"not": {"terms": {qb.normalize_edges(settings.param.branch_dimension)[0].value: [t for t, p in settings.param.branch.items() if p.disable]}}})
    return disabled, exists, fields


def get_parameters_specific(settings, fields, g):
    """
    ANOTHER COMPLICATED PARAMETER EXTRACTION ROUTINE
    TODO: MAKE A GENERALIZED PARAMETER STANDARD TO SIMPLIFY THIS LOGIC
    EG: LIST OF {"where":<condition>, "values":<overrides>} TO CONSTRUCT A SOPHISTICATED PARAMETER OBJECT
    """
    lookup = []
    for f in qb.reverse(listwrap(fields)):
        if isinstance(f, basestring):
            lookup.append(settings.param.test[literal_field(g[settings.param.test_dimension])])
        elif f.name and f.value:
            k = split_field(f.name)[-1]
            lookup.append(settings.param['test' if k == 'name' else k][literal_field(g[f.name])])
        else:
            for k, v in f.items():
                lookup.append(settings.param['test' if k == 'name' else k][literal_field(g[settings.param.test_dimension][k])])
    lookup.append(settings.param.branch[literal_field(g[settings.param.branch_dimension])])
    lookup.append(settings.param.default)
    test_param = set_default(*lookup)
    return test_param


def alert_sustained_median(settings, qb, alerts_db):
    """
    find single points that deviate from the trend
    """

    # OBJECTSTORE = settings.objectstore.schema + ".objectstore"
    oldest_ts = (NOW - MAX_AGE).milli
    verbose = nvl(settings.param.verbose, VERBOSE)
    if settings.param.debug == None:
        debug = DEBUG or DEBUG_TOUCH_ALL_ALERTS
    else:
        debug = settings.param.debug

    if debug:
        Log.alert("Sustained Median Debugging is ON")
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
        disabled, exists, fields = get_settings_pre(settings, qb)
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
                    nvl(settings.param.debug_filter, True)
                ]}
            ]},
            "format": "table",  # SPARSENESS REQUIRED
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
    for g, min_push_date in qb.groupby(new_test_points, query.edges):
        if not min_push_date:
            continue
        try:
            # FIND SPECIFIC PARAMETERS FOR THIS SLICE
            test_param = get_parameters_specific(settings, fields, g)

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
                        [s for s in source_ref if not s.name.startswith("Talos.Test") and not s.name.startswith("B2G.Test")] +  # TODO: FIX BIG HACK!  WE SHOULD HAVE A WAY TO UNION() THE SELECT CLAUSE
                        query.edges,
                    "where": {"and": [
                        {"term": g},
                        {"range": {test_param.sort.value: {"gte": min_date}}}
                    ]},
                },
                "sort": test_param.sort.name
            })

            all_touched.update(qb.select(all_test_results, source_ref.name))

            # REMOVE ALL TESTS EXCEPT MOST RECENT FOR EACH REVISION
            # REMOVE TESTS MISSING A VALUE
            # THIS MAKES THE PAST WINDOW TOO SMALL, THEN FORCING A RECALC.
            # THIS CONVERGES TO 'DONE' EVENTUALLY
            test_results = qb.run({
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
                "start_date": convert.milli2datetime(min_date)
            })

            if verbose:
                Log.note("Find sustained_median exceptions")

            def diff_by_association(r, i, rows):
                # MARK IT DIFF IF IT IS IN THE T-TEST MOUNTAIN OF HIGH SCORE
                if rows[i - 1].is_diff and r.ttest_result.score > test_param.min_mscore:
                    r.is_diff = True
                return None

            # APPLY WINDOW FUNCTIONS
            stats = qb.run({
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
                        "value": diff_percent
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
            for g2, data in qb.groupby(stats, "is_diff", contiguous=True):
                if g2.is_diff:
                    best = qb.sort(data, ["ttest_result.score", "diff"]).last()
                    if best.ttest_result.score > test_param.min_tscore:
                        best["pass"] = True

            # TESTS THAT HAVE BEEN (RE)EVALUATED GIVEN THE NEW INFORMATION
            evaled_tests.update(qb.run({
                "from": test_results,
                "select": source_ref.name,
                "where": {"term": {"ignored": False}}
            }))

            if debug:
                File("test_values.txt").write(convert.list2tab(qb.run({
                    "from": stats,
                    "select": [
                        {"name": test_param.sort.name, "value": lambda x: convert.datetime2string(convert.milli2datetime(x[test_param.sort.name]), "%d-%b-%Y %H:%M:%S")},
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
            new_exceptions = qb.filter(stats, {"term": {"pass": True}})
            for v in new_exceptions:
                if v.ignored:
                    continue
                alert = Dict(
                    status="NEW",
                    push_date=convert.milli2datetime(v[test_param.sort.name]),
                    tdad_id=wrap_dot({
                        s.name: v[s.name] for s in source_ref
                    }),
                    reason=settings.param.reason,
                    revision=v[settings.param.revision_dimension],
                    details=v,
                    severity=settings.param.severity,
                    confidence=v.ttest_result.score,
                    branch=v[settings.param.branch_dimension],
                    test=v[settings.param.test_dimension],
                    platform=nvl(v.B2G.Device, v.Eideticker.Device, v.Talos.OS.name + " " + v.Talos.OS.version, v.Device),
                    percent=str(round(v.diff_percent * 100, 1)) + "%",
                    keyrevision=v[settings.param.revision_dimension],
                    mergedfrom=''
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
        old_alerts = Null
    else:
        old_alerts = []
        # TODO: SOLVE THIS PROBLEM:  THE WIDE DATA REQUIREMENTS ARE MAKING LARGE SQL STATEMENTS
        # REALLY, THEY ARE LONG LISTS OF DATA, SO THERE IS OPPORTUNITY FOR COMPRESSION;
        # WE COULD CREATE TABLE, LOAD TABLE, THEN EXECUTE QUERY USING A JOIN
        # WE COULD SEND A STORED PROCEDURE, AND THEN CALL IT WITH THE DATA (BUT IS THAT SMALLER?)
        for i, et in qb.groupby(evaled_tests, size=100):  # SMALLER SQL STATEMENTS
            old_alerts.extend(MySQL(alerts_db).query({
                "from": "alerts",
                "select": [
                    "id",
                    "tdad_id",
                    "status",
                    "last_updated",
                    "severity",
                    "confidence",
                    "details",
                    "comment",
                    "branch",
                    "test",
                    "platform",
                    "percent",
                    "keyrevision",
                    "mergedfrom"
                ],
                "where": {"and": [
                    {"terms": {"tdad_id": et}},
                    {"term": {"reason": settings.param.reason}}
                ]}
            }))

    update_alert_status(settings, alerts_db, alerts, old_alerts)

    if verbose:
        Log.note("Marking {{num}} {{ids}} as 'done'", {
            "num": len(all_touched),
            "ids": source_ref.name
        })

    for g, t in qb.groupby(all_touched, source_ref.leftBut(1).name):
        try:
            qb.update({
                "set": {settings.param.mark_complete: "done"},
                "where": {"and": [
                    {"and": [
                        {"terms": {source_ref.last().value: qb.select(t, source_ref.last().name)}}
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
    constants.set(settings.constants)
    try:
        with startup.SingleInstance(flavor_id=settings.args.filename):
            # MORE SETTINGS
            Log.note("Finding exceptions in index {{index_name}}", {"index_name": settings.query["from"].name})

            es_settings = settings.query["from"].settings
            queries.config.default.settings = es_settings

            with FromES(es_settings) as qb:
                qb.addDimension(settings.dimension)

                with MySQL(settings.alerts) as alerts_db:
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




