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
from dzAlerts.daemons.alert import update_alert_status

from dzAlerts.daemons.util import significant_difference, significant_score_difference
from dzAlerts.util.cnv import CNV
from dzAlerts.util.env import startup
from dzAlerts.util.env.elasticsearch import ElasticSearch
from dzAlerts.util.env.files import File
from dzAlerts.util.maths import Math
from dzAlerts.util.queries.db_query import esfilter2sqlwhere, DBQuery
from dzAlerts.util.queries.es_query import ESQuery
from dzAlerts.util.sql.db import DB, SQL
from dzAlerts.util.env.logs import Log
from dzAlerts.util.queries import Q
from dzAlerts.util.struct import nvl, StructList
from dzAlerts.util.times.durations import Duration


SUSTAINED_REASON = "b2g_alert_sustained_median"
REASON = "b2g_alert_revision"   # name of the reason in alert_reason
LOOK_BACK = Duration(days=90)
MIN_AGE = Duration(hours=2)
NOW = datetime.utcnow()
SEVERITY = 0.7

# What needs to be in a notifications email?
#      * Link to datazilla graph centered on event that triggered notification.  Ideally, highlight the regression range datapoints.
#      * Gaia git revision before and after event.  Preferably as a github compare URL.
#      * Gecko hg revision before and after event.  Preferably as a pushlog URL.
#      * Also nice to have Gecko git revisions before and after event.  Some people prefer git for gecko, but they seem the minority.
#      * Firmware version before and after event; reported in datazilla as fields starting with "Firmware".
#      * Device type; hamachi vs inari vs tarako, etc
#      * b2gperf version (not currently reported in datazilla)
#      * Summary statistics for the regression; mean, median, stdev before and after event
#
SUBJECT = [
    "[ALERT][B2G] {{details.example.B2G.Test.name}} regressed by {{details.example.diff|round(digits=2)}}{{details.example.units}} in ",
    {
        "from": "details.tests",
        "template": "{{test.suite}}",
        "separator": ", "
    }
    ]
TEMPLATE = [
    """
    <div>
    	<div style="font-size: 150%;font-weight: bold;">Score: {{score|round(digits=3)}}</div><br>
    <span style="font-size: 120%; display:inline-block">Gaia: <a href="https://github.com/mozilla-b2g/gaia/commit/{{revision.gaia}}">{{revision.gaia|left(12)}}...</a></span>
    [<a href="https://github.com/mozilla-b2g/gaia/commit/{{details.example.past_revision.gaia}}">Previous</a>]<br>

    <span style="font-size: 120%; display:inline-block">Gecko: <a href="http://git.mozilla.org/?p=releases/gecko.git;a=commit;h={{revision.gecko}}">{{revision.gecko}}</a></span>
    [<a href="http://git.mozilla.org/?p=releases/gecko.git;a=commit;h={{details.example.past_revision.gecko}}">Previous</a>]

    <br>
    <br>
    {{details.total_exceptions}} exceptional events:<br>
    <table>
    <thead><tr><td>Device</td><td>Suite</td><td>Test Name</td><td>DZ Link</td><td>Github Diff</td><td>Date/Time</td><td>Before</td><td>After</td><td>Diff</td></tr></thead>
    """, {
        "from": "details.tests",
        "template": """<tr>
            <td>{{example.B2G.Device|upper}}</td>
            <td>{{test.suite}}</td>
            <td>{{test.name}}</td>
            <td><a href="https://datazilla.mozilla.org/b2g/?branch={{example.B2G.Branch}}&device={{example.B2G.Device}}&range={{example.date_range}}&test={{test.name}}&app_list={{test.suite}}&gaia_rev={{example.B2G.Revision.gaia}}&gecko_rev={{example.B2G.Revision.gecko}}&plot=median\">Datazilla!</a></td>
            <td><a href="https://github.com/mozilla-b2g/gaia/compare/{{example.past_revision.gaia}}...{{example.B2G.Revision.gaia}}">DIFF</a></td>
            <td>{{example.push_date|datetime}}</td>
            <td>{{example.past_stats.mean|round(digits=4)}}</td>
            <td>{{example.future_stats.mean|round(digits=4)}}</td>
            <td>{{example.diff|round(digits=2)}}</td>
            </tr>
        """
    },
    """</table></div>"""
]

#GET ACTIVE ALERTS
# assumes there is an outside agent corrupting our test results
# this will look at all alerts on a revision, and figure out the probability there is an actual regression

def b2g_alert_revision(settings):
    assert settings.alerts != None
    settings.db.debug = settings.param.debug
    with DB(settings.alerts) as alerts_db:
        with ESQuery(ElasticSearch(settings.query["from"])) as esq:
            dbq = DBQuery(alerts_db)

            esq.addDimension(CNV.JSON2object(File(settings.dimension.filename).read()))

            #TODO: REMOVE, LEAVE IN DB
            if alerts_db.debug:
                alerts_db.execute("update reasons set email_subject={{subject}}, email_template={{template}} where code={{reason}}", {
                    "template": CNV.object2JSON(TEMPLATE),
                    "subject": CNV.object2JSON(SUBJECT),
                    "reason": REASON
                })
                alerts_db.flush()

            #EXISTING SUSTAINED EXCEPTIONS
            existing_sustained_alerts = dbq.query({
                "from": "alerts",
                "select": "*",
                "where": {"and": [
                    {"term": {"reason": settings.param.reason}},
                    {"not": {"term": {"status": "obsolete"}}},
                    {"range": {"create_time": {"lt": NOW - MIN_AGE}}},  # DO NOT ALERT WHEN TOO YOUNG
                    {"range": {"create_time": {"gte": NOW - LOOK_BACK}}}
                ]}
            })

            tests = Q.index(existing_sustained_alerts, ["revision", "details.B2G.Test"])

            #SUMMARIZE
            alerts = StructList()

            total_tests = esq.query({
                "from": "b2g_alerts",
                "select": {"name": "count", "aggregate": "count"},
                "edges": [
                    "B2G.Revision"
                ],
                "where": {"and": [
                    {"terms": {"B2G.Revision": list(set(existing_sustained_alerts.revision))}}
                ]}
            })

            # GROUP BY ONE DIMENSION ON 1D CUBE IS REALLY JUST ITERATING OVER THAT DIMENSION, BUT EXPENSIVE
            for revision, total_test_count in Q.groupby(total_tests, ["B2G.Revision"]):
            #FIND TOTAL TDAD FOR EACH INTERESTING REVISION
                revision = revision["B2G.Revision"]
                total_exceptions = tests[(revision, )]  # FILTER BY revision

                parts = StructList()
                for g, exceptions in Q.groupby(total_exceptions, ["details.B2G.Test"]):
                    worst_in_test = Q.sort(exceptions, ["confidence", "details.diff_percent"]).last()
                    example = worst_in_test.details

                    num_except = len(exceptions)
                    if num_except == 0:
                        continue

                    part = {
                        "test": g.details.B2G.Test,
                        "num_exceptions": num_except,
                        "num_tests": total_test_count,
                        "confidence": worst_in_test.confidence,
                        "example": example
                    }
                    parts.append(part)

                parts = Q.sort(parts, [{"field": "confidence", "sort": -1}])
                worst_in_revision = parts[0].example

                alerts.append({
                    "status": "new",
                    "create_time": CNV.milli2datetime(worst_in_revision.push_date),
                    "reason": REASON,
                    "revision": revision,
                    "tdad_id": revision,
                    "details": {
                        "revision": revision,
                        "total_tests": total_test_count,
                        "total_exceptions": len(total_exceptions),
                        "tests": parts,
                        "example": worst_in_revision
                    },
                    "severity": SEVERITY,
                    "confidence": nvl(worst_in_revision.result.score, -Math.log10(1-worst_in_revision.result.confidence), 8)  # confidence was never more accurate than 8 decimal places
                })


            #EXISTING REVISION-LEVEL ALERTS
            old_alerts = dbq.query({
                "from": "alerts",
                "select": "*",
                "where": {"and": [
                    {"term": {"reason": REASON}},
                    {"range": {"create_time": {"gte": NOW - LOOK_BACK}}},
                    {"or": [
                        {"terms": {"revision": set(existing_sustained_alerts.revision)}},
                        {"term": {"reason": SUSTAINED_REASON}},
                        {"term": {"status": "obsolete"}},
                        {"range": {"create_time": {"gte": NOW - LOOK_BACK}}}
                    ]}
                ]},
                # "sort":"status",
                # "limit":10
            })

            found_alerts = Q.unique_index(alerts, "revision")
            old_alerts = Q.unique_index(old_alerts, "revision")

            update_alert_status(settings, alerts_db, found_alerts, old_alerts)

            #SHOW SUSTAINED ALERTS ARE COVERED
            alerts_db.execute("""
                INSERT INTO hierarchy (parent, child)
                SELECT
                    r.id parent,
                    p.id child
                FROM
                    alerts p
                LEFT JOIN
                    hierarchy h on h.child=p.id
                LEFT JOIN
                    alerts r on r.revision=p.revision AND r.reason={{parent_reason}}
                WHERE
                    {{where}}
            """, {
                "where": esfilter2sqlwhere(alerts_db, {"and": [
                    {"term": {"p.reason": settings.param.reason}},
                    {"terms": {"p.revision": Q.select(existing_sustained_alerts, "revision")}},
                    {"missing": "h.parent"}
                ]}),
                "parent_reason": REASON
            })


def main():
    settings = startup.read_settings()
    Log.start(settings.debug)
    try:
        Log.note("Summarize by revision {{schema}}", {"schema": settings.perftest.schema})
        b2g_alert_revision(settings)
    finally:
        Log.stop()


if __name__ == '__main__':
    main()
