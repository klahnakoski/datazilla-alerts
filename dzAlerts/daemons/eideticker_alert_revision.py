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
from dzAlerts.daemons.talos_alert_revision import MECURIAL_PATH, TBPL_PATH

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
from dzAlerts.util.struct import nvl, StructList, Struct
from dzAlerts.util.times.dates import Date
from dzAlerts.util.times.durations import Duration


SUSTAINED_REASON = "eideticker_alert_sustained_median"
REASON = "eideticker_alert_revision"   # name of the reason in alert_reason
LOOK_BACK = Duration(days=90)
NOW = datetime.utcnow()
SEVERITY = 0.7
DEBUG_TOUCH_ALL_ALERTS = False  # True IF ALERTS WILL BE UPDATED, EVEN IF THE QUALITY IS NO DIFFERENT
TEST_REVISION = '2d88803a0b9c'

SUBJECT = "[ALERT][Eideticker] {{details.example.Eideticker.Test}} regressed by {{details.example.diff_percent|percent(digits=2)}} in {{details.example.Eideticker.Branch}}";

TEMPLATE = [
    """
    <div>
    <div style="font-size: 150%;font-weight: bold;">Score: {{score|round(digits=3)}}</div><br>
    <span style="font-size: 120%; display:inline-block">Revision: <a href="http://git.mozilla.org/?p=releases/gecko.git;a=commit;h={{revision}}">{{revision}}</a></span>
    [<a href="http://git.mozilla.org/?p=releases/gecko.git;a=commit;h={{details.example.past_revision}}">Previous</a>]

    <br>
    <br>
    {{details.total_exceptions}} exceptional events:<br>
    <table>
    <thead><tr>
    <td>Device</td>
    <td>Suite</td>
    <td>Test Name</td>
    <td>Eideticker</td>
    <td>Charts</td>
    <td>DIFF</td>
    <td>Date/Time</td>
    <td>Before</td>
    <td>After</td>
    <td>Diff</td>
    </tr></thead>
    """, {
        "from": "details.tests",
        "template": """<tr>
            <td>{{example.Eideticker.Device|upper}}</td>
            <td>{{test.suite}}</td>
            <td>{{test.name}}</td>
            <td><a href="http://eideticker.mozilla.org{{example.eideticker.url.path}}/#/{{example.Eideticker.Device}}/{{example.Eideticker.Branch}}/{{example.Eideticker.Test}}/{{example.eideticker.url.metricname}}">Eideticker</a></td>
            <td><a href="http://people.mozilla.org/~klahnakoski/talos/Alert-Eideticker.html#{{example.charts.url|url}}">charts!</a></td>
            <td><a href="https://github.com/mozilla-b2g/gaia/compare/{{example.past_revision.gaia}}...{{example.Eideticker.Revision.gaia}}">DIFF</a></td>
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

def eideticker_alert_revision(settings):
    assert settings.alerts != None
    settings.db.debug = settings.param.debug
    debug = settings.param.debug



    with DB(settings.alerts) as db:
        with ESQuery(ElasticSearch(settings.query["from"])) as esq:
            dbq = DBQuery(db)

            esq.addDimension(CNV.JSON2object(File(settings.dimension.filename).read()))

            #TODO: REMOVE, LEAVE IN DB
            if db.debug:
                db.execute("update reasons set email_subject={{subject}}, email_template={{template}} where code={{reason}}", {
                    "template": CNV.object2JSON(TEMPLATE),
                    "subject": CNV.object2JSON(SUBJECT),
                    "reason": REASON
                })
                db.flush()

            #EXISTING SUSTAINED EXCEPTIONS
            existing_sustained_alerts = dbq.query({
                "from": "alerts",
                "select": "*",
                "where": {"and": [
                    {"term": {"reason": settings.param.reason}},
                    {"not": {"term": {"status": "obsolete"}}},
                    True if DEBUG_TOUCH_ALL_ALERTS else {"range": {"create_time": {"gte": NOW - LOOK_BACK}}}
                ]}
            })

            tests = Q.index(existing_sustained_alerts, ["revision", "details.Eideticker.Test"])

            #EXISTING REVISION-LEVEL ALERTS
            old_alerts = dbq.query({
                "from": "alerts",
                "select": "*",
                "where": {"and": [
                    {"term": {"reason": REASON}},
                    {"or":[
                        {"terms": {"revision": set(existing_sustained_alerts.revision)}},
                        {"term": {"status": "obsolete"}},
                        {"range": {"create_time": {"gte": NOW - LOOK_BACK}}}
                    ]}
                ]}
            })
            old_alerts = Q.unique_index(old_alerts, "revision")

            #SUMMARIZE
            known_alerts = StructList()

            total_tests = esq.query({
                "from": "eideticker_alerts",
                "select": {"name": "count", "aggregate": "count"},
                "edges": [
                    "Eideticker.Revision"
                ],
                "where": {"and": [
                    {"terms": {"Eideticker.Revision": list(set(existing_sustained_alerts.revision))}}
                ]}
            })

            # GROUP BY ONE DIMENSION ON 1D CUBE IS REALLY JUST ITERATING OVER THAT DIMENSION, BUT EXPENSIVE
            for revision, total_test_count in Q.groupby(total_tests, ["Eideticker.Revision"]):
            #FIND TOTAL TDAD FOR EACH INTERESTING REVISION
                revision = revision["Eideticker.Revision"]
                total_exceptions = tests[(revision, )]  # FILTER BY revision

                parts = StructList()
                for g, exceptions in Q.groupby(total_exceptions, ["details.Eideticker.Test"]):
                    worst_in_test = Q.sort(exceptions, ["confidence", "details.diff_percent"]).last()
                    example = worst_in_test.details
                    # ADD SOME SPECIFIC URL PARAMETERS
                    branch = example.Eideticker.Branch
                    stop = Math.max(example.push_date_max, (2*example.push_date) - example.push_date_min)

                    example.mercurial.url.branch = branch
                    example.eideticker.url = Struct(
                        metricname="timetostableframe",
                        path=nvl(example.path, "")
                    )
                    example.charts.url = Struct(
                        sampleMin=Date(example.push_date_min).floor().format("%Y-%m-%d"),
                        sampleMax=Date(stop).floor().format("%Y-%m-%d"),
                        test=example.Eideticker.Test,
                        branch=example.Eideticker.Branch,
                        device=example.Eideticker.Device
                    )

                    num_except = len(exceptions)
                    if num_except == 0:
                        continue

                    part = {
                        "test": g.details.Eideticker.Test,
                        "num_exceptions": num_except,
                        "num_tests": total_test_count,
                        "confidence": worst_in_test.confidence,
                        "example": example
                    }
                    parts.append(part)

                parts = Q.sort(parts, [{"field": "confidence", "sort": -1}])
                worst_in_revision = parts[0].example

                known_alerts.append({
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

            known_alerts = Q.unique_index(known_alerts, "revision")

            #NEW ALERTS, JUST INSERT
            new_alerts = known_alerts - old_alerts
            if new_alerts:
                for revision in new_alerts:
                    revision.id = SQL("util.newid()")
                    revision.last_updated = NOW
                db.insert_list("alerts", new_alerts)

            #SHOW SUSTAINED ALERTS ARE COVERED
            db.execute("""
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
                "where": esfilter2sqlwhere(db, {"and": [
                    {"term": {"p.reason": settings.param.reason}},
                    {"terms": {"p.revision": Q.select(existing_sustained_alerts, "revision")}},
                    {"missing": "h.parent"}
                ]}),
                "parent_reason": REASON
            })

            #CURRENT ALERTS, UPDATE IF DIFFERENT
            changed_alerts = known_alerts & old_alerts
            for changed_alert in changed_alerts:
                if len(nvl(changed_alert.solution, "").strip()) != 0:
                    continue  # DO NOT TOUCH SOLVED ALERTS

                old_alert = old_alerts[changed_alert]
                if DEBUG_TOUCH_ALL_ALERTS or old_alert.status == 'obsolete' or significant_difference(changed_alert.severity, old_alert.severity) or significant_score_difference(changed_alert.confidence, old_alert.confidence):
                    changed_alert.last_updated = NOW
                    db.update("alerts", {"id": old_alert.id}, changed_alert)

                    if DEBUG_TOUCH_ALL_ALERTS:
                        db.execute("UPDATE alerts SET last_sent=NULL WHERE id={{id}}", {"id": old_alert.id})

            #OLD ALERTS, OBSOLETE
            for old_alert in old_alerts - known_alerts:
                if old_alert.status == 'obsolete':
                    continue
                db.update("alerts", {"id": old_alert.id}, {"status": "obsolete", "last_updated": NOW})


def main():
    settings = startup.read_settings()
    Log.start(settings.debug)
    try:
        Log.note("Summarize by revision {{schema}}", {"schema": settings.perftest.schema})
        eideticker_alert_revision(settings)
    finally:
        Log.stop()


if __name__ == '__main__':
    main()