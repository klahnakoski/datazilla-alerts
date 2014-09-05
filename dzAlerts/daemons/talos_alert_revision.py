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

from datetime import datetime
from dzAlerts.daemons.util import update_alert_status
from dzAlerts.util.cnv import CNV
from dzAlerts.util.env import startup
from dzAlerts.util.env.elasticsearch import ElasticSearch
from dzAlerts.util.env.files import File
from dzAlerts.util.maths import Math
from dzAlerts.util.queries.db_query import esfilter2sqlwhere, DBQuery
from dzAlerts.util.queries.es_query import ESQuery
from dzAlerts.util.sql.db import DB
from dzAlerts.util.env.logs import Log
from dzAlerts.util.queries import Q
from dzAlerts.util.struct import nvl, StructList, Struct
from dzAlerts.util.times.dates import Date
from dzAlerts.util.times.durations import Duration


DEBUG_TOUCH_ALL_ALERTS = False
UPDATE_EMAIL_TEMPLATE = True
REASON = "talos_alert_revision"   # name of the reason in alert_reason
LOOK_BACK = Duration(days=90)
MIN_AGE = Duration(hours=2)
NOW = datetime.utcnow()
SEVERITY = 0.7

# FROM tcp://s4n4.qa.phx1.mozilla.com:3306/pushlog_hgmozilla_1/branches
MECURIAL_PATH = {
    "Firefox": "mozilla-central",
    "Try": "try",
    "B2G-Inbound": "integration/b2g-inbound",
    "Mozilla-Aurora": "releases/mozilla-aurora",
    "Mozilla-Beta": "releases/mozilla-beta",
    "Mozilla-Release": "releases/mozilla-release",
    "Mozilla-Esr10": "releases/mozilla-esr10",
    "Accessibility": "projects/accessibility",
    "Addon-SDK": "projects/addon-sdk",
    "Build-System": "projects/build-system",
    "Devtools": "projects/devtools",
    "Fx-Team": "integration/fx-team",
    "Ionmonkey": "projects/ionmonkey",
    "JägerMonkey": "projects/jaegermonkey",
    "Profiling": "projects/profiling",
    "Services-Central": "services/services-central",
    "UX": "projects/ux",
    "Alder": "projects/alder",
    "Ash": "projects/ash",
    "Birch": "projects/birch",
    "Cedar": "projects/cedar",
    "Elm": "projects/elm",
    "Holly": "projects/holly",
    "Larch": "projects/larch",
    "Maple": "projects/maple",
    "Oak": "projects/oak",
    "Pine": "projects/pine",
    "Electrolysis": "projects/electrolysis",
    "Graphics": "projects/graphics",
    "Places": "projects/places",
    "Mozilla-Inbound": "integration/mozilla-inbound",
}

TBPL_PATH = {
    "B2G-Inbound": "B2g-Inbound"
}


SUBJECT = "[ALERT][{{details.example.tbpl.url.branch}}] {{details.example.Talos.Test.name}} regressed by {{details.example.diff_percent|percent(digits=2)}} in {{details.example.Talos.Test.suite}}"

TEMPLATE = [
    """
    <div>
        <div style="font-size: 150%;font-weight: bold;">Score: {{score|round(digits=3)}}</div><br>
        <span style="font-size: 120%; display:inline-block">
        [<a href="https://hg.mozilla.org/{{details.example.mercurial.url.branch}}/rev/{{revision}}">{{revision}}</a>]
        </span>
        [<a href="https://hg.mozilla.org/{{details.example.mercurial.url.branch|lower}}/rev/{{details.example.past_revision}}">Previous</a>]
        [<a href="https://tbpl.mozilla.org/?tree={{details.example.tbpl.url.branch}}&rev={{revision}}">TBPL</a>]
    <br>
    <br>
    {{details.total_exceptions}} exceptional events:<br>
    <table>
    <thead><tr><td>Branch</td><td>Platform</td><td>Suite</td><td>Test Name</td><td></td><td></td><td>Diff</td><td>Date/Time</td><td>Before</td><td>After</td><td>Diff</td></tr></thead>
    """, {
        "from": "details.tests",
        "template": """<tr>
            <td>{{example.Talos.Product}} {{example.Talos.Branch}}</td>
            <td>{{example.Talos.OS.name}} ({{example.Talos.OS.version}})</td>
            <td>{{test.suite}}</td>
            <td>{{test.name}}</td>
            <td><a href="https://datazilla.mozilla.org/?{{example.datazilla.url|url}}">Datazilla!</a></td>
            <td><a href="http://people.mozilla.org/~klahnakoski/talos/Alert-Results.html#{{example.charts.url|url}}">charts!</a></td>
            <td><a href="">DIFF</a></td>
            <td>{{example.push_date|datetime}}</td>
            <td>{{example.past_stats.mean|round(digits=4)}}</td>
            <td>{{example.future_stats.mean|round(digits=4)}}</td>
            <td>{{example.diff|round(digits=2)}}</td>
            </tr>
        """
    },
    """</table></div>"""
]

# GET ACTIVE ALERTS
# assumes there is an outside agent corrupting our test results
# this will look at all alerts on a revision, and figure out the probability there is an actual regression

def talos_alert_revision(settings):
    assert settings.alerts != None
    settings.db.debug = settings.param.debug
    with DB(settings.alerts) as alerts_db:
        with ESQuery(ElasticSearch(settings.query["from"])) as esq:

            dbq = DBQuery(alerts_db)
            esq.addDimension(CNV.JSON2object(File(settings.dimension.filename).read()))

            # TODO: REMOVE, LEAVE IN DB
            if UPDATE_EMAIL_TEMPLATE:
                alerts_db.execute("update reasons set email_subject={{subject}}, email_template={{template}}, email_style={{style}} where code={{reason}}", {
                    "template": CNV.object2JSON(TEMPLATE),
                    "subject": CNV.object2JSON(SUBJECT),
                    "style": File("resources/css/email_style.css").read(),
                    "reason": REASON
                })
                alerts_db.flush()

            # EXISTING SUSTAINED EXCEPTIONS
            existing_sustained_alerts = dbq.db.query("""
                SELECT
                    a.*
                FROM
                    (# ENSURE ALL ALERTS FOR GIVEN REVISION ARE OVER 2 HOURS OLD
                    SELECT
                        revision
                    FROM
                        alerts a
                    WHERE
                        create_time >= {{min_time}} AND
                        {{where}}
                    GROUP BY
                        revision
                    HAVING
                        max(create_time) < {{max_time}}
                    ) r
                JOIN
                    alerts a ON a.revision=r.revision
                WHERE
                    {{where}}
            """, {
                "where": esfilter2sqlwhere(dbq.db, {"and": [
                    {"term": {"a.reason": settings.param.reason}},
                    {"not": {"term": {"a.status": "obsolete"}}}
                ]}),
                "max_time": NOW - MIN_AGE,  # DO NOT ALERT WHEN TOO YOUNG
                "min_time": Date.MIN if DEBUG_TOUCH_ALL_ALERTS else NOW - LOOK_BACK
            })
            for a in existing_sustained_alerts:
                a.details = CNV.JSON2object(a.details)
                try:
                    if a.revision.rtrim()[0] in ["{", "["]:
                        a.revision = CNV.JSON2object(a.revision)
                except Exception, e:
                    pass

            tests = Q.index(existing_sustained_alerts, ["revision", "details.Talos.Test"])

            # SUMMARIZE
            alerts = StructList()

            total_tests = esq.query({
                "from": "talos",
                "select": {"name": "count", "aggregate": "count"},
                "edges": [
                    "Talos.Revision"
                ],
                "where": {"and": [
                    {"terms": {"Talos.Revision": list(set(existing_sustained_alerts.revision))}}
                ]}
            })

            # GROUP BY ONE DIMENSION ON 1D CUBE IS REALLY JUST ITERATING OVER THAT DIMENSION, BUT EXPENSIVE
            for revision, total_test_count in Q.groupby(total_tests, ["Talos.Revision"]):
            # FIND TOTAL TDAD FOR EACH INTERESTING REVISION
                revision = revision["Talos.Revision"]
                total_exceptions = tests[(revision, )]  # FILTER BY revision

                parts = StructList()
                for g, exceptions in Q.groupby(total_exceptions, ["details.Talos.Test"]):
                    worst_in_test = Q.sort(exceptions, ["confidence", "details.diff_percent"]).last()
                    example = worst_in_test.details
                    # ADD SOME SPECIFIC URL PARAMETERS
                    branch = example.Talos.Branch.replace("-Non-PGO", "")
                    stop = Math.max(example.push_date_max, (2*example.push_date) - example.push_date_min) + Duration.DAY.milli
                    start = Math.min(example.push_date_min, stop-Duration.WEEK.milli)

                    example.tbpl.url.branch = TBPL_PATH.get(branch, branch)
                    example.mercurial.url.branch = MECURIAL_PATH.get(branch, branch)
                    example.datazilla.url = Struct(
                        project="talos",
                        product=example.Talos.Product,
                        repository=example.Talos.Branch, #+ ("" if worst_in_test.Talos.Branch.pgo else "-Non-PGO")
                        os=example.Talos.OS.name,
                        os_version=example.Talos.OS.version,
                        test=example.Talos.Test.suite,
                        graph=example.Talos.Test.name,
                        graph_search=example.Talos.Revision,
                        start=start/1000,
                        stop=stop/1000,
                        x86="true" if example.Talos.Platform == "x86" else "false",
                        x86_64="true" if example.Talos.Platform == "x86_64" else "false",
                    )
                    example.charts.url = Struct(
                        sampleMin=Date(start).floor().format("%Y-%m-%d"),
                        sampleMax=Date(stop).floor().format("%Y-%m-%d"),
                        test=example.Talos.Test.name,
                        branch=example.Talos.Branch,
                        os=example.Talos.OS.name + "." + example.Talos.OS.version,
                        platform=example.Talos.Platform
                    )

                    num_except = len(exceptions)
                    if num_except == 0:
                        continue

                    part = {
                        "test": g.details.Talos.Test,
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


            # EXISTING REVISION-LEVEL ALERTS
            old_alerts = dbq.query({
                "from": "alerts",
                "select": "*",
                "where": {"and": [
                    {"term": {"reason": REASON}},
                    {"or": [
                        {"terms": {"tdad_id": set(alerts.tdad_id)}},
                        {"terms": {"revision": set(existing_sustained_alerts.revision)}},
                        {"range": {"create_time": {"gte": NOW - LOOK_BACK}}}
                    ]}
                ]},
                # "sort":"status",
                # "limit":10
            })

            update_alert_status(settings, alerts_db, alerts, old_alerts)

            # SHOW SUSTAINED ALERTS ARE COVERED
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
                    {"terms": {"p.revision": set(Q.select(existing_sustained_alerts, "revision"))}},
                    {"missing": "h.parent"}
                ]}),
                "parent_reason": REASON
            })


def main():
    settings = startup.read_settings()
    Log.start(settings.debug)
    try:
        with startup.SingleInstance(flavor_id=settings.args.filename):
            Log.note("Summarize by revision {{schema}}", {"schema": settings.perftest.schema})
            talos_alert_revision(settings)
    finally:
        Log.stop()


if __name__ == '__main__':
    main()
