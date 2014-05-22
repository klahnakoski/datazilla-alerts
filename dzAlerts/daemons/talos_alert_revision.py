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

from dzAlerts.daemons import talos_sustained_median
from dzAlerts.daemons.util import significant_difference
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


REASON = "talos_alert_revision"   # name of the reason in alert_reason
LOOK_BACK = timedelta(days=90)
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


SUBJECT = [
    "[ALERT][{{details.example.tbpl.url.branch}}] {{details.example.Talos.Test.name}} regressed by {{details.example.diff_percent|percent(digits=2)}} in ",
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
        <span style="font-size: 120%; display:inline-block">
        [<a href="https://hg.mozilla.org/{{details.example.mercurial.url.branch}}/rev/{{revision}}">{{revision}}</a>]
        </span>
        [<a href="https://hg.mozilla.org/{{details.example.mercurial.url.branch|lower}}/rev/{{details.example.past_revision}}">Previous</a>]
        [<a href="https://tbpl.mozilla.org/?tree={{details.example.tbpl.url.branch}}&rev={{revision}}">TBPL</a>]
    <br>
    <br>
    {{details.total_exceptions}} exceptional events:<br>
    <table>
    <thead><tr><td>Branch</td><td>Platform</td><td>Suite</td><td>Test Name</td><td>DZ Link</td><td>Diff</td><td>Date/Time</td><td>Before</td><td>After</td><td>Diff</td></tr></thead>
    """, {
        "from": "details.tests",
        "template": """<tr>
            <td>{{example.Talos.Product}} {{example.Talos.Branch}}</td>
            <td>{{example.Talos.OS.name}} ({{example.Talos.OS.version}})</td>
            <td>{{test.suite}}</td>
            <td>{{test.name}}</td>
            <td><a href="https://datazilla.mozilla.org/?product={{example.Talos.Product}}&repository={{example.datazilla.url.branch}}&start={{example.push_date_min|unix}}&stop={{example.datazilla.url.stop|unix}}&os={{example.Talos.OS.name}}&os_version={{example.Talos.OS.version}}&test={{example.Talos.Test.suite}}&graph={{example.Talos.Test.name}}&graph_search={{example.Talos.Revision}}&project=talos&x86={{example.datazilla.url.x86}}&x86_64={{example.datazilla.url.x86_64}}">Datazilla!</a></td>
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

#GET ACTIVE ALERTS
# assumes there is an outside agent corrupting our test results
# this will look at all alerts on a revision, and figure out the probability there is an actual regression

def talos_alert_revision(settings):
    assert settings.alerts != None
    settings.db.debug = settings.param.debug
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
                    {"term": {"reason": talos_sustained_median.REASON}},
                    {"not": {"term": {"status": "obsolete"}}},
                    {"range": {"create_time": {"gte": NOW - LOOK_BACK}}}
                ]}
            })

            tests = Q.index(existing_sustained_alerts, ["revision", "details.Talos.Test"])

            #EXISTING REVISION-LEVEL ALERTS
            old_alerts = dbq.query({
                "from": "alerts",
                "select": "*",
                "where": {"and": [
                    {"term": {"reason": REASON}},
                    {"or": [
                        {"terms": {"revision": set(existing_sustained_alerts.revision)}},

                        {"term": {"reason": talos_sustained_median.REASON}},
                        {"term": {"status": "obsolete"}},
                        {"range": {"create_time": {"gte": NOW - LOOK_BACK}}}
                    ]}
                ]}
            })
            old_alerts = Q.unique_index(old_alerts, "revision")

            #SUMMARIZE
            known_alerts = StructList()
            for revision in set(existing_sustained_alerts.revision):
            #FIND TOTAL TDAD FOR EACH INTERESTING REVISION
                total_tests = esq.query({
                    "from": "talos",
                    "select": {"name": "count", "aggregate": "count"},
                    "where": {"and":[
                        {"term": {"Talos.Revision": revision}}
                    ]}
                })
                total_exceptions = tests[(revision, )]  # FILTER BY revision

                parts = StructList()
                for g, exceptions in Q.groupby(total_exceptions, ["details.Talos.Test"]):
                    worst_in_test = Q.sort(exceptions, ["confidence", "details.diff_percent"]).last()
                    example = worst_in_test.details
                    # ADD SOME DATAZILLA SPECIFIC URL PARAMETERS
                    branch = example.Talos.Branch.replace("-Non-PGO", "")
                    example.tbpl.url.branch = TBPL_PATH.get(branch, branch)
                    example.mercurial.url.branch = MECURIAL_PATH.get(branch, branch)
                    example.datazilla.url.branch = example.Talos.Branch #+ ("" if worst_in_test.Talos.Branch.pgo else "-Non-PGO")
                    example.datazilla.url.x86 = "true" if example.Talos.Platform == "x86" else "false"
                    example.datazilla.url.x86_64 = "true" if example.Talos.Platform == "x86_64" else "false"
                    example.datazilla.url.stop = Math.max(example.push_date_max, (2*example.push_date) - example.push_date_min)

                    num_except = len(exceptions)
                    if num_except == 0:
                        continue

                    part = {
                        "test": g.details.Talos.Test,
                        "num_exceptions": num_except,
                        "num_tests": total_tests,
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
                        "total_tests": total_tests,
                        "total_exceptions": len(total_exceptions),
                        "tests": parts,
                        "example": worst_in_revision
                    },
                    "severity": SEVERITY,
                    "confidence": worst_in_revision.result.confidence
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
                    {"term": {"p.reason": talos_sustained_median.REASON}},
                    {"terms": {"p.revision": Q.select(existing_sustained_alerts, "revision")}},
                    {"missing": "h.parent"}
                ]}),
                "parent_reason": REASON
            })

            #CURRENT ALERTS, UPDATE IF DIFFERENT
            for known_alert in known_alerts & old_alerts:
                if len(nvl(known_alert.solution, "").strip()) != 0:
                    continue  # DO NOT TOUCH SOLVED ALERTS

                old_alert = old_alerts[known_alert]
                if old_alert.status == 'obsolete' or significant_difference(known_alert.severity, old_alert.severity) or significant_difference(known_alert.confidence, old_alert.confidence):
                    known_alert.last_updated = NOW
                    db.update("alerts", {"id": old_alert.id}, known_alert)

            #OLD ALERTS, OBSOLETE
            for old_alert in old_alerts - known_alerts:
                if old_alert.status == 'obsolete':
                    continue
                db.update("alerts", {"id": old_alert.id}, {"status": "obsolete", "last_updated": NOW, "details":None})


def main():
    settings = startup.read_settings()
    Log.start(settings.debug)
    try:
        Log.note("Summarize by revision {{schema}}", {"schema": settings.perftest.schema})
        talos_alert_revision(settings)
    finally:
        Log.stop()


if __name__ == '__main__':
    main()
