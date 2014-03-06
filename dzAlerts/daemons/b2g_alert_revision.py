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

from dzAlerts.daemons import alert_sustained, b2g_sustained_median
from dzAlerts.util.cnv import CNV
from dzAlerts.util.env import startup
from dzAlerts.util.env.elasticsearch import ElasticSearch
from dzAlerts.util.env.files import File
from dzAlerts.util.queries.db_query import esfilter2sqlwhere, DBQuery
from dzAlerts.util.queries.es_query import ESQuery
from dzAlerts.util.sql.db import DB, SQL
from dzAlerts.util.env.logs import Log
from dzAlerts.util.queries import Q
from dzAlerts.util.struct import nvl, wrap
from dzAlerts.daemons.alert import significant_difference


REASON = "b2g_alert_revision"   # name of the reason in alert_reason
LOOK_BACK = timedelta(days=90)
SEVERITY = 0.7
TEMPLATE = [
    """
    <div><h2>{{score}} - {{revision}}</h2>
    {{details.total_exceptions}} exceptional events<br>
    <a href="https://bugzilla.mozilla.org/show_bug.cgi?id={{bug_id}}">Bugzilla - {{details.bug_description}}</a><br>
    <a href="https://datazilla.mozilla.org/?start={{example.push_date_min}}&stop={{example.push_date_max}}&product={{example.product}}&repository={{example.branch}}&os={{example.operating_system_name}}&os_version={{example.operating_system_version}}&test={{test_name}}&graph_search={{revision}}&error_bars=false&project=talos\">Datazilla</a><br>
    """, {
        "from": "details.tests",
        "template": """
            {{test_name}}: {{num_exceptions}} exceptions, out of {{num_pages}} pages,
            (<a href="https://datazilla.mozilla.org/?start={{example.push_date_min}}&stop={{example.push_date_max}}&product={{example.product}}&repository={{example.branch}}&os={{example.operating_system_name}}&os_version={{example.operating_system_version}}&test={{test_name}}&graph_search={{..revision}}&error_bars=false&project=talos\">
            {{example.page_url}}</a> {{example.push_date|datetime}}, before: {{example.past_stats.mean}}, after: {{example.future_stats.mean}})<br>
        """
    }
]

#GET ACTIVE ALERTS
# assumes there is an outside agent corrupting our test results
# this will look at all alerts on a revision, and figure out the probability there is an actual regression

def b2g_alert_revision(settings):
    assert settings.alerts != None
    settings.db.debug = settings.param.debug
    with DB(settings.alerts) as db:

        dbq = DBQuery(db)
        esq = ESQuery(ElasticSearch(settings.query["from"]))
        esq.addDimension(CNV.JSON2object(File(settings.dimension.filename).read()))

        #TODO: REMOVE, LEAVE IN DB
        db.execute("update alert_reasons set email_template={{template}} where code={{reason}}", {
            "template": CNV.object2JSON(TEMPLATE),
            "reason": REASON
        })

        #EXISTING SUSTAINED EXCEPTIONS
        existing_sustained_alerts = dbq.query({
            "from": "alerts",
            "select": "*",
            "where": {"and": [
                {"term": {"reason": b2g_sustained_median.REASON}},
                {"not": {"term": {"status": "obsolete"}}},
                {"range": {"create_time": {"gte": datetime.utcnow() - LOOK_BACK}}}
            ]}
        })

        tests = Q.index(existing_sustained_alerts, ["revision", "details.B2G.Test"])

        #EXISTING REVISION-LEVEL ALERTS
        old_alerts = dbq.query({
            "from": "alerts",
            "select": "*",
            "where": {"and": [
                {"terms": {"revision": set(existing_sustained_alerts.revision)}},
                {"term": {"reason": REASON}}
            ]}
        })
        old_alerts = Q.unique_index(old_alerts, "revision")

        #SUMMARIZE
        known_alerts = []
        for revision in set(existing_sustained_alerts.revision):
        #FIND TOTAL TDAD FOR EACH INTERESTING REVISION
            total_tests = esq.query({
                "from": "b2g_alerts",
                "select": {"name": "count", "aggregate": "count"},
                "where": {"terms": {"B2G.Revision": revision}}
            })
            total_exceptions = tests[(revision, )]  # FILTER BY revision

            parts = []
            for g, exceptions in Q.groupby(total_exceptions, ["details.B2G.Test"]):
                worst_in_test = Q.sort(exceptions, ["confidence", "details.diff"]).last()

                num_except = len(exceptions)
                if num_except == 0:
                    continue

                part = {
                    "test": g.details.B2G.Test,
                    "num_exceptions": num_except,
                    "num_tests": total_tests,
                    "confidence": worst_in_test.confidence,
                    "example": worst_in_test.details
                }
                parts.append(part)

            parts = Q.sort(parts, [{"field": "confidence", "sort": -1}])
            worst_in_revision = parts[0].example

            known_alerts.append({
                "status": "new",
                "create_time": CNV.milli2datetime(worst_in_revision.push_date),
                "reason": REASON,
                "revision": revision,
                "tdad_id": {"test_run_id": worst_in_revision.test_run_id, "B2G": {"Test": worst_in_revision.B2G.Test}},
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

        testSelect = Q.select(known_alerts, ["tdad_id.test_run_id"])

        #NEW ALERTS, JUST INSERT
        new_alerts = known_alerts - old_alerts
        if new_alerts:
            for revision in new_alerts:
                revision.id = SQL("util_newid()")
                revision.last_updated = datetime.utcnow()
            db.insert_list("alerts", new_alerts)

        #SHOW SUSTAINED ALERTS ARE COVERED
        db.execute("""
            INSERT INTO alert_hierarchy (parent, child)
            SELECT
                r.id parent,
                p.id child
            FROM
                alerts p
            LEFT JOIN
                alert_hierarchy h on h.child=p.id
            LEFT JOIN
                alerts r on r.revision=p.revision AND r.reason={{parent_reason}}
            WHERE
                {{where}}
        """, {
            "where": esfilter2sqlwhere(db, {"and": [
                {"term": {"p.reason": b2g_sustained_median.REASON}},
                {"terms": {"p.revision": Q.select(existing_sustained_alerts, "revision")}},
                {"missing": "h.parent"}
            ]}),
            "parent_reason": REASON
        })

        #CURRENT ALERTS, UPDATE IF DIFFERENT
        for existing in known_alerts & old_alerts:
            if len(nvl(existing.solution, "").strip()) != 0:
                continue  # DO NOT TOUCH SOLVED ALERTS

            e = old_alerts[existing.revision]
            if significant_difference(existing.severity, e.severity) or significant_difference(existing.confidence, e.confidence):
                existing.last_updated = datetime.utcnow()
                db.update("alerts", {"id": existing.id}, existing)

        #OLD ALERTS, OBSOLETE
        for e in old_alerts - known_alerts:
            e.status = 'obsolete'
            e.last_updated = datetime.utcnow()
            db.update("alerts", {"id": e.id}, e)


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
