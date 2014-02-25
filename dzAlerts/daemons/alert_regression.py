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

from dzAlerts.daemons import alert_sustained
from dzAlerts.util.cnv import CNV
from dzAlerts.util.env import startup
from dzAlerts.util.queries.db_query import esfilter2sqlwhere
from dzAlerts.util.sql.db import DB, SQL
from dzAlerts.util.env.logs import Log
from dzAlerts.util.queries import Q
from dzAlerts.util.struct import nvl
from dzAlerts.daemons.alert import significant_difference


REASON = "alert_regression"   # name of the reason in alert_reason
LOOK_BACK = timedelta(days=20)
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

def alert_regression(settings):
    assert settings.perftest != None
    settings.db.debug = settings.param.debug
    with DB(settings.perftest) as db:
        #TODO: REMOVE, LEAVE IN DB
        db.execute("update alert_reasons set email_template={{template}} where code={{reason}}", {
            "template": CNV.object2JSON(TEMPLATE),
            "reason": REASON
        })

        # NEW SINGLE POINT EXCEPTIONS
        # TODO: DO NOT USE solution
        some_exceptions = db.query("""
            SELECT
                t.revision,
                t.test_name
            FROM
                alerts a
            LEFT JOIN
                test_data_all_dimensions t on t.id=a.tdad_id
            LEFT JOIN
                alert_hierarchy h on h.child=a.id
            WHERE
                h.child IS NULL AND
                a.reason={{reason}} AND
                a.status<>'obsolete' AND
                a.create_time>={{start_time}}
            LIMIT
                1000
        """, {
            "reason": alert_sustained.REASON,
            "start_time": datetime.utcnow() - LOOK_BACK
        })
        interesting_revisions = set(Q.select(some_exceptions, "revision"))

        #EXISTING POINT EXCEPTIONS
        existing_points = db.query("""
            SELECT
                a.*
            FROM
                alerts a
            WHERE
                {{where}}
        """, {
            "where": esfilter2sqlwhere(db, {"and": [
                {"terms": {"revision": interesting_revisions}},
                {"term": {"reason": alert_sustained.REASON}},
                {"not": {"term": {"status": "obsolete"}}}
            ]})
        })
        for e in existing_points:
            e.details = CNV.JSON2object(e.details)

        existing_points = Q.index(existing_points, ["details.revision", "details.test_name"])


        #EXISTING REVISION-LEVEL ALERTS
        old_alerts = db.query("""
            SELECT
                a.*
            FROM
                alerts a
            WHERE
                {{where}}
        """, {
            "where": esfilter2sqlwhere(db, {"and": [
                {"terms": {"revision": interesting_revisions}},
                {"term": {"reason": REASON}}
            ]})
        })
        for e in old_alerts:
            e.details = CNV.JSON2object(e.details)

        old_alerts = Q.unique_index(old_alerts, "details.revision")

        #FIND TOTAL TDAD FOR EACH INTERESTING REVISION
        tests = db.query("""
            SELECT
                revision,
                test_name,
                count(1) num_tdad
            FROM
                test_data_all_dimensions t
            WHERE
                {{where}}
            GROUP BY
                t.revision,
                t.test_name
        """, {
            "where": esfilter2sqlwhere(db, {"terms": {"revision": interesting_revisions}})
        })
        tests = Q.unique_index(tests, ["revision", "test_name"])


        #SUMMARIZE
        known_alerts = []
        for revision in interesting_revisions:
            total_tests = sum(Q.select(tests[revision], "num_tdad"))
            total_exceptions = len(existing_points[revision])

            parts = []
            for t in tests[revision]:
                exceptions = existing_points[t.revision, t.test_name]
                worst_in_test = Q.sort(exceptions, ["confidence", "details.diff"]).last()

                num_except = len(exceptions)
                if num_except == 0:
                    continue

                part = {
                    "test_name": t.test_name,
                    "num_exceptions": num_except,
                    "num_pages": t.num_tdad,
                    "confidence": worst_in_test.confidence,
                    "example": worst_in_test.details
                }
                parts.append(part)

            parts = Q.sort(parts, [{"field": "confidence", "sort": -1}])
            worst_in_revision = parts[0].example

            known_alerts.append({
                "status": "new",
                "create_time": CNV.unix2datetime(worst_in_revision.push_date),
                "reason": REASON,
                "revision": revision,
                "tdad_id": worst_in_revision.tdad_id,
                "details": {
                    "revision": revision,
                    "total_tests": total_tests,
                    "total_exceptions": total_exceptions,
                    "tests": parts,
                    "example": worst_in_revision
                },
                "severity": SEVERITY,
                "confidence": nvl(worst_in_revision.result.confidence, worst_in_revision.sustained_result.confidence)  # Take worst
            })

        known_alerts = Q.unique_index(known_alerts, "details.revision")

        #NEW ALERTS, JUST INSERT
        new_alerts = known_alerts - old_alerts
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
                {"term": {"p.reason": alert_sustained.REASON}},
                {"terms": {"p.revision": Q.select(existing_points, "revision")}},
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
        alert_regression(settings)
    finally:
        Log.stop()


if __name__ == '__main__':
    main()
