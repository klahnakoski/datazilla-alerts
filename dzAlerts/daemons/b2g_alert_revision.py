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

from dzAlerts.daemons import b2g_sustained_median
from dzAlerts.daemons.util import significant_difference
from dzAlerts.util.cnv import CNV
from dzAlerts.util.env import startup
from dzAlerts.util.env.elasticsearch import ElasticSearch
from dzAlerts.util.env.files import File
from dzAlerts.util.queries.db_query import esfilter2sqlwhere, DBQuery
from dzAlerts.util.queries.es_query import ESQuery
from dzAlerts.util.sql.db import DB, SQL
from dzAlerts.util.env.logs import Log
from dzAlerts.util.queries import Q
from dzAlerts.util.struct import nvl


REASON = "b2g_alert_revision"   # name of the reason in alert_reason
LOOK_BACK = timedelta(days=30)
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

TEMPLATE = [
    """
    <div><h2>Score: {{score}}</h2>
    <h3>Gaia: {{revision.gaia}}</h3>
    <h3>Gecko: {{revision.gecko}}</h2>
    {{details.total_exceptions}} exceptional events:<br>
    """, {
        "from": "details.tests",
        "template": """
            {{example.B2G.Device|upper}}: {{test.suite}}.{{test.name}}: {{num_exceptions}} exceptions,
            (<a href="https://datazilla.mozilla.org/b2g/?branch={{example.B2G.Branch}}&device={{example.B2G.Device}}&range={{example.date_range}}&test={{test.name}}&app_list={{test.suite}}&gaia_rev={{example.B2G.Revision.gaia}}&gecko_rev={{example.B2G.Revision.gecko}}&plot=median\">
            Datazilla!</a> {{example.push_date|datetime}}, before: {{example.past_stats.mean}}, after: {{example.future_stats.mean}})<br>
        """
    },
    "</div>"
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
        db.flush()

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
        for known_alert in known_alerts & old_alerts:
            if len(nvl(known_alert.solution, "").strip()) != 0:
                continue  # DO NOT TOUCH SOLVED ALERTS

            old_alert = old_alerts[known_alert]
            if significant_difference(known_alert.severity, old_alert.severity) or significant_difference(known_alert.confidence, old_alert.confidence):
                known_alert.last_updated = datetime.utcnow()
                db.update("alerts", {"id": old_alert.id}, known_alert)

        #OLD ALERTS, OBSOLETE
        for old_alert in old_alerts - known_alerts:
            old_alert.status = 'obsolete'
            old_alert.last_updated = datetime.utcnow()
            db.update("alerts", {"id": old_alert.id}, old_alert)


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