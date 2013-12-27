################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## Author: Kyle Lahnakoski (kyle@lahnakoski.com)
################################################################################



from datetime import datetime, timedelta

from scipy.stats import binom

from dzAlerts.daemons import alert_exception
from dzAlerts.util import startup, struct
from dzAlerts.util.cnv import CNV
from dzAlerts.util.db import DB, SQL
from dzAlerts.util.logs import Log
from dzAlerts.util.maths import Math
from dzAlerts.util.queries import Q
from dzAlerts.util.struct import nvl
from dzAlerts.daemons.alert import significant_difference


FALSE_POSITIVE_RATE = .01   # WHAT % OF TIME DO WE EXPECTY ANOMALOUS RESULTS?

def get_false_positive_rate(test_name):
    if test_name == "tp5n":
        return 0.04
    return FALSE_POSITIVE_RATE

REASON = "alert_revision"   # name of the reason in alert_reason
LOOK_BACK = timedelta(days=10)
SEVERITY = 0.9
TEMPLATE = [
    """
    <div><h2>{{score}} - {{revision}}</h2>
    {{num_exceptions}} exceptional events<br>
    <a href="https://datazilla.mozilla.org/talos/summary/{{branch}}/{{revision}}">Datazilla</a><br>
    <a href="https://bugzilla.mozilla.org/show_bug.cgi?id={{bug_id}}">Bugzilla - {{bug_description}}</a><br>
    """, {
        "from": "details.tests",
        "template": """
            <hr>
            On page {{.page_url}}<br>
            <a href="https://tbpl.mozilla.org/?tree={{.branch}}&rev={{.revision}}">TBPL</a><br>
            <a href="https://hg.mozilla.org/rev/{{.revision}}">Mercurial</a><br>
            <a href="https://datazilla.mozilla.org/talos/summary/{{.branch}}/{{.revision}}">Datazilla</a><br>
            <a href="http://people.mozilla.com/~klahnakoski/test/es/DZ-ShowPage.html#page={{.page_url}}&sampleMax={{.push_date}}000&sampleMin={{.push_date_min}}000&branch={{.branch}}">Kyle's ES</a><br>
            Raw data: {{.raw_data}}
            """,
        "separator": "<hr>"
    }
]

#GET ACTIVE ALERTS
# assumes there is an outside agent corrupting our test results
# this will look at all alerts on a revision, and figure out the probability there is an actual regression

def alert_revision(settings):
    assert settings.perftest != None
    settings.db.debug = settings.param.debug
    with DB(settings.perftest) as db:
        db.execute("update alert_reasons set email_template={{template}} where code={{reason}}", {
            "template": TEMPLATE,
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
            WHERE
                (a.solution is null OR trim(a.solution)='') AND
                a.reason={{reason}} AND
                a.status<>'obsolete'
            LIMIT
                10
        """, {
            "reason": alert_exception.REASON
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
            "where": db.esfilter2sqlwhere({"and": [
                {"terms": {"revision": interesting_revisions}},
                {"term": {"reason": alert_exception.REASON}},
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
            "where": db.esfilter2sqlwhere({"and": [
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
            "where": db.esfilter2sqlwhere({"terms": {"revision": interesting_revisions}})
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
                    "confidence": binom(t.num_tdad, get_false_positive_rate(t.test_name)).cdf(num_except-1),
                    "example": worst_in_test.details
                }
                parts.append(part)

            worst_in_revision = Q.sort(parts, ["confidence"]).last().example

            known_alerts.append({
                "status": "new",
                "create_time": datetime.utcnow(),
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
                "confidence": Math.max([c.confidence for c in struct.wrap(parts)])  # Take worst
            })

        known_alerts = Q.unique_index(known_alerts, "details.revision")

        #NEW ALERTS, JUST INSERT
        new_alerts = known_alerts - old_alerts
        for revision in new_alerts:
            revision.id = SQL("util_newid()")
            revision.last_updated = datetime.utcnow()
        db.insert_list("alerts", new_alerts)

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
        alert_revision(settings)
    finally:
        Log.stop()


if __name__ == '__main__':
    main()
