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
from pyLibrary import convert
from pyLibrary.debugs import startup
from pyLibrary.env import elasticsearch
from pyLibrary.env.files import File
from pyLibrary.maths import Math
from pyLibrary.queries.db_query import esfilter2sqlwhere, DBQuery
from pyLibrary.queries.es_query import ESQuery
from pyLibrary.sql.db import DB
from pyLibrary.debugs.logs import Log
from pyLibrary.queries import qb
from pyLibrary.dot import nvl, Dict, DictList
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import Duration


DEBUG_TOUCH_ALL_ALERTS = False
UPDATE_EMAIL_TEMPLATE = True
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
    "[ALERT][B2G] {{details.example.B2G.Test.name}} regressed by {{details.example.diff_percent|percent(digits=3)}}{{details.example.units}} in ",
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
        [<a href="https://github.com/mozilla-b2g/gaia/commit/{{details.example.past_revision.gaia}}">Previous</a>]
        [<a href="https://github.com/mozilla-b2g/gaia/compare/{{details.example.past_revision.gaia}}...{{details.example.B2G.Revision.gaia}}">DIFF</a>]<br>

        <span style="font-size: 120%; display:inline-block">Gecko: <a href="{{revision.gecko_repository}}/rev/{{revision.gecko}}">{{revision.gecko}}</a></span>
        [<a href="{{revision.gecko_repository}}/rev/{{details.example.past_revision.gecko}}">Previous</a>]
        [<a href="{{revision.gecko_repository}}/pushloghtml?fromchange={{details.example.past_revision.gecko}}&tochange={{revision.gecko}}">DIFF</a>]<br>

    <br>
    <br>
    {{details.total_exceptions}} exceptional events:<br>
    <table>
    <thead><tr><td>Device</td><td>Suite</td><td>Test Name</td><td>DZ Link</td><td>Date/Time</td><td>Before</td><td>After</td><td>Diff</td></tr></thead>
    """, {
        "from": "details.tests",
        "template": """<tr>
            <td>{{example.B2G.Device|upper}}</td>
            <td>{{test.suite}}</td>
            <td>{{test.name|html}}</td>
            <td><a href="https://datazilla.mozilla.org/b2g/?branch={{example.B2G.Branch|url}}&device={{example.B2G.Device|url}}&range={{example.date_range|url}}&test={{test.name|url}}&app_list={{test.suite|url}}&gaia_rev={{example.B2G.Revision.gaia|url}}&gecko_rev={{example.B2G.Revision.gecko|url}}&plot=median\">Datazilla!</a></td>
            <td>{{example.push_date|datetime}}</td>
            <td>{{example.past_stats.mean|round(digits=4)}}</td>
            <td>{{example.future_stats.mean|round(digits=4)}}</td>
            <td>{{example.diff|round(digits=2)}}</td>
            </tr>
        """
    },
    """</table></div>"""
]


def b2g_alert_revision(settings):
    assert settings.alerts != None
    settings.db.debug = settings.param.debug
    with DB(settings.alerts) as alerts_db:
        with ESQuery(elasticsearch.Index(settings.query["from"])) as esq:
            dbq = DBQuery(alerts_db)

            esq.addDimension(convert.json2value(File(settings.dimension.filename).read()))

            # TODO: REMOVE, LEAVE IN DB
            if UPDATE_EMAIL_TEMPLATE:
                alerts_db.execute("update reasons set email_subject={{subject}}, email_template={{template}}, email_style={{style}} where code={{reason}}", {
                    "template": convert.value2json(TEMPLATE),
                    "subject": convert.value2json(SUBJECT),
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
                        push_date >= {{min_time}} AND
                        {{where}}
                    GROUP BY
                        revision
                    HAVING
                        max(push_date) < {{max_time}}
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
                a.details = convert.json2value(a.details)
                try:
                    if a.revision.rstrip()[0] in ["{", "["]:
                        a.revision = convert.json2value(a.revision)
                except Exception, e:
                    pass

            tests = qb.index(existing_sustained_alerts, ["revision", "details.B2G.Test"])

            # SUMMARIZE
            alerts = DictList()

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
            # THIS IS A existing_sustained_alerts LEFT JOIN total_tests ON (revision,)
            for revision, total_exceptions in qb.groupby(existing_sustained_alerts, ["details.B2G.Revision"]):
            # FIND TOTAL TDAD FOR EACH INTERESTING REVISION
                revision = revision["details.B2G.Revision"]
                total_test_count = total_tests[{"B2G.Revision": revision}]

                parts = DictList()
                for g, exceptions in qb.groupby(total_exceptions, ["details.B2G.Test"]):
                    worst_in_test = qb.sort(exceptions, ["confidence", "details.diff_percent"]).last()
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

                parts = qb.sort(parts, [{"field": "confidence", "sort": -1}])
                worst_in_revision = parts[0].example

                alerts.append(Dict(
                    status= "NEW",
                    push_date= convert.milli2datetime(worst_in_revision.push_date),
                    reason= REASON,
                    revision= revision,
                    tdad_id= revision,
                    details={
                        "revision": revision,
                        "total_tests": total_test_count,
                        "total_exceptions": len(total_exceptions),
                        "tests": parts,
                        "example": worst_in_revision
                    },
                    severity= SEVERITY,
                    confidence= nvl(worst_in_revision.result.score, -Math.log10(1-worst_in_revision.result.confidence), 8),  # confidence was never more accurate than 8 decimal places
                    branch=worst_in_revision.B2G.Branch,
                    test=worst_in_revision.B2G.Test,
                    platform=worst_in_revision.B2G.Platform,
                    percent=unicode(worst_in_revision.diff_percent*100)+"%",
                    keyrevision=worst_in_revision.B2G.Revision
                ))


            # EXISTING REVISION-LEVEL ALERTS
            old_alerts = dbq.query({
                "from": "alerts",
                "select": "*",
                "where": {"and": [
                    {"term": {"reason": REASON}},
                    {"or": [
                        {"terms": {"tdad_id": set(alerts.tdad_id)}},
                        {"terms": {"revision": set(existing_sustained_alerts.revision)}},
                        {"range": {"push_date": {"gte": NOW - LOOK_BACK}}}
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
                    {"terms": {"p.revision": set(qb.select(existing_sustained_alerts, "revision"))}},
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
            b2g_alert_revision(settings)
    finally:
        Log.stop()


if __name__ == '__main__':
    main()
