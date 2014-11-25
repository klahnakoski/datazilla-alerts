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
from pyLibrary.env import startup, elasticsearch
from pyLibrary.env.files import File
from pyLibrary.maths import Math
from pyLibrary.queries.db_query import esfilter2sqlwhere, DBQuery
from pyLibrary.queries.es_query import ESQuery
from pyLibrary.sql.db import DB
from pyLibrary.env.logs import Log
from pyLibrary.queries import Q
from pyLibrary.structs import nvl, StructList, Struct
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import Duration


DEBUG_TOUCH_ALL_ALERTS = False
UPDATE_EMAIL_TEMPLATE = True
SUSTAINED_REASON = "eideticker_alert_sustained_median"
REASON = "eideticker_alert_revision"   # name of the reason in alert_reason
LOOK_BACK = Duration(days=90)
MIN_AGE = Duration(hours=2)
NOW = datetime.utcnow()
SEVERITY = 0.7
DEBUG_TOUCH_ALL_ALERTS = False  # True IF ALERTS WILL BE UPDATED, EVEN IF THE QUALITY IS NO DIFFERENT
TEST_REVISION = '2d88803a0b9c'

SUBJECT = "[ALERT][Eideticker] {{details.example.Eideticker.Test}} regressed by {{details.example.diff_percent|percent(digits=2)}} in {{details.example.Eideticker.Branch}}"

TEMPLATE = [
    """
    <div>
    <div style="font-size: 150%;font-weight: bold;">Score: {{score|round(digits=3)}}</div><br>
    <span style="font-size: 120%; display:inline-block">Revision: <a href="https://hg.mozilla.org/integration/mozilla-inbound/rev/{{revision}}">{{revision}}</a></span>
    [<a href="https://hg.mozilla.org/integration/mozilla-inbound/rev/{{details.example.past_revision}}">Previous</a>]

    <br>
    <br>
    {{details.total_exceptions}} exceptional events:<br>
    <table>
    <thead><tr>
    <td>Device</td>
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
            <td>{{example.Eideticker.Test}}</td>
            <td><a href="http://eideticker.mozilla.org/#{{example.eideticker.url.path}}/{{example.Eideticker.Device}}/{{example.Eideticker.Branch}}/{{example.Eideticker.Test}}/{{example.eideticker.url.metric}}/90">Eideticker</a></td>
            <td><a href="http://people.mozilla.org/~klahnakoski/talos/Alert-Eideticker.html#{{example.charts.url|url}}">charts!</a></td>
            <td><a href="https://hg.mozilla.org/mozilla-central/pushloghtml?fromchange={{example.past_revision}}&tochange={{example.revision}}">DIFF</a></td>
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

def eideticker_alert_revision(settings):
    assert settings.alerts != None
    settings.db.debug = settings.param.debug
    debug = settings.param.debug



    with DB(settings.alerts) as alerts_db:
        with ESQuery(elasticsearch.Index(settings.query["from"])) as esq:
            dbq = DBQuery(alerts_db)

            esq.addDimension(convert.JSON2object(File(settings.dimension.filename).read()))

            # TODO: REMOVE, LEAVE IN DB
            if UPDATE_EMAIL_TEMPLATE:
                alerts_db.execute("update reasons set email_subject={{subject}}, email_template={{template}}, email_style={{style}} where code={{reason}}", {
                    "template": convert.object2JSON(TEMPLATE),
                    "subject": convert.object2JSON(SUBJECT),
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
                a.details = convert.JSON2object(a.details)
                try:
                    if a.revision.rtrim()[0] in ["{", "["]:
                        a.revision = convert.JSON2object(a.revision)
                except Exception, e:
                    pass

            tests = Q.index(existing_sustained_alerts, ["revision", "details.Eideticker.Test"])

            # SUMMARIZE
            alerts = StructList()

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
            # FIND TOTAL TDAD FOR EACH INTERESTING REVISION
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
                        metric=example.metric,
                        path=nvl(example.path, "")
                    )
                    example.charts.url = Struct(
                        sampleMin=Date(example.push_date_min).floor().format("%Y-%m-%d"),
                        sampleMax=Date(stop).floor().format("%Y-%m-%d"),
                        test=example.Eideticker.Test,
                        branch=example.Eideticker.Branch,
                        device=example.Eideticker.Device,
                        metric=example.metric
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

                alerts.append(Struct(
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
                    severity=SEVERITY,
                    confidence=nvl(worst_in_revision.result.score, -Math.log10(1 - worst_in_revision.result.confidence), 8), # confidence was never more accurate than 8 decimal places
                    branch=worst_in_revision.Eideticker.Branch,
                    test=worst_in_revision.Eideticker.Test,
                    platform=worst_in_revision.Eideticker.Device,
                    percent=unicode(worst_in_revision.diff_percent*100)+"%",
                    keyrevision=worst_in_revision.Eideticker.Revision
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
            eideticker_alert_revision(settings)
    finally:
        Log.stop()


if __name__ == '__main__':
    main()
