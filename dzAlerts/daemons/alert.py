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
from math import log
from dzAlerts.daemons import b2g_alert_revision
from dzAlerts.util.cnv import CNV
from dzAlerts.util.env import startup
from dzAlerts.util.queries import Q
from dzAlerts.util.queries.db_query import esfilter2sqlwhere
from dzAlerts.util.strings import expand_template
from dzAlerts.util.maths import Math
from dzAlerts.util.env.logs import Log
from dzAlerts.util.sql.db import DB, SQL
from dzAlerts.util.struct import nvl

ALERT_LIMIT = Math.bayesian_add(0.90, 0.70)  #SIMPLE severity*confidence LIMIT (FOR NOW)
HEADER = "<h3>This is for testing only.</h3><br>"
#TBPL link: https://tbpl.mozilla.org/?rev=c3598b276048
#TBPL test results:  https://tbpl.mozilla.org/?tree=Mozilla-Inbound&rev=c9429cf294af
#HG: https://hg.mozilla.org/mozilla-central/rev/330feedee4f1
#BZ: https://bugzilla.mozilla.org/show_bug.cgi?id=702559
#DZ: https://datazilla.mozilla.org/talos/summary/Mozilla-Inbound/897654df47b6?product=Firefox&branch_version=23.0a1
#                            "product":v.product,
#                            "branch":v.branch,
#                            "branch_version":v.branch_version,
#                            "revision":v.revision

SEPARATOR = "<hr>\n"
RESEND_AFTER = timedelta(days=1)
LOOK_BACK = timedelta(days=60)
MAX_EMAIL_LENGTH = 15000
EPSILON = 0.0001
SEND_REASONS = [b2g_alert_revision.REASON]


def send_alerts(settings, db):
    """
    BLINDLY SENDS ALERTS FROM THE ALERTS TABLE, ASSUMING ALL HAVE THE SAME STRUCTURE.
    """
    debug = settings.param.debug
    db.debug = debug

    try:
        new_alerts = db.query("""
            SELECT
                a.id alert_id,
                a.reason,
                r.description,
                a.details,
                a.severity,
                a.confidence,
                a.revision,
                r.email_template
            FROM
                alerts a
            JOIN
                alert_reasons r on r.code = a.reason
            WHERE
                (
                    a.last_sent IS NULL OR
                    a.last_sent < a.last_updated OR
                    a.last_sent < {{last_sent}}
                ) AND
                a.status <> 'obsolete' AND
                bayesian_add(a.severity, a.confidence) > {{alert_limit}} AND
                a.solution IS NULL AND
                a.reason in {{reasons}} AND
                a.create_time > {{min_time}}
            ORDER BY
                bayesian_add(a.severity, a.confidence) DESC,
                json.number(details, "diff") DESC
            LIMIT
                10
        """, {
            "last_sent": datetime.utcnow() - RESEND_AFTER,
            "alert_limit": ALERT_LIMIT - EPSILON,
            "min_time": datetime.utcnow()-LOOK_BACK,
            "reasons": SQL("("+", ".join(db.quote_value(v) for v in SEND_REASONS)+")")
        })

        if not new_alerts:
            if debug:
                Log.note("Nothing important to email")
            return

        #poor souls that signed up for emails
        listeners = db.query("SELECT email FROM alert_listeners")
        listeners = [x["email"] for x in listeners]
        listeners = ";".join(listeners)

        for alert in new_alerts:
            body = [HEADER]
            if alert.confidence >= 1:
                alert.confidence = 0.999999

            alert.details = CNV.JSON2object(alert.details)
            alert.revision = CNV.JSON2object(alert.revision)
            alert.score = str(-log(1.0-Math.bayesian_add(alert.severity, alert.confidence), 10))  #SHOW NUMBER OF NINES
            alert.details.url = alert.details.page_url
            example = alert.details.example
            for e in alert.details.tests.example + [example]:
                if e.push_date_min:
                    e.push_date_max = (2 * e.push_date) - e.push_date_min
                    e.date_range = (datetime.utcnow()-CNV.milli2datetime(e.push_date_min)).total_seconds()/(24*60*60)  #REQUIRED FOR DATAZILLA B2G CHART REFERENCE
                    e.date_range = nvl(nvl(*[v for v in (7, 30, 60) if v > e.date_range]), 90)  #PICK FIRST v > CURRENT VALUE

            body.append(expand_template(CNV.JSON2object(alert.email_template), alert))
            body = "".join(body)

            if debug:
                Log.note("EMAIL: {{email}}", {"email": body})

            if len(body) > MAX_EMAIL_LENGTH:
                Log.note("Truncated the email body")
                suffix = "... (has been truncated)"
                body = body[0:MAX_EMAIL_LENGTH - len(suffix)] + suffix   #keep it reasonable

            db.call("email_send", (
                listeners, #to
                settings.param.email.title,
                body, #body
                None
            ))

            #I HOPE I CAN SEND ARRAYS OF NUMBERS
            db.execute(
                "UPDATE alerts SET last_sent={{time}} WHERE {{where}}", {
                    "time": datetime.utcnow(),
                    "where": esfilter2sqlwhere(db, {"terms": {"id": Q.select(new_alerts, "alert_id")}})
                })

            break  #FOR DEBUGGING

    except Exception, e:
        Log.error("Could not send alerts", e)


def update_h0_rejected(db, start_date, possible_alerts):
    """
    REVIEW THE ALERT TABLE AND ENSURE THE test_data_all_dimensions(h0_rejected)
    COLUMN REFLECTS THE ALERT STATI
    TODO: GETTING EXPENSIVE TO RUN (at 200K alerts)
    """

    db.execute("""
        UPDATE
            test_data_all_dimensions t
        JOIN (
            SELECT
                tdad_id,
                max(CASE WHEN status<>'obsolete' THEN 1 ELSE 0 END) h0
            FROM
                alerts a
            WHERE
                {{where}}
            GROUP BY
                tdad_id
            ) a ON a.tdad_id = t.id
        SET t.h0_rejected = a.h0
    """, {
        "where": esfilter2sqlwhere(db, {"terms": {"a.tdad_id": possible_alerts}})
    })



if __name__ == '__main__':
    settings = startup.read_settings()
    Log.start(settings.debug)

    try:
        Log.note("Running alerts off of schema {{schema}}", {"schema": settings.perftest.schema})

        with DB(settings.perftest) as db:
            send_alerts(
                settings=settings,
                db=db
            )
    except Exception, e:
        Log.warning("Failure to run alerts", cause=e)
    finally:
        Log.stop()

