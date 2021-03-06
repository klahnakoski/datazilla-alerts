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
from dzAlerts.daemons import b2g_alert_revision, talos_alert_revision
from dzAlerts.daemons.email_send import email_send
from dzAlerts.daemons.talos_alert_revision import TEMPLATE, SUBJECT,  REASON
from dzAlerts.util.cnv import CNV
from dzAlerts.util.env import startup
from dzAlerts.util.queries import Q
from dzAlerts.util.queries.db_query import esfilter2sqlwhere
from dzAlerts.util.strings import expand_template
from dzAlerts.util.maths import Math
from dzAlerts.util.env.logs import Log
from dzAlerts.util.sql.db import DB, SQL
from dzAlerts.util.struct import nvl
from dzAlerts.util.env.emailer import Emailer

ALERT_LIMIT = Math.bayesian_add(0.90, 0.70)  #SIMPLE severity*confidence LIMIT (FOR NOW)
HEADER = "<h3>Performance Regression Alert</h3>"
FOOTER = "<hr><a style='font-size:70%' href='https://wiki.mozilla.org/FirefoxOS/Performance/Investigating_Alerts'>Understanding this alert</a>"
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
RESEND_AFTER = timedelta(days=7)
LOOK_BACK = timedelta(days=30)
MAX_EMAIL_LENGTH = 15000
EPSILON = 0.0001
SEND_REASONS = [b2g_alert_revision.REASON, talos_alert_revision.REASON]


def send_alerts(settings, db):
    """
    BLINDLY SENDS ALERTS FROM THE ALERTS TABLE, ASSUMING ALL HAVE THE SAME STRUCTURE.
    """
    debug = settings.param.debug
    db.debug = debug

    #TODO: REMOVE, LEAVE IN DB
    if db.debug:
        db.execute("update reasons set email_subject={{subject}}, email_template={{template}} where code={{reason}}", {
            "template": CNV.object2JSON(TEMPLATE),
            "subject": CNV.object2JSON(SUBJECT),
            "reason": REASON
        })
        db.flush()

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
                r.email_template,
                r.email_subject
            FROM
                alerts a
            JOIN
                reasons r on r.code = a.reason
            WHERE
                a.last_sent IS NULL AND
                a.status <> 'obsolete' AND
                math.bayesian_add(a.severity, a.confidence) > {{alert_limit}} AND
                a.solution IS NULL AND
                a.reason in {{reasons}} AND
                a.create_time > {{min_time}}
            ORDER BY
                math.bayesian_add(a.severity, a.confidence) DESC,
                json.number(left(details, 65000), "diff_percent") DESC
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

        for alert in new_alerts:
            #poor souls that signed up for emails
            listeners = ";".join(db.query("SELECT email FROM listeners WHERE reason={{reason}}", {"reason": alert.reason}).email)

            body = [HEADER]
            if alert.confidence >= 1:
                alert.confidence = 0.999999

            alert.details = CNV.JSON2object(alert.details)
            try:
                alert.revision = CNV.JSON2object(alert.revision)
            except Exception, e:
                pass
            alert.score = str(-log(1.0-Math.bayesian_add(alert.severity, alert.confidence), 10))  #SHOW NUMBER OF NINES
            alert.details.url = alert.details.page_url
            example = alert.details.example
            for e in alert.details.tests.example + [example]:
                if e.push_date_min:
                    e.push_date_max = (2 * e.push_date) - e.push_date_min
                    e.date_range = (datetime.utcnow()-CNV.milli2datetime(e.push_date_min)).total_seconds()/(24*60*60)  #REQUIRED FOR DATAZILLA B2G CHART REFERENCE
                    e.date_range = nvl(nvl(*[v for v in (7, 30, 60) if v > e.date_range]), 90)  #PICK FIRST v > CURRENT VALUE

            subject = expand_template(CNV.JSON2object(alert.email_subject), alert)
            body.append(expand_template(CNV.JSON2object(alert.email_template), alert))
            body = "".join(body)+FOOTER

            if debug:
                Log.note("EMAIL: {{email}}", {"email": body})

            if len(body) > MAX_EMAIL_LENGTH:
                Log.note("Truncated the email body")
                suffix = "... (has been truncated)"
                body = body[0:MAX_EMAIL_LENGTH - len(suffix)] + suffix   #keep it reasonable

            db.call("mail.send", (
                listeners, #to
                subject,
                body, #body
                None
            ))

            #I HOPE I CAN SEND ARRAYS OF NUMBERS
            db.execute(
                "UPDATE alerts SET last_sent={{time}} WHERE {{where}}", {
                    "time": datetime.utcnow(),
                    "where": esfilter2sqlwhere(db, {"terms": {"id": Q.select(new_alerts, "alert_id")}})
                })

    except Exception, e:
        Log.error("Could not send alerts", e)




if __name__ == '__main__':
    settings = startup.read_settings()
    Log.start(settings.debug)

    try:
        Log.note("Running alerts off of schema {{schema}}", {"schema": settings.perftest.schema})

        with DB(settings.alerts) as db:
            send_alerts(
                settings=settings,
                db=db
            )
    except Exception, e:
        Log.warning("Failure to run alerts", cause=e)
    finally:
        Log.stop()

