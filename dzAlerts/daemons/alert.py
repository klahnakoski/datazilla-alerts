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
from math import log10

from pynliner import Pynliner

from dzAlerts.daemons import b2g_alert_revision, talos_alert_revision, eideticker_alert_revision
from pyLibrary import convert
from pyLibrary.env import startup
from pyLibrary.queries import Q
from pyLibrary.queries.db_query import esfilter2sqlwhere
from pyLibrary.strings import expand_template
from pyLibrary.maths import Math
from pyLibrary.env.logs import Log
from pyLibrary.sql.db import DB, SQL
from pyLibrary.structs import nvl
from pyLibrary.testing.fuzzytestcase import assertAlmostEqualValue
from pyLibrary.thread.threads import Thread
from pyLibrary.times.durations import Duration


ALERT_LIMIT = Math.bayesian_add(0.90, 0.70)  # SIMPLE severity*confidence LIMIT (FOR NOW)
HEADER = "<h3>Performance Regression Alert</h3>"
FOOTER = "<hr><a style='font-size:70%' href='https://wiki.mozilla.org/FirefoxOS/Performance/Investigating_Alerts'>Understanding this alert</a>"

SEPARATOR = "<hr>\n"
RESEND_AFTER = Duration(days=7)
LOOK_BACK = Duration(days=30)
MAIL_LIMIT = 10  # DO NOT SEND TOO MANY MAILS AT ONCE
EPSILON = 0.0001
VERBOSE = True
SEND_REASONS = [b2g_alert_revision.REASON, talos_alert_revision.REASON, eideticker_alert_revision.REASON]
DEBUG_TOUCH_ALL_ALERTS = False
NOW = datetime.utcnow()


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
                r.email_template,
                r.email_subject,
                r.email_style
            FROM
                alerts a
            JOIN
                reasons r on r.code = a.reason
            WHERE
                a.last_sent IS NULL AND
                a.status <> 'obsolete' AND
                math.bayesian_add(a.severity, 1-power(10, -a.confidence)) > {{alert_limit}} AND
                a.comment IS NULL AND
                a.reason in {{reasons}} AND
                a.push_date > {{min_time}}
            ORDER BY
                math.bayesian_add(a.severity, 1-power(10, -a.confidence)) DESC,
                json.number(left(details, 65000), "diff_percent") DESC
            LIMIT
                {{limit}}
        """, {
            "last_sent": datetime.utcnow() - RESEND_AFTER,
            "alert_limit": ALERT_LIMIT - EPSILON,
            "min_time": datetime.utcnow() - LOOK_BACK,
            "reasons": SQL("(" + ", ".join(db.quote_value(v) for v in SEND_REASONS) + ")"),
            "limit": MAIL_LIMIT
        })

        if not new_alerts:
            if debug:
                Log.note("Nothing important to email")
            return

        for alert in new_alerts:
            # poor souls that signed up for emails
            listeners = ";".join(db.query("SELECT email FROM listeners WHERE reason={{reason}}", {"reason": alert.reason}).email)
            body = [HEADER]

            alert.details = convert.JSON2object(alert.details)
            try:
                alert.revision = convert.JSON2object(alert.revision)
            except Exception, e:
                pass

            if alert.confidence > 4:
                alert.score = alert.confidence + log10(alert.severity + pow(10, -alert.confidence) * (1 - 2 * alert.severity)) - log10(1 - alert.severity)
                try:
                    temp = -log10(1.0 - Math.bayesian_add(alert.severity, 1.0 - (10.0 ** (-alert.confidence))))
                    assertAlmostEqualValue(alert.score, temp, digits=6)
                except Exception:
                    pass
            else:
                alert.score = str(-log10(1.0 - Math.bayesian_add(alert.severity, 1.0 - (10.0 ** (-alert.confidence)))))  # SHOW NUMBER OF NINES
            alert.details.url = alert.details.page_url
            example = alert.details.example
            for e in alert.details.tests.example + [example]:
                if e.push_date_min:
                    e.push_date_max = (2 * e.push_date) - e.push_date_min
                    e.date_range = (datetime.utcnow() - convert.milli2datetime(e.push_date_min)).total_seconds() / (24 * 60 * 60)  # REQUIRED FOR DATAZILLA B2G CHART REFERENCE
                    e.date_range = nvl(nvl(*[v for v in (7, 30, 60) if v > e.date_range]), 90)  # PICK FIRST v > CURRENT VALUE

            subject = expand_template(convert.JSON2object(alert.email_subject), alert)
            if len(subject) > 200:
                subject = subject[:197] + "..."
            body.append(expand_template(convert.JSON2object(alert.email_template), alert))
            body = "".join(body) + FOOTER
            if alert.email_style == None:
                Log.note("Email has no style")
            else:
                body = Pynliner().from_string(body).with_cssString(alert.email_style).run()

            if debug:
                Log.note("EMAIL: {{email}}", {"email": body})

            db.call("mail.send", (
                listeners, # to
                subject,
                body, # body
                None
            ))

            # I HOPE I CAN SEND ARRAYS OF NUMBERS
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
        Log.note("Running alerts off of schema {{schema|upper}}", {"schema": settings.perftest.schema})

        with DB(settings.alerts) as db:
            send_alerts(
                settings=settings,
                db=db
            )
    except Exception, e:
        Log.warning("Failure to run alerts", cause=e)
    finally:
        Thread.sleep(seconds=2)
        Log.stop()

