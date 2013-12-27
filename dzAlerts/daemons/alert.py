################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## Author: Kyle Lahnakoski (kyle@lahnakoski.com)
################################################################################


from datetime import datetime, timedelta
from dzAlerts.util.cnv import CNV
from dzAlerts.util.queries import Q
from dzAlerts.util.strings import expand_template
from dzAlerts.util.maths import Math
from dzAlerts.util.logs import Log
from dzAlerts.util.db import DB
from dzAlerts.util import startup

ALERT_LIMIT = Math.bayesian_add(0.90, 0.70)  #SIMPLE severity*confidence LIMIT (FOR NOW)
HEADER = "<h3>This is for testing only.  It may be misleading.</h3><br><br>"
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
MAX_EMAIL_LENGTH = 15000
EPSILON = 0.0001


def send_alerts(settings, db):
    """
    BLINDLY SENDS ALERTS FROM THE ALERTS TABLE, ASSUMING ALL HAVE THE SAME STRUCTURE.
    THIS SHOULD BE CHANGED SO EACH TYPE OF ALERT IS RESPONSBLE FOR IT'S OWN TEMPLATE
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
                t.branch,
                r.email_template
            FROM
                alerts a
            JOIN
                alert_reasons r on r.code = a.reason
            JOIN
                test_data_all_dimensions t ON t.id = a.tdad_id
            WHERE
                (
                    a.last_sent IS NULL OR
                    a.last_sent < a.last_updated OR
                    a.last_sent < {{last_sent}}
                ) AND
                a.status <> 'obsolete' AND
                bayesian_add(a.severity, a.confidence) > {{alert_limit}} AND
                a.solution IS NULL
            ORDER BY
                bayesian_add(a.severity, a.confidence) DESC,
                json.number(details, "diff") DESC
            LIMIT
                1000
            """, {
            "last_sent": datetime.utcnow() - RESEND_AFTER,
            "alert_limit": ALERT_LIMIT - EPSILON
        })

        if len(new_alerts) == 0:
            if debug:
                Log.note("Nothing important to email")
            return

        body = [HEADER]
        for alert in new_alerts:
            if alert.confidence >= 1:
                alert.confidence = 0.999999

            details = CNV.JSON2object(alert.details)
            for k, v in alert.items():
                if k not in details:
                    details[k] = v
            details.score = str(round(Math.bayesian_add(alert.severity, alert.confidence) * 100, 0)) + "%"  #AS A PERCENT
            details.ulr = details.page_url
            if details.push_date != None and details.push_date_min != None:
                details.push_date_max = (2 * details.push_date) - details.push_date_min
            details.reason = expand_template(alert.description, details)

            body.append(expand_template(alert.email_template, details))
        body = SEPARATOR.join(body)

        #poor souls that signed up for emails
        listeners = db.query("SELECT email FROM alert_listeners")
        listeners = [x["email"] for x in listeners]
        listeners = ";".join(listeners)

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
        if len(new_alerts) > 0:
            db.execute(
                "UPDATE alerts SET last_sent={{time}} WHERE {{where}}", {
                    "time": datetime.utcnow(),
                    "where": db.esfilter2sqlwhere({"terms": {"id": Q.select(new_alerts, "alert_id")}})
                })

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
        "where": db.esfilter2sqlwhere({"terms": {"a.tdad_id": possible_alerts}})
    })


#ARE THESE SEVERITY OR CONFIDENCE NUMBERS SIGNIFICANTLY DIFFERENT TO WARRANT AN
#UPDATE?
SIGNIFICANT = 0.2


def significant_difference(a, b):
    if a / b < (1 - SIGNIFICANT) or (1 + SIGNIFICANT) < a/b:
        return True
    if a in (0.0, 1.0) or b in (0.0, 1.0):
        return True
    b_diff = Math.bayesian_subtract(a, b)
    if 0.3 < b_diff < 0.7:
        return False
    return True


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

