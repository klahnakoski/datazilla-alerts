
################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## Author: Kyle Lahnakoski (kyle@lahnakoski.com)
################################################################################


from datetime import datetime, timedelta
from dzAlerts.util.cnv import CNV
from dzAlerts.util.strings import expand_template
from dzAlerts.util.maths import Math
from dzAlerts.util.logs import Log
from dzAlerts.util.db import DB
from dzAlerts.util.startup import startup



ALERT_LIMIT = Math.bayesian_add(0.90, 0.70)  #SIMPLE severity*confidence LIMIT (FOR NOW)
HEADER = "<h2>This is for testing only.  It may be misleading.</h2><br><br>"
#TBPL link: https://tbpl.mozilla.org/?rev=c3598b276048
#TBPL test results:  https://tbpl.mozilla.org/?tree=Mozilla-Inbound&rev=c9429cf294af
#HG: https://hg.mozilla.org/mozilla-central/rev/330feedee4f1
#BZ: https://bugzilla.mozilla.org/show_bug.cgi?id=702559
#DZ: https://datazilla.mozilla.org/talos/summary/Mozilla-Inbound/897654df47b6?product=Firefox&branch_version=23.0a1
#                            "product":v.product,
#                            "branch":v.branch,
#                            "branch_version":v.branch_version,
#                            "revision":v.revision
TEMPLATE =  """<div><h2>{{score}} - {{revision}}</h2>{{reason}}<br>
            On page {{page_url}}<br>
            <a href=\"https://tbpl.mozilla.org/?tree={{branch}}&rev={{revision}}\">TBPL</a><br>
            <a href=\"https://hg.mozilla.org/rev/{{revision}}\">Mercurial</a><br>
            <a href=\"https://bugzilla.mozilla.org/show_bug.cgi?id={{bug_id}}\">Bugzilla - {{bug_description}}</a><br>
            <a href=\"https://datazilla.mozilla.org/talos/summary/{{branch}}/{{revision}}\">Datazilla</a><br>
            <a href=\"http://people.mozilla.com/~klahnakoski/test/es/DZ-ShowPage.html#page={{page_url}}&sampleMax={{push_date}}000&sampleMin={{push_date_min}}000&branch={{branch}}\">Kyle's ES</a><br>
            Raw data: {{raw_data}}
            </div>"""
SEPARATOR = "<hr>\n"
RESEND_AFTER = timedelta(days = 1)
MAX_EMAIL_LENGTH = 8000
EPSILON = 0.0001



def send_alerts(db, debug):
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
                t.revision,
                t.branch
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
                bayesian_add(a.severity, a.confidence) DESC
            """, {
                "last_sent":datetime.utcnow()-RESEND_AFTER,
                "alert_limit":ALERT_LIMIT-EPSILON
            })

        if len(new_alerts)==0:
            if env.debug: Log.note("Nothing important to email")
            return

        body=[HEADER]
        for alert in new_alerts:
            if alert.confidence>=1: alert.confidence = 0.999999
            
            details = CNV.JSON2object(alert.details)
            for k,v in alert.items():
                if k not in details:
                    details[k]=v
            details.score = str(round(Math.bayesian_add(alert.severity, alert.confidence)*100, 0))+"%"  #AS A PERCENT
            details.reason = expand_template(alert.description, details)
            body.append(expand_template(TEMPLATE, details))
        body = SEPARATOR.join(body)

#        listeners = SQLQuery.run({
#            "select":{"value":"email"},
#            "from":"alert_email_listener"
#        })
        #poor souls that signed up for emails
        listeners = db.query("SELECT email FROM alert_listeners")
        listeners = [x["email"] for x in listeners]
        listeners = ";".join(listeners)

        if len(body)>MAX_EMAIL_LENGTH:
            Log.note("Truncated the email body")
            suffix="... (has been truncated)"
            body = body[0:MAX_EMAIL_LENGTH-len(suffix)]+suffix   #keep it reasonable

        db.call("email_send", (
            listeners, #to
            "Bad news from tests", #title
            body, #body
            None
        ))

        #I HOPE I CAN SEND ARRAYS OF NUMBERS
        if len(new_alerts)>0:
            db.execute(
                "UPDATE alerts SET last_sent={{time}} WHERE id IN {{send_list}}",
                {"time":datetime.utcnow(), "send_list":[a["alert_id"] for a in new_alerts]}
            )

    except Exception, e:
        Log.error("Could not send alerts", e)

        
# REVIEW THE ALERT TABLE AND ENSURE THE test_data_all_dimensions(h0_rejected)
# COLUMN REFLECTS THE ALERT STATI
def update_h0_rejected(db, start_date):
    db.execute("""
        UPDATE 
            test_data_all_dimensions t
        JOIN (
            SELECT
                tdad_id,
                max(CASE WHEN status<>'obsolete' THEN 1 ELSE 0 END) h0
            FROM
                alerts
            GROUP BY
                tdad_id
            ) a ON a.tdad_id = t.id
        SET t.h0_rejected = a.h0
    """)


#ARE THESE SEVERITY OR CONFIDENCE NUMBERS SIGNIFICANTLY DIFFERENT TO WARRANT AN
#UPDATE?
SIGNIFICANT = 0.2
def significant_difference(a, b):
    return (1-SIGNIFICANT)<a/b or a/b<(1+SIGNIFICANT)




if __name__ == '__main__':
    settings = startup.read_settings()
    Log.start(settings.debug)

    try:
        Log.note("Running alerts off of schema {{schema}}", {"schema":settings.database.schema})

        with DB(settings.database) as db:
            send_alerts(
                db = db,
                debug = settings.debug is not None
            )
    except Exception, e:
        Log.warning("Failure to run alerts", cause = e)
    finally:
        Log.stop()

