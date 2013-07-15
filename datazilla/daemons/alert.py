from datetime import datetime, timedelta
import json
from string import Template
from datazilla.util.cnv import CNV
from datazilla.util.map import Map
from datazilla.util.maths import bayesian_add
from datazilla.util.debug import D


ALERT_LIMIT = bayesian_add(0.90, 0.70)  #SIMPLE severity*confidence LIMIT (FOR NOW)
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
TEMPLATE = Template("<div><h2>${score} - ${revision}</h2>${reason}<br>\n"+
                    "<a href=\"https://tbpl.mozilla.org/?tree=${branch}&rev=${revision}\">TBPL</a><br>\n"+
                    "<a href=\"https://hg.mozilla.org/rev/${revision}\">Mercurial - ${hg_description}</a><br>\n"+
                    "<a href=\"https://bugzilla.mozilla.org/show_bug.cgi?id=${bug_id}\">Bugzilla - ${bug_description}</a><br>\n"+
                    "<a href=\"https://datazilla.mozilla.org/talos/summary/${branch}/${revision}?product=${product}&branch_version=${branch_version}\">Datazilla</a><br>\n"+
                    "Raw data: ${raw_data}"+
                    "</div>\n")
SEPARATOR = "<hr>\n"
RESEND_AFTER = timedelta(days=1)
MAX_EMAIL_LENGTH = 8000
EPSILON = 0.0001



def send_alerts(**env):
    env=Map(**env)
    assert env.db is not None

    db = env.db
    db.debug = env.debug

    try:
        new_alerts = db.query("""
            SELECT
                a.id alert_id,
                a.reason,
                r.description,
                a.details,
                a.severity,
                a.confidence,
                t.revision
            FROM
                alert_mail a
            JOIN
                alert_reasons r on r.code=a.reason
            JOIN
                test_data_all_dimensions t ON t.id=a.test_series
            WHERE
                (
                    a.last_sent IS NULL OR
                    a.last_sent < a.last_updated OR
                    a.last_sent < ${last_sent}
                ) AND
                a.status <> 'obsolete' AND
                bayesian_add(a.severity, a.confidence) > ${alert_limit} AND
                a.solution IS NULL
            ORDER BY
                bayesian_add(a.severity, a.confidence) DESC
            """, {
                "last_sent":datetime.utcnow()-RESEND_AFTER,
                "alert_limit":ALERT_LIMIT-EPSILON
            })

        if len(new_alerts)==0:
            if env.debug: D.println("Nothing important to email")
            return

        body=[HEADER]
        for alert in new_alerts:
            details=CNV.JSON2object(alert.details)
            #EXPAND THE MESSAGE
            if alert.confidence>=1: alert.confidence=0.999999999
            body.append(TEMPLATE.substitute({
                "score":str(round(bayesian_add(alert.severity, alert.confidence)*100, 0))+"%",  #AS A PERCENT
                "revision":alert.revision,
                "reason":Template(alert.description).substitute(details)
                }))
        body=SEPARATOR.join(body)

#        listeners = SQLQuery.run({
#            "select":{"value":"email"},
#            "from":"alert_email_listener"
#        })
        #poor souls that signed up for emails
        listeners=db.query("SELECT email FROM alert_listeners")
        listeners = [x["email"] for x in listeners]
        listeners = ";".join(listeners)

        if len(body)>MAX_EMAIL_LENGTH:
            D.println("Truncated the email body")
            suffix="... (has been truncated)"
            body=body[0:MAX_EMAIL_LENGTH-len(suffix)]+suffix   #keep it reasonable

        db.call("email_send", (
            listeners, #to
            "Bad news from tests", #title
            body, #body
            None
        ))

        #I HOPE I CAN SEND ARRAYS OF NUMBERS
        if len(new_alerts)>0:
            db.execute(
                "UPDATE alert_mail SET last_sent=${time} WHERE id IN ${send_list}",
                {"time":datetime.utcnow(), "send_list":[a["alert_id"] for a in new_alerts]}
            )

    except Exception, e:
        D.error("Could not send alerts", e)

        
# REVIEW THE ALERT TABLE AND ENSURE THE test_data_all_dimensions(h0_rejected)
# COLUMN REFLECTS THE ALERT STATI
def update_h0_rejected(db, start_date):
    db.execute("""
        UPDATE 
            test_data_all_dimensions t
        JOIN (
            SELECT
                test_series,
                max(CASE WHEN status<>'obsolete' THEN 1 ELSE 0 END) h0
            FROM
                alert_mail
            GROUP BY
                test_series
            ) a ON a.test_series=t.id
        SET t.h0_rejected=a.h0
    """)
    