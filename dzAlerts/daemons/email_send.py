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
from dzAlerts.util.env import startup
from dzAlerts.util.env.logs import Log
from dzAlerts.util.sql.db import DB
from dzAlerts.util.env.emailer import Emailer
from dzAlerts.util.struct import nvl


def email_send(db, emailer, debug):
    db.debug = debug

    ## VERIFY self SHOULD BE THE ONE PERFORMING OPS (TO PREVENT MULTIPLE INSTANCES NEEDLESSLY RUNNING)
    try:

        ## EXIT EARLY IF THERE ARE NO EMAILS TO SEND
        has_mail = db.query("SELECT max(new_mail) new_mail FROM mail.notify")
        if has_mail[0]["new_mail"] == 0:
            Log.note("No emails to send")
            return

        ## GET LIST OF MAILS TO SEND
        emails = db.query("""
            SELECT
                c.id,
                group_concat(d.deliver_to SEPARATOR ',') `to`,
                c.subject,
                c.body
            FROM
                mail.content c
            LEFT JOIN
                mail.delivery d ON d.content=c.id
            WHERE
                d.content IS NOT NULL AND
                c.date_sent IS NULL
            GROUP BY
                c.id

            """)

        ## SEND MAILS
        not_done = 0   ## SET TO ONE IF THERE ARE MAIL FAILURES, AND THERE ARE MAILS STILL LEFT TO SEND
        num_done = 0
        with emailer:
            for email in emails:
                try:
                    emailer.send_email(
                        to_addrs=email.to.split(','),
                        subject=email.subject,
                        html_data=email.body
                    )

                    db.execute("UPDATE mail.content SET date_sent={{now}} WHERE id={{id}}", {"id": email.id, "now": datetime.utcnow()})
                    db.flush()
                    num_done += len(email.to.split(','))
                except Exception, e:
                    Log.warning("Problem sending email", e)
                    not_done = 1

            db.execute("UPDATE mail.notify SET new_mail={{not_done}}", {"not_done": not_done})

        Log.note(str(num_done) + " emails have been sent")
    except Exception, e:
        Log.error("Could not send emails", e)


def main():
    settings = startup.read_settings()
    Log.start(settings.debug)
    try:
        Log.note("Running email using schema {{schema}}", {"schema": settings.perftest.schema})
        with DB(settings.alerts) as db:
            email_send(
                db=db,
                emailer=Emailer(settings.email),
                debug=nvl(settings.debug, False)
            )
    except Exception, e:
        Log.warning("Failure to send emails", cause=e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
