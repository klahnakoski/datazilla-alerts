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
import dzAlerts
from dzAlerts.daemons.alert import send_alerts
from dzAlerts.daemons.eideticker_alert_revision import eideticker_alert_revision
from dzAlerts.daemons.email_send import email_send
from dzAlerts.daemons.talos_alert_revision import talos_alert_revision
from dzAlerts.util.env import startup
from dzAlerts.util.env.emailer import Emailer
from dzAlerts.util.env.logs import Log
from dzAlerts.util.sql.db import DB, SQL
from dzAlerts.util.struct import nvl, Struct
from dzAlerts.util.times.durations import Duration

def main():
    settings = startup.read_settings()
    Log.start(settings.debug)
    try:
        Log.note("Setup alert for testing the email template")

        dzAlerts.daemons.eideticker_alert_revision.UPDATE_EMAIL_TEMPLATE = True
        dzAlerts.daemons.eideticker_alert_revision.NOW = datetime(2014, 9, 4)
        dzAlerts.daemons.alert.NOW = datetime(2014, 9, 4)
        dzAlerts.daemons.alert.LOOK_BACK = Duration(days=90)

        with DB(settings.alerts) as db:
            REVISION = '1fcffcc9fc4a'
            db.execute("DELETE FROM hierarchy WHERE parent IN (SELECT id FROM alerts WHERE revision={{rev}})", {"rev": REVISION})
            db.execute("DELETE FROM hierarchy WHERE child IN (SELECT id FROM alerts WHERE revision={{rev}})", {"rev": REVISION})
            db.execute("DELETE FROM alerts WHERE revision={{rev}}", {"rev": REVISION})
            db.insert("alerts", Struct(
                id=SQL("util.newid()"),
                status="NEW",
                push_date=datetime(2014, 8, 25, 12, 11),
                last_updated=datetime(2014, 8, 25, 12, 11),
                last_sent=None,
                tdad_id='{"Talos": {"Test": {"name": "56.com", "suite": "tp5o"}}, "test_run_id": 6712677}',
                reason="talos_alert_sustained_median",
                details='{"Talos": {"Branch": "Mozilla-Inbound", "OS": {"name": "mac", "version": "OS X 10.8"}, "Platform": "x86_64", "Product": "Firefox", "Revision": "1fcffcc9fc4a", "Test": {"name": "56.com", "suite": "tp5o"}}, "diff": 77.01736111111114, "diff_percent": 0.23529224567730997, "future_stats": {"count": 8, "kurtosis": -0.6941176660806985, "mean": 404.34375, "samples": [414.0, 387.0, 396.0, 327.25, 406.0, 451.0, 407.0, 329.0, 386.75, 407.0, 334.0, 427.0, 443.0, 424.0, 399.0, 418.75], "skew": -0.2975758172505008, "variance": 89.5771484375}, "ignored": false, "is_diff": true, "pass": true, "past_revision": "4e528ecd25c9", "past_stats": {"count": 12, "kurtosis": -1.4438848928913826, "mean": 327.32638888888886, "samples": [325.25, 330.0, 324.3333333333333, 327.0, 327.0, 331.0, 325.25, 336.0, 325.3333333333333, 330.0, 404.0, 329.3333333333333, 335.0, 325.0, 324.0, 328.0, 329.0, 326.75, 324.25, 324.75], "skew": 0.17205625344909786, "variance": 3.344280478428118}, "push_date": 1408968718000, "push_date_max": 1409001995000, "push_date_min": 1408867476000, "result": {"mstat": 16.2, "score": 2.986409980126293}, "test_run_id": 6712677, "ttest_result": {"score": 7.273468843310873, "tstat": -6.939915362620783}, "value": 414.0}',
                revision=REVISION,
                severity="0.8",
                confidence="7.27",
                comment=None,
                branch="Mozilla-Inbound",
                test='{"name": "56.com", "suite": "tp5o"}}',
                platform='{"name": "mac", "version": "OS X 10.8"}',
                percent="23.5%",
                keyrevision=REVISION
            ))

        talos_alert_revision(settings)

        with DB(settings.alerts) as db:
            send_alerts(
                settings=settings,
                db=db
            )
            email_send(
                db=db,
                emailer=Emailer(settings.email),
                debug=nvl(settings.debug, False)
            )
    finally:
        Log.stop()


if __name__ == '__main__':
    main()

