from datetime import datetime
import dzAlerts
from dzAlerts.daemons.alert import send_alerts
from dzAlerts.daemons.eideticker_alert_revision import eideticker_alert_revision
from dzAlerts.daemons.email_send import email_send
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

        dzAlerts.daemons.eideticker_alert_revision.DEBUG_UPDATE_EMAIL_TEMPLATE = True
        dzAlerts.daemons.eideticker_alert_revision.NOW = datetime(2014, 8, 28)
        dzAlerts.daemons.alert.NOW = datetime(2014, 8, 28)
        dzAlerts.daemons.alert.LOOK_BACK = Duration(days=90)

        with DB(settings.alerts) as db:
            REVISION = '70ce13bca890'
            db.execute("DELETE FROM hierarchy WHERE parent IN (SELECT id FROM alerts WHERE revision={{rev}})", {"rev": REVISION})
            db.execute("DELETE FROM hierarchy WHERE child IN (SELECT id FROM alerts WHERE revision={{rev}})", {"rev": REVISION})
            db.execute("DELETE FROM alerts WHERE revision={{rev}}", {"rev": REVISION})
            db.insert("alerts", Struct(
                id=SQL("util.newid()"),
                status="new",
                create_time=datetime(2014, 8, 15, 2, 52),
                last_updated=datetime(2014, 8, 21, 18, 30),
                last_sent=None,
                tdad_id='{"metric": "timetostableframe", "path": "/b2g", "uuid": "b180c5a8247d11e4b12b10ddb19e8514"}',
                reason="eideticker_alert_sustained_median",
                details='{"Eideticker": {"App": "b2g-nightly", "Branch": "b2g-inbound", "Device": "flame-512", "Revision": "70ce13bca890", "Test": "b2g-dialer-startup"}, "diff": -0.5888888888888889, "diff_percent": -1.0, "future_stats": {"count": 6, "mean": 0.0, "samples": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.7, 0.0, 0.0, 0.4666666666666667], "variance": 0}, "ignored": false, "is_diff": true, "metric": "timetostableframe", "pass": true, "past_revision": "55c89fcd3b1b", "past_stats": {"count": 6, "kurtosis": -1.9650294009192446, "mean": 0.5888888888888889, "samples": [0.5166666666666667, 0.5, 0.6833333333333333, 0.5, 0.6666666666666666, 0.6833333333333333, 0.6666666666666666, 0.5, 0.48333333333333334, 0.7166666666666667], "skew": 0.0005847782480937579, "variance": 0.007006172839506164}, "path": "/b2g", "push_date": 1408071135000, "push_date_max": 1408251734000, "push_date_min": 1407733928000, "result": {"mstat": 12.8, "score": 2.29330794920446}, "ttest_result": {"score": 4.547369230017969, "tstat": 5.555580184309633}, "uuid": "b180c5a8247d11e4b12b10ddb19e8514", "value": 0.0}',
                revision=REVISION,
                severity="0.8",
                confidence="4.54736923002",
                solution=None
            ))

        eideticker_alert_revision(settings)

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

