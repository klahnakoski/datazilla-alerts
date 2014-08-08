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

        dzAlerts.daemons.eideticker_alert_revision.NOW = datetime(2014, 8, 8)
        dzAlerts.daemons.alert.NOW = datetime(2014, 8, 8)
        dzAlerts.daemons.alert.LOOK_BACK = Duration(days=90)

        with DB(settings.alerts) as db:
            db.execute("DELETE FROM hierarchy WHERE parent IN (SELECT id FROM alerts WHERE revision='2d88803a0b9c')")
            db.execute("DELETE FROM alerts WHERE revision='2d88803a0b9c'")
            db.insert("alerts", Struct(
                id=SQL("util.newid()"),
                status="new",
                create_time=datetime(2014, 7, 9, 6, 6),
                last_updated=datetime(2014, 8, 8, 0, 17),
                last_sent=None,
                tdad_id='{"path": "/b2g", "uuid": "fab6e420077f11e4a8d510ddb19eacac"}',
                reason="eideticker_alert_sustained_median",
                details='{"Eideticker": {"Branch": "b2g-nightly", "Device": "flame", "Revision": "2d88803a0b9c", "Test": "b2g-gallery-startup", "Version": "mozilla-central"}, "diff": 0.4541666666666666, "diff_percent": 0.13885350318471334, "future_stats": {"count": 4, "kurtosis": -1.3600001258336032, "mean": 3.725, "samples": [3.8333333333333335, 3.783333333333333, 3.75, 3.716666666666667, 3.7, 3.7333333333333334, 3.683333333333333, 3.7], "skew": 0.0, "variance": 0.00034722222222249854}, "ignored": false, "is_diff": true, "pass": true, "past_revision": "196d05832e12", "past_stats": {"count": 4, "kurtosis": -0.7174301121761535, "mean": 3.2708333333333335, "samples": [3.183333333333333, 3.216666666666667, 3.25, 3.25, 3.3666666666666667, 3.35, 3.4, 3.2333333333333334], "skew": 1.0776051731667742, "variance": 0.002135416666666501}, "path": "/b2g", "push_date": 1404885995000, "push_date_max": 1405000789000, "push_date_min": 1404709699000, "result": {"mstat": 16.0, "score": 2.9453929650517896}, "ttest_result": {"score": 8.790657193438616, "tstat": -13.737419509221672}, "uuid": "fab6e420077f11e4a8d510ddb19eacac", "value": 3.8333333333333335}',
                revision="2d88803a0b9c",
                severity="0.8",
                confidence="8.790657193",
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

