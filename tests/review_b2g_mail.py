from datetime import datetime
import dzAlerts
from dzAlerts.daemons.alert import send_alerts
from dzAlerts.daemons.b2g_alert_revision import b2g_alert_revision
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

        dzAlerts.daemons.eideticker_alert_revision.UPDATE_EMAIL_TEMPLATE = True
        dzAlerts.daemons.eideticker_alert_revision.NOW = datetime(2014, 9, 4)
        dzAlerts.daemons.alert.NOW = datetime(2014, 9, 4)
        dzAlerts.daemons.alert.LOOK_BACK = Duration(days=90)

        with DB(settings.alerts) as db:
            REVISION = '{"gaia": "4f4a6988d65525d07058b2fcb93eb1ae6387d3ac", "gecko": "6b1690ad2cf5"}'
            db.execute("DELETE FROM hierarchy WHERE parent IN (SELECT id FROM alerts WHERE revision={{rev}})", {"rev": REVISION})
            db.execute("DELETE FROM hierarchy WHERE child IN (SELECT id FROM alerts WHERE revision={{rev}})", {"rev": REVISION})
            db.execute("DELETE FROM alerts WHERE revision={{rev}}", {"rev": REVISION})
            db.insert("alerts", Struct(
                id=SQL("util.newid()"),
                status="new",
                create_time=datetime(2014, 8, 29, 11, 21),
                last_updated=datetime(2014, 9, 03, 12, 31),
                last_sent=None,
                tdad_id='{"B2G": {"Test": {"name": "startup_>_moz-app-visually-complete", "suite": "video"}}, "test_run_id": 425046}',
                reason="b2g_alert_sustained_median",
                details='{"B2G": {"Branch": "master", "Device": "flame-319MB", "OS": "Firefox OS", "Platform": "Gonk", "Revision": {"gaia": "4f4a6988d65525d07058b2fcb93eb1ae6387d3ac", "gecko": "6b1690ad2cf5"}, "Test": {"name": "startup_>_moz-app-visually-complete", "suite": "video"}}, "diff": 31.68424008333318, "diff_percent": 0.0295342085433644, "future_stats": {"count": 6, "kurtosis": -1.109595044734751, "mean": 1104.48224775, "samples": [1114.528724, 1084.8174479999998, 1105.3508325, 1121.1980985, 1094.294634, 1110.3117969999998, 1102.1879945, 1112.0326559999999, 1101.4671615, 1095.543045], "skew": -0.12031377608579173, "variance": 31.0017395906616}, "ignored": false, "is_diff": true, "pass": true, "past_revision": {"gaia": "ae42e2ced7a0766e2ff4a1ef8b6c3fa5c9fe2eba", "gecko": "72f99f685cc6"}, "past_stats": {"count": 6, "kurtosis": -0.45857757914149166, "mean": 1072.7980076666668, "samples": [1078.0291404999998, 1071.8390359999999, 1050.3552345, 1071.4365885000002, 1076.1577080000002, 1048.4376035, 1099.3075250000002, 1074.5034895000001, 1082.205547, 1064.8220835], "skew": -0.7092727077162456, "variance": 17.991576741915196}, "push_date": 1409360039000, "push_date_max": 1409409172000, "push_date_min": 1409321961000, "result": {"mstat": 12.8, "score": 2.29330794920446}, "test_run_id": 425046, "ttest_result": {"score": 4.574637878443277, "tstat": -5.5854556322320255}, "value": 1114.528724}',
                revision=REVISION,
                severity="0.8",
                confidence="4.574",
                solution=None
            ))

        b2g_alert_revision(settings)

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

