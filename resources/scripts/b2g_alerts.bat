REM RUN FROM MAIN DIRECTORY

SET PYTHONPATH=.

CALL pypy dzAlerts\daemons\sustained_median.py  --settings_file "./b2g_settings.json"
CALL pypy dzAlerts\daemons\b2g_alert_revision.py  --settings_file "./b2g_settings.json"
CALL pypy dzAlerts\daemons\alert.py  --settings_file "./b2g_settings.json"
CALL pypy dzAlerts\daemons\email_send.py  --settings_file "./b2g_settings.json"
