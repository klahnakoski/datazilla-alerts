REM RUN FROM MAIN DIRECTORY

SET PYTHONPATH=.

CALL python dzAlerts\daemons\sustained_median.py  --settings_file "./b2g_settings.json"
CALL python dzAlerts\daemons\b2g_alert_revision.py  --settings_file "./b2g_settings.json"
CALL python dzAlerts\daemons\alert.py  --settings_file "./b2g_settings.json"
CALL python dzAlerts\daemons\email_send.py  --settings_file "./b2g_settings.json"
