REM RUN FROM MAIN DIRECTORY

SET PYTHONPATH=.

CALL python dzAlerts\daemons\talos_sustained_median.py  --settings_file "./talos_alert_settings.json"
CALL python dzAlerts\daemons\b2g_revision.py  --settings_file "./talos_alert_settings.json"
CALL python dzAlerts\daemons\alert.py  --settings_file "./alert_settings.json"
CALL python dzAlerts\daemons\email_send.py  --settings_file "./alert_settings.json"
