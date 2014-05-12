REM RUN FROM MAIN DIRECTORY

SET PYTHONPATH=.

CALL pypy dzAlerts\daemons\talos_sustained_median.py  --settings_file "./talos_settings.json"
CALL pypy dzAlerts\daemons\talos_alert_revision.py  --settings_file "./talos_settings.json"
CALL pypy dzAlerts\daemons\alert.py  --settings_file "./talos_settings.json"
CALL pypy dzAlerts\daemons\email_send.py  --settings_file "./talos_settings.json"
