REM RUN FROM MAIN DIRECTORY

SET PYTHONPATH=.

REM CALL python dzAlerts\daemons\alert_exception.py  --settings_file "./alert_settings.json"
REM CALL python dzAlerts\daemons\alert_revision.py  --settings_file "./alert_settings.json"
CALL python dzAlerts\daemons\alert_sustained.py  --settings_file "./alert_settings.json"
CALL python dzAlerts\daemons\alert_regression.py  --settings_file "./alert_settings.json"
CALL python dzAlerts\daemons\alert.py  --settings_file "./alert_settings.json"
CALL python dzAlerts\daemons\email_send.py  --settings_file "./alert_settings.json"
