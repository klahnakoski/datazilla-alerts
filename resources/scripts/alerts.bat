REM RUN FROM MAIN DIRECTORY

SET PYTHONPATH=.

CALL python dzAlerts\daemons\alert_exception.py  --settings_file "./alert_settings.json"
REM CALL python dzAlerts\daemons\alert.py  --settings_file "./alert_settings.json"
REM CALL python dzAlerts\daemons\email_send.py  --settings_file "./alert_settings.json"
