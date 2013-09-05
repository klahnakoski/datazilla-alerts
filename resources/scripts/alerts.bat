REM RUN FROM MAIN DIRECTORY


python dzAlerts\daemons\alert_exception.py  --settings_file "./alert_settings.json"

python dzAlerts\daemons\alert.py  --settings_file "./alert_settings.json"

python dzAlerts\daemons\email_send.py  --settings_file "./alert_settings.json"