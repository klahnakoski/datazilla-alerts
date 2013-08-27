REM RUN FROM MAIN DIRECTORY


python src\python\datazilla.daemons.alert_exception.py  --settings_file "./alert_settings.json"

python src\python\datazilla.daemons.alert.py  --settings_file "./alert_settings.json"

python src\python\datazilla.daemons.email_send.py  --settings_file "./alert_settings.json"