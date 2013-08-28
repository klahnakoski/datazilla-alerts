REM RUN FROM MAIN DIRECTORY


python src\python\daemons\alert_exception.py  --settings_file "./alert_settings.json"

python src\python\daemons\alert.py  --settings_file "./alert_settings.json"

python src\python\daemons\email_send.py  --settings_file "./alert_settings.json"