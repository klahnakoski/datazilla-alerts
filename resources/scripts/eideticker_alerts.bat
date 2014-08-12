REM RUN FROM MAIN DIRECTORY

SET PYTHONPATH=.
CALL pypy dzAlerts\daemons\sustained_median.py  --settings_file "./eideticker_settings.json"
CALL pypy dzAlerts\daemons\eideticker_alert_revision.py  --settings_file "./eideticker_settings.json"
CALL pypy dzAlerts\daemons\alert.py  --settings_file "./eideticker_settings.json"
CALL pypy dzAlerts\daemons\email_send.py  --settings_file "./eideticker_settings.json"
