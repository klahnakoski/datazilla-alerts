REM RUN FROM MAIN DIRECTORY

SET PYTHONPATH=.
CALL pypy dzAlerts\import\eideticker_import.py --settings=eideticker_import_settings.json
CALL pypy dzAlerts\import\eideticker_import.py --settings=eideticker_import_b2g_settings.json
CALL pypy dzAlerts\daemons\sustained_median.py  --settings_file "./eideticker_settings.json"
CALL pypy dzAlerts\daemons\eideticker_alert_revision.py  --settings_file "./eideticker_settings.json"
REM CALL pypy dzAlerts\daemons\alert.py  --settings_file "./eideticker_settings.json"
REM CALL pypy dzAlerts\daemons\email_send.py  --settings_file "./eideticker_settings.json"
