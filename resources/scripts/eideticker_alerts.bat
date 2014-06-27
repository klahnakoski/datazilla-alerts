REM RUN FROM MAIN DIRECTORY

SET PYTHONPATH=.
CALL pypy dzAlerts\daemons\sustained_median.py  --settings_file "./eideticker_settings.json"
