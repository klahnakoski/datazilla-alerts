REM RUN FROM MAIN DIRECTORY

SET PYTHONPATH=.

CALL python dzAlerts\daemons\alert_sustained_median.py  --settings_file "./b2g_settings.json"
