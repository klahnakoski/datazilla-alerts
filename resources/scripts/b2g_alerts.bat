REM RUN FROM MAIN DIRECTORY

SET PYTHONPATH=.

CALL python dzAlerts\daemons\b2g_sustained_median.py  --settings_file "./b2g_settings.json"
