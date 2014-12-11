REM RUN FROM MAIN DIRECTORY

SET PYTHONPATH=.

CALL pypy dzAlerts\imports\treeherder.py  --settings_file "./treeherder_settings.json"
