REM RUN FROM MAIN DIRECTORY

SET PYTHONPATH=.

CALL pypy dzAlerts\imports\treeherder.py  --settings_file "resources/config/treeherder_dev_settings.json"
