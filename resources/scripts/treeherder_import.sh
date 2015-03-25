cd /home/ec2-user/datazilla-alerts
git pull origin treeherder

export PYTHONPATH=.
python27 dzAlerts/imports/treeherder.py  --settings_file resources/config/treeherder_staging_settings.json
