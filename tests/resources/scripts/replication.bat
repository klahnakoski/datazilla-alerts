
SET PYTHONPATH=.
python tests\resources\python\prod_to_objectstore.py --settings ./replication_settings.json
python tests\resources\python\objectstore_to_cube.py --settings ./replication_settings.json