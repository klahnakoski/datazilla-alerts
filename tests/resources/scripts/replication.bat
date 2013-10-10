
SET PYTHONPATH=.
CALL python tests\resources\python\prod_to_objectstore.py --settings ./replication_settings.json
CALL python tests\resources\python\objectstore_to_cube.py --settings ./replication_settings.json