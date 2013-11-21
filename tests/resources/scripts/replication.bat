
SET PYTHONPATH=.
pypy tests\resources\python\prod_to_objectstore.py --settings ./replication_settings.json
pypy tests\resources\python\objectstore_to_cube.py --settings ./replication_settings.json