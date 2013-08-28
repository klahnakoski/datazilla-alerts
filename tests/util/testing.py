################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################

import os
from util.cnv import CNV
from util.db import DB
from util.debug import D

settings_file=os.environ.get("SETTINGS_FILE")

with open(settings_file) as f:
    settings=CNV.JSON2object(f.read())


def make_test_database(settings):
    settings.database.debug=True
    no_schema=settings.database.copy()
    no_schema.schema=""

    D.println("CLEAR DATABASE ${database}", {"database":settings.database.schema})
    with DB(no_schema) as db:
        db.execute("DROP DATABASE IF EXISTS "+settings.database.schema)
        db.commit()
        db.begin()
        db.execute("CREATE DATABASE "+settings.database.schema)


    #TEMPLATE HAS {engine} TAG THAT MUST BE REPLACED
    D.println("BUILD NEW DATABASE ${database}", {"database":settings.database.schema})
    with open("C:\\Users\\klahnakoski\\git\\datazilla\\datazilla\\model\\sql\\template_schema\\schema_perftest.sql.tmpl") as f:
        content = f.read()
        content=content.replace("{engine}", "InnoDB")
    DB.execute_sql(settings.database, content)

    DB.execute_file(settings.database, "C:\\Users\\klahnakoski\\git\\datazilla\\datazilla\\model\\sql\\migration\\v1.1 alerts.sql")
    DB.execute_file(settings.database, "C:\\Users\\klahnakoski\\git\\datazilla\\datazilla\\model\\sql\\migration\\v1.2 email.sql")
    DB.execute_file(settings.database, "C:\\Users\\klahnakoski\\git\\datazilla\\datazilla\\model\\sql\\migration\\v1.3 test_data_all_dimensions.sql")

    with DB(settings.database) as db:
        db.execute("ALTER TABLE test_data_all_dimensions DROP FOREIGN KEY `fk_test_run_id_tdad`")
        db.execute("ALTER TABLE pages DROP FOREIGN KEY `fk_pages_test`")

    D.println("DATABASE READY ${database}", {"database":settings.database.schema})
