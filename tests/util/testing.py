
################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################

import os
from dzAlerts.util.cnv import CNV
from dzAlerts.util.db import DB
from dzAlerts.util.debug import D
from dzAlerts.util.files import File


def make_test_database(settings):
    try:
        settings.database.debug=True
        no_schema=settings.database.copy()
        no_schema.schema=""

        D.println("CLEAR DATABASE {{database}}", {"database":settings.database.schema})
        with DB(no_schema) as db:
            db.execute("DROP DATABASE IF EXISTS "+settings.database.schema)
            db.flush()
            db.execute("CREATE DATABASE "+settings.database.schema)


        #TEMPLATE HAS {engine} TAG THAT MUST BE REPLACED
        D.println("BUILD NEW DATABASE {{database}}", {"database":settings.database.schema})
        DB.execute_file(settings.database, "tests/resources/sql/schema_perftest.sql")
        DB.execute_file(settings.database, "tests/resources/sql/Add test_data_all_dimensions.sql")

        D.println("MIGREATE {{database}} TO NEW SCHEMA", {"database":settings.database.schema})
        DB.execute_file(settings.database, "resources/migration/v1.1 alerts.sql")
        DB.execute_file(settings.database, "resources/migration/v1.2 email.sql")

        with DB(settings.database) as db:
            db.execute("ALTER TABLE test_data_all_dimensions DROP FOREIGN KEY `fk_test_run_id_tdad`")
            db.execute("ALTER TABLE pages DROP FOREIGN KEY `fk_pages_test`")

        D.println("DATABASE READY {{database}}", {"database":settings.database.schema})
    except Exception, e:
        D.error("Database setup failed", e)