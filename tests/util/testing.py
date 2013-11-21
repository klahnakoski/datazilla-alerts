
################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################

from dzAlerts.util.db import DB
from dzAlerts.util.logs import Log
from dzAlerts.util.struct import StructList


class Emailer:
    """
    dummy emailer
    """

    
    def __init__(self, settings):
        self.settings=settings
        self.sent=StructList()


    def send_email(self, **args):
        self.sent.append(args)      #SIMPLY RECORD THE CALL FOR LATER VERIFICATION



        
def make_test_database(settings):
    try:
        settings.database.debug=True
        no_schema=settings.database.copy()
        no_schema.schema=""

        Log.note("CLEAR DATABASE {{database}}", {"database":settings.database.schema})
        with DB(no_schema) as db:
            db.execute("DROP DATABASE IF EXISTS "+settings.database.schema)
            db.flush()
            db.execute("CREATE DATABASE "+settings.database.schema)


        #TEMPLATE HAS {engine} TAG THAT MUST BE REPLACED
        Log.note("BUILD NEW DATABASE {{database}}", {"database":settings.database.schema})
        DB.execute_file(settings.database, "tests/resources/sql/schema_perftest.sql")
        DB.execute_file(settings.database, "tests/resources/sql/Add test_data_all_dimensions.sql")

        Log.note("MIGRATE {{database}} TO NEW SCHEMA", {"database":settings.database.schema})
        DB.execute_file(settings.database, "resources/migration/v1.1 alerts.sql")
        DB.execute_file(settings.database, "resources/migration/v1.2 email.sql")

        with DB(settings.database) as db:
            db.execute("ALTER TABLE test_data_all_dimensions DROP FOREIGN KEY `fk_test_run_id_tdad`")
            db.execute("ALTER TABLE pages DROP FOREIGN KEY `fk_pages_test`")
            db.execute("DELETE FROM email_delivery")
            db.execute("DELETE FROM email_attachment")
            db.execute("DELETE FROM email_content")

        Log.note("DATABASE READY {{database}}", {"database":settings.database.schema})
    except Exception, e:
        Log.error("Database setup failed", e)