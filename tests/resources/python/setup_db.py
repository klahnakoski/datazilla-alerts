from pyLibrary.env import startup
from pyLibrary.env.logs import Log
from pyLibrary.sql.db import DB


def main():
    try:
        settings = startup.read_settings()
        Log.start(settings.debug)
        settings.database.schema = None  # DB CREATED, IGNORED

        for i in range(2):
            # DUE TO CIRCULAR DEPENDENCIES, RUN THESE TWICE
            DB.execute_file(settings.database, settings.sql.rstrip("/")+"/util/util.sql", ignore_errors=True)
            DB.execute_file(settings.database, settings.sql.rstrip("/")+"/util/debug.sql", ignore_errors=True)
            DB.execute_file(settings.database, settings.sql.rstrip("/")+"/util/cnv.sql", ignore_errors=True)
            DB.execute_file(settings.database, settings.sql.rstrip("/")+"/util/string.sql", ignore_errors=True)
            DB.execute_file(settings.database, settings.sql.rstrip("/")+"/util/math.sql", ignore_errors=True)
            DB.execute_file(settings.database, settings.sql.rstrip("/")+"/util/json.sql", ignore_errors=True)
            DB.execute_file(settings.database, settings.sql.rstrip("/")+"/util/mail.sql", ignore_errors=True)

        DB.execute_file(settings.database, settings.sql.rstrip("/")+"/alerts.sql")
        DB.execute_file(settings.database, settings.sql.rstrip("/")+"/alerts_migrate01.sql")
        DB.execute_file(settings.database, settings.sql.rstrip("/")+"/alerts_migrate02.sql")

        Log.note("DB setup complete")
    except Exception, e:
        Log.warning("Failure to setup DB", cause=e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
