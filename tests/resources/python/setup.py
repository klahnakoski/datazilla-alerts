from dzAlerts.util.env import startup
from dzAlerts.util.env.logs import Log
from dzAlerts.util.sql.db import DB


def main():
    try:
        settings = startup.read_settings()
        Log.start(settings.debug)

        DB.execute_file(settings.database, settings.sql.rstrip("/")+"/util/util.sql")
        DB.execute_file(settings.database, settings.sql.rstrip("/")+"/util/debug.sql")
        DB.execute_file(settings.database, settings.sql.rstrip("/")+"/util/cnv.sql")
        DB.execute_file(settings.database, settings.sql.rstrip("/")+"/util/string.sql")
        DB.execute_file(settings.database, settings.sql.rstrip("/")+"/util/math.sql")
        DB.execute_file(settings.database, settings.sql.rstrip("/")+"/util/json.sql")
        DB.execute_file(settings.database, settings.sql.rstrip("/")+"/util/mail.sql")
        DB.execute_file(settings.database, settings.sql.rstrip("/")+"/alerts.sql")
        Log.note("DB setup complete")
    except Exception, e:
        Log.warning("Failure to setup DB", cause=e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
