# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import unicode_literals
from datetime import timedelta, datetime
import functools
import requests
from dzAlerts.util.env import startup
from dzAlerts.util.sql.sql import find_holes
from dzAlerts.util.struct import nvl
from dzAlerts.util.env.logs import Log
from dzAlerts.util.maths import Math
from dzAlerts.util.queries import Q
from dzAlerts.util.times.timer import Timer
from dzAlerts.util.sql.db import DB
from dzAlerts.util.cnv import CNV
from dzAlerts.util.thread.multithread import Multithread
from dzAlerts.util.thread.threads import Lock


db_lock = Lock("db lock")


def etl(name, db, settings, id):
    try:
        with Timer(str(name) + " read from prod " + str(id)):
            content = requests.get(settings.source.service.blob_url + "/" + str(id), timeout=30).content
            if content.startswith("Id not found:"):
                Log.note("Id not found: {{id}}", {"id": id})
                return False
            data = CNV.JSON2object(content)

        if data.test_run_id == None and data.date_loaded > CNV.datetime2unix(datetime.utcnow() - timedelta(weeks=1)):
            # WE WILL IGNORE RECORDS THAT ARE YOUNG AND HAVE NO test_run_id
            Log.note("Id has not test_run_id: {{id}}", {"id": id})
            return True  # LIE, TO ALLOW PROCESSING TO CONTINUE WITH GOOD RECORDS

        with Timer(str(name) + " push to local " + str(id)):
            with db_lock:
                db.insert("objectstore", {
                    "id": id,
                    "test_run_id": data.test_run_id,
                    "date_loaded": data.date_loaded,
                    "revision": data.json_blob.test_build.revision,
                    "branch": data.json_blob.test_build.branch,
                    "json_blob": CNV.object2JSON(data.json_blob),
                })
                db.flush()
                return True
    except Exception, e:
        Log.warning("Can not load {{id}}", {"id": id}, e)
        return False


def replicate_table(table_name, id_name, source, destination):
    """
     COPY TABLE FROM ONE SCHEMA TO ANOTHER.  MUST HAVE AN id COLUMN THAT IS STRICTLY INCREASING
     SO ONLY A DIFF IS REQUIRED TO KEEP TABLES IN SYNCH
    """

    BATCH_SIZE = 1000

    max_id = destination.query("SELECT max({{id}}) `max` FROM {{table}}", {
        "table": destination.quote_column(table_name),
        "id": destination.quote_column(id_name)
    })[0].max

    if max_id == None:
        max_id = -1

    while True:
        missing = source.query("SELECT * FROM {{table}} WHERE {{id}}>{{max}} ORDER BY {{id}} LIMIT {{limit}}", {
            "table": destination.quote_column(table_name),
            "id": destination.quote_column(id_name),
            "max": max_id,
            "limit": BATCH_SIZE
        })

        if not missing:
            return
        max_id = MAX(*Q.select(missing, id_name))
        destination.insert_list(table_name, missing)
        destination.flush()


def copy_pushlog(settings):
    with DB(settings.destination.pushlog) as local:
        with DB(settings.source.pushlog) as prod:
            try:
                replicate_table("branch_map", "id", prod, local)
                replicate_table("branches", "id", prod, local)
                replicate_table("pushlogs", "id", prod, local)
                replicate_table("changesets", "id", prod, local)
            except Exception, e:
                Log.error("Failure during update of pushlog", e)


def copy_objectstore(settings):
    with DB(settings.destination.objectstore) as db:
        try:
            functions = [functools.partial(etl, *["ETL" + str(t), db, settings]) for t in range(settings.source.service.threads)]

            if settings.source.service.max - settings.source.service.min <= 100:
                #FOR SMALL NUMBERS, JUST LOAD THEM ALL AGAIN
                missing_ids = set(range(settings.source.service.min, settings.source.service.max))
            else:
                holes = find_holes(
                    db,
                    table_name="objectstore",
                    column_name="id",
                    filter={"script": "1=1"},
                    _range=settings.source.service
                )

                missing_ids = set()
                for hole in holes:
                    missing_ids = missing_ids.union(set(range(hole.min, hole.max)))

            Log.note("{{num}} missing ids", {"num": len(missing_ids)})
            settings.num_not_found = 0

            with Multithread(functions) as many:
                for result in many.execute([{"id": id} for id in missing_ids]):
                    if not result:
                        settings.num_not_found += 1
                        if settings.num_not_found > 100:
                            many.stop()
                            break
                    else:
                        settings.num_not_found = 0

        except (KeyboardInterrupt, SystemExit):
            Log.note("Shutdow Started, please be patient")
        except Exception, e:
            Log.error("Unusual shutdown!", e)
        Log.note("Done")


if __name__ == "__main__":
    try:
        settings = startup.read_settings()
        settings.source.service.threads = nvl(settings.source.service.threads, 1)
        Log.start(settings.debug)
        copy_objectstore(settings)
        copy_pushlog(settings)
    except Exception, e:
        Log.error("Problem", e)
    finally:
        Log.stop()
