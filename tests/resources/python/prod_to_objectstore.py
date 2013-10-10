
################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## Author: Kyle Lahnakoski (kyle@lahnakoski.com)
################################################################################
import functools
from math import floor
import requests
from dzAlerts.util.basic import nvl
from dzAlerts.util.logs import Log
from dzAlerts.util.maths import Math
from dzAlerts.util.query import Q
from dzAlerts.util.startup import startup
from dzAlerts.util.struct import Null
from dzAlerts.util.timer import Timer
from dzAlerts.util.db import DB, SQL
from dzAlerts.util.cnv import CNV
from dzAlerts.util.multithread import Multithread
from dzAlerts.util.threads import Lock


db_lock=Lock("db lock")


def etl(name, db, settings, id):
    try:
        with Timer(str(name)+" read from prod "+str(id)):
            content = requests.get(settings.source.service.blob_url + "/" + str(id), timeout=30).content
            if content.startswith("Id not found:"):
                Log.note("Id not found: {{id}}", {"id":id})
                return False
            data=CNV.JSON2object(content)

        if data.test_run_id == Null:
            #DZ IS RESPONSIBLE FOR THIS NUMBER, WHICH MAY NOT BE SET YET
            return False

        with Timer(str(name)+" push to local "+str(id)):
            with db_lock:
                db.insert("objectstore", {
                    "id":id,
                    "test_run_id":data.test_run_id,
                    "date_loaded":data.date_loaded,
                    "revision":data.json_blob.test_build.revision,
                    "branch":data.json_blob.test_build.branch,
                    "json_blob":CNV.object2JSON(data.json_blob),
                })
                db.flush()
                return True
    except Exception, e:
        Log.warning("Can not load {{id}}", {"id":id}, e)
        return False




def get_existing_ids(db):
    #FIND WHAT'S MISSING IN LOCAL ALREADY
    ranges = db.query("""
        SELECT DISTINCT
                id,
                `end`
        FROM (
            SELECT STRAIGHT_JOIN
                a.id,
                "min" `end`
            FROM
                objectstore a
            WHERE
                a.id={{min}}
        UNION ALL
            SELECT
                a.id,
                "min" `end`
            FROM
                objectstore a
            LEFT JOIN
                objectstore c ON c.id=a.id-1
            WHERE
                c.id IS NULL AND
                a.id BETWEEN {{min}} AND {{max}}
        UNION ALL
            SELECT STRAIGHT_JOIN
                a.id,
                "max" `end`
            FROM
                objectstore a
            LEFT JOIN
                objectstore b ON b.id=a.id+1
            WHERE
                b.id IS NULL AND
                a.id BETWEEN {{min}} AND {{max}}
        UNION ALL
            SELECT STRAIGHT_JOIN
                a.id,
                "max" `end`
            FROM
                objectstore a
            WHERE
                a.id={{max}}
        ) a
        ORDER BY
            id,
            `end` desc
    """, {"min":settings.source.service.min, "max":settings.source.service.max})
    #RESULT COMES IN min/max PAIRS, IN ORDER
    for i, r in enumerate(ranges):
        r.index=int(floor(i/2))
    unstacked_ranges = Q.unstack(ranges, keys=["index"], column='end', value="id")

    existing_ids = set()
    for r in unstacked_ranges:
        existing_ids = existing_ids.union(
            range(r.min, r.max + 1))

    Log.note("Number of ids in local: "+str(len(existing_ids)))
    return existing_ids


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

    if max_id == Null: max_id=-1

    while True:
        missing = source.query("SELECT * FROM {{table}} WHERE {{id}}>{{max}} ORDER BY {{id}} LIMIT {{limit}}", {
            "table": destination.quote_column(table_name),
            "id": destination.quote_column(id_name),
            "max": max_id,
            "limit": BATCH_SIZE
        })

        if len(missing) == 0:
            return
        max_id = Math.max(Q.select(missing, id_name))
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
            functions=[functools.partial(etl, *["ETL"+str(t), db, settings]) for t in range(settings.source.service.threads)]

            if settings.source.service.max-settings.source.service.min<=100:
                #FOR SMALL NUMBERS, JUST LOAD THEM ALL AGAIN
                missing_ids=set(range(settings.source.service.min, settings.source.service.max))
            else:
                existing_ids = get_existing_ids(db)
                missing_ids=Q.sort(set(range(settings.source.service.min, settings.source.service.max)) - existing_ids)
            settings.num_not_found=0

            with Multithread(functions) as many:
                for result in many.execute([{"id":id} for id in missing_ids]):
                    if not result:
                        settings.num_not_found+=1
                        if settings.num_not_found>100:
                            many.stop()
                            break
                    else:
                        settings.num_not_found=0

        except (KeyboardInterrupt, SystemExit):
            Log.note("Shutdow Started, please be patient")
        except Exception, e:
            Log.error("Unusual shutdown!", e)
        Log.note("Done")


if __name__=="__main__":
    try:
        settings=startup.read_settings()
        settings.source.service.threads=nvl(settings.source.service.threads, 1)
        Log.start(settings.debug)
        copy_objectstore(settings)
        copy_pushlog(settings)
    except Exception, e:
        Log.error("Problem", e)
    finally:
        Log.stop()
