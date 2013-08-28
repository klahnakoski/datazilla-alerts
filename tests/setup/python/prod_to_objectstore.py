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
from util.basic import nvl
from util.debug import D
from util.query import Q
from util.startup import startup
from util.timer import Timer
from util.db import DB
from util.cnv import CNV
from util.multithread import Multithread
from util.threads import Lock


db_lock=Lock("db lock")


def etl(name, db, settings, id):
    try:
        with Timer(str(name)+" read from prod "+str(id)):
            content = requests.get(settings.production.blob_url + "/" + str(id), timeout=30).content
            if content.startswith("Id not found:"):
                D.println("Id not found: "+str(id))
                return False
            data=CNV.JSON2object(content)

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
        D.warning("Can not load {{id}}", {"id":id}, e)
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
    """, {"min":settings.production.min, "max":settings.production.max})
    #RESULT COMES IN min/max PAIRS, IN ORDER
    for i, r in enumerate(ranges):
        r.index=int(floor(i/2))
    unstacked_ranges = Q.unstack(ranges, keys=["index"], column='end', value="id")

    existing_ids = set()
    for r in unstacked_ranges:
        existing_ids = existing_ids.union(
            range(r.min, r.max + 1))

    D.println("Number of ids in local: "+str(len(existing_ids)))
    return existing_ids


def extract_from_datazilla_using_id(settings):
    with DB(settings.database) as db:
        try:
            functions=[functools.partial(etl, *["ETL"+str(t), db, settings]) for t in range(settings.production.threads)]

            if settings.production.max-settings.production.min<=100:
                #FOR SMALL NUMBERS, JUST LOAD THEM ALL AGAIN
                missing_ids=set(range(settings.production.min, settings.production.max))
            else:
                existing_ids = get_existing_ids(db)
                missing_ids=set(range(settings.production.min, settings.production.max)) - existing_ids
            settings.num_not_found=0

            with Multithread(functions) as many:
                for result in many.execute([{"id":id} for id in missing_ids]):
                    if not result:
                        settings.num_not_found+=1
                        if settings.num_not_found>100:
                            many.stop()
                            break
#                    else:
#                        settings.num_not_found=0
        except (KeyboardInterrupt, SystemExit):
            D.println("Shutdow Started, please be patient")
        except Exception, e:
            D.error("Unusual shutdown!", e)
        D.println("Done")


settings=startup.read_settings()
settings.production.threads=nvl(settings.production.threads, 1)
D.start(settings.debug)
extract_from_datazilla_using_id(settings)
D.stop()
