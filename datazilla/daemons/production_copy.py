from math import floor
import threading
import requests
from util.basic import nvl
from util.debug import D
from util.query import Q
from util.startup import startup

from util.timer import Timer
from util.db import DB
from util.cnv import CNV
from util.map import Map


file_lock=threading.Lock()
db_lock=threading.Lock()

## RETURN TRUE IF LOADED
def etl(blob_id, db, settings):
    try:
        with Timer("read from prod "+str(blob_id)):
            content = requests.get(settings.production.blob_url + "/" + str(blob_id)).content
            data=CNV.JSON2object(content)
#            revision=data.json_blob.test_build.revision


        with Timer("push to local "+str(blob_id)):
            with db_lock:
                db.insert("objectstore", {
                    "id":blob_id,
                    "test_run_id":data.test_run_id,
                    "date_loaded":data.date_loaded,
                    "error_flag":"N",
                    "error_msg":None,
                    "json_blob":CNV.object2JSON(data.json_blob),
                    "worker_id":None,
                    "revision":data.json_blob.test_build.revision
                })
                db.flush()
                return True
    except Exception, e:
        D.warning("Can not load "+str(blob_id), e)
        return False


def etl_main_loop(db, VAL, existing_ids, settings):
    try:
        for blob_id in range(settings.production.min, settings.production.max):
            if blob_id % settings.production.threads != VAL: continue
            if blob_id in existing_ids: continue
            try:
                success=etl(blob_id, db, settings)
            except Exception, e:
                D.warning("Can not load data for id ${id}", {"id": blob_id})
    finally:
        db.set_refresh_interval(1)


def get_existing_ids(db):
    #FIND WHAT'S MISSING IN LOCAL ALREADY
    ranges = db.query("""
		SELECT
			id,
			`end`
		FROM (
			SELECT STRAIGHT_JOIN
                a.id,
				"max" `end`
            FROM
                objectstore a
            LEFT JOIN
                objectstore b ON b.id=a.id+1
            WHERE
                b.id IS NULL
		UNION ALL
            SELECT
                a.id,
				"min" `end`
            FROM
                objectstore a
            LEFT JOIN
                objectstore c ON c.id=a.id-1
            WHERE
                c.id IS NULL
		) a
		ORDER BY
			id,
			`end` desc
    """)
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
        existing_ids = get_existing_ids(db)

        threads=[]
        for t in range(settings.production.threads):
            thread=threading.Thread(target=etl_main_loop, args=(db, t, existing_ids, settings))
            thread.start()
            threads.append(thread)

        for t in threads:
            t.join()





settings=startup.read_settings()
settings.production.threads=nvl(settings.production.threads, 1)



extract_from_datazilla_using_id(settings)

