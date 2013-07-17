################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## Author: Kyle Lahnakoski (kyle@lahnakoski.com)
################################################################################



from math import floor
from multiprocessing import Queue
import threading
import requests
from util.basic import nvl
from util.debug import D
from util.query import Q
from util.startup import startup

from util.timer import Timer
from util.db import DB, SQL
from util.cnv import CNV


file_lock=threading.Lock()
db_lock=threading.Lock()

class Prod2Local(threading.Thread):



    def __init__(self, name, queue, db, settings):
        threading.Thread.__init__(self)
        self.name=name
        self.queue=nvl(queue, Queue())
        self.db=db
        self.settings=settings
        self.keep_running=True
        self.start()

    ## RETURN TRUE IF LOADED
    def etl(self, blob_id):
        try:
            with Timer(str(self.name)+" read from prod "+str(blob_id)):
                content = requests.get(self.settings.production.blob_url + "/" + str(blob_id)).content
                if content.startswith("Id not found:"):
                    D.println("Id not found: "+str(blob_id))
                    with db_lock:
                        self.settings.num_not_found+=1
                    return
                data=CNV.JSON2object(content)

            with Timer(str(self.name)+" push to local "+str(blob_id)):
                with db_lock:
                    self.db.insert("objectstore", {
                        "id":blob_id,
                        "test_run_id":SQL("util_newid()"),
                        "date_loaded":data.date_loaded,
                        "error_flag":"N",
                        "error_msg":None,
                        "json_blob":CNV.object2JSON(data.json_blob),
                        "worker_id":None,
                        "revision":data.json_blob.test_build.revision
                    })
                    self.db.flush()
                    return True
        except Exception, e:
            D.warning("Can not load "+str(blob_id), e)
            return False


    def run(self):
        while self.keep_running:
            blob_id=self.queue.get()
            if blob_id=="stop": return
            try:
                self.etl(blob_id)
                with db_lock:
                    if self.settings.num_not_found>=100:
                        return   #GIVE UP
            except Exception, e:
                D.warning("Can not load data for id ${id}", {"id": blob_id})
#            finally:
#                self.queue.task_done()


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
                b.id IS NULL AND
                a.id BETWEEN ${min} AND ${max}
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
                a.id BETWEEN ${min} AND ${max}
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
        existing_ids = get_existing_ids(db)
        settings.num_not_found=0

        threads=[]
        try:
            #MAKE THREADS
            queue=Queue()
            for t in range(settings.production.threads):
                thread=Prod2Local("ETL"+str(t), queue, db, settings)
                threads.append(thread)

            #FILL QUEUE WITH WORK
            for blob_id in range(settings.production.min, settings.production.max):
                if blob_id in existing_ids: continue
                queue.put(blob_id, False)

            #SEND ENOUGH STOPS
            for t in threads:
                queue.put("stop")

            #WAIT FOR FINISH
            for t in threads:
                t.join()
        except Exception, e:
            D.error("Unusual shutdown!", e)
        except BaseException, f:
            D.println("Shutdow Started, please be patient")
        finally:
            for t in threads:
                t.keep_running=False
            for t in threads:
                t.join()





settings=startup.read_settings()
settings.production.threads=nvl(settings.production.threads, 1)

extract_from_datazilla_using_id(settings)

