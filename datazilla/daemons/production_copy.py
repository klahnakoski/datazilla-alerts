from Queue import Full
from math import floor
from multiprocessing import Queue
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

class Prod2Local(threading.Thread):

    ## RETURN TRUE IF LOADED

    def __init__(self, name, db, settings):
        threading.Thread.__init__(self)
        self.name=name
        self.db=db
        self.settings=settings
        self.queue=Queue()
        self.keep_running=True
        self.start()

    def etl(self, blob_id):
        try:
            with Timer(str(self.name)+" read from prod "+str(blob_id)):
                content = requests.get(self.settings.production.blob_url + "/" + str(blob_id)).content
                data=CNV.JSON2object(content)
    #            revision=data.json_blob.test_build.revision


            with Timer(str(self.name)+" push to local "+str(blob_id)):
                with db_lock:
                    self.db.insert("objectstore", {
                        "id":blob_id,
                        "test_run_id":data.test_run_id,
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


    #RETURN TRUE IF GOOD
    def send(self, message):
        try:
            self.queue.put(message, False)
            return True
        except Full, f:
            return False

    def run(self):
        while self.keep_running:
            blob_id=self.queue.get()
            if blob_id=="stop": return
            try:
                success=self.etl(blob_id)
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

        try:
            #MAKE THREADS
            threads=[]
            for t in range(settings.production.threads):
                thread=Prod2Local("ETL"+str(t), db, settings)
                threads.append(thread)

            #FILL QUEUES WITH WORK
            curr=0
            for blob_id in range(settings.production.min, settings.production.max):
                if blob_id in existing_ids: continue
                try:
                    success=threads[curr].send(blob_id)
                    if not success: blob_id-=1  #try again
                    curr=(curr+1)%settings.production.threads
                except Exception, e:
                    D.warning("Problem sending ${id} to thread", {"id":blob_id})

            #SEND STOP, AND WAIT FOR FINISH
            for t in threads:
                t.send("stop")

        except BaseException, e:
            D.println("Shutdow Started, please be patient")
            for t in threads:
                 t.keep_running=False

        for t in threads:
            t.join()




settings=startup.read_settings()
settings.production.threads=nvl(settings.production.threads, 1)



extract_from_datazilla_using_id(settings)

