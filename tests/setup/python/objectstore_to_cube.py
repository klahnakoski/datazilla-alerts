
from util.cnv import CNV
from util.db import SQL, DB
from util.debug import D
from util.startup import startup
from util.stats import z_moment2stats, Z_moment
from util.timer import Timer


BATCH_SIZE=100  #SMALL, SO IT DOES NOT LOCK UP DB FOR LONG 


def objectstore_to_cube(db, r):
    try:
        json=CNV.JSON2object(r.json_blob)

        if len(json.results)==0:
            db.insert("test_data_all_dimensions", {
                "test_run_id":r.test_run_id,
                "product_id":0,
                "operating_system_id":0,
                "test_id":0,
                "page_id":0,
                "date_received":json.testrun.date,
                "revision":json.test_build.revision,

                "product":json.test_build.name,
                "branch":json.test_build.branch,
                "branch_version":json.test_build.version,
                "operating_system_name":json.test_machine.os,
                "operating_system_version":json.test_machine.osversion,
                "processor":json.test_machine.platform,

                "build_type":r.build_type,
                "machine_name":json.test_machine.name,
                "pushlog_id":r.pushlog_id,
                "push_date":r.push_date,
                "test_name":json.testrun.suite,
                "page_url":None,
                "mean":None,
                "std":None,
                "n_replicates":None
            })
            return


        for p,m in json.results.items():
            S=z_moment2stats(Z_moment.new_instance(m[5:]))
            db.insert("test_data_all_dimensions", {
                "test_run_id":r.test_run_id,
                "product_id":0,
                "operating_system_id":0,
                "test_id":0,
                "page_id":0,
                "date_received":json.testrun.date,
                "revision":json.test_build.revision,

                "product":json.test_build.name,
                "branch":json.test_build.branch,
                "branch_version":json.test_build.version,
                "operating_system_name":json.test_machine.os,
                "operating_system_version":json.test_machine.osversion,
                "processor":json.test_machine.platform,

                "build_type":r.build_type,
                "machine_name":json.test_machine.name,
                "pushlog_id":r.pushlog_id,
                "push_date":r.push_date,
                "test_name":json.testrun.suite,
                "page_url":p,
                "mean":S.mean,
                "std":S.std,
                "n_replicates":S.count
            })
    except Exception, e:
        D.error("Conversion failed", e)

        
def main_loop(db, settings):

    while True:

        with Timer("Process objectstore") as t:
            ## GET EVERYTHING MISSING FROM tdad (AND JOIN IN PUSHLOG)
            num=db.foreach("""
                SELECT STRAIGHT_JOIN
                    o.test_run_id `test_run_id`,
                    CASE WHEN instr(bm.alt_name, "Non-PGO") THEN "non" ELSE "opt" END build_type,
                    pl.id `pushlog_id`,
                    pl.date `push_date`,
                    json_blob
                FROM
                    ${objectstore}.objectstore o
                LEFT JOIN
                    ${perftest}.test_data_all_dimensions AS tdad ON tdad.test_run_id=o.test_run_id
                LEFT JOIN
                    ${pushlog}.changesets AS ch ON ch.revision=o.revision AND ch.branch=o.branch
                LEFT JOIN
                    ${pushlog}.pushlogs AS pl ON pl.id = ch.pushlog_id
                LEFT JOIN
                    ${pushlog}.branches AS br ON pl.branch_id = br.id
                LEFT JOIN
                    ${pushlog}.branch_map AS bm ON br.name = bm.name
                WHERE
                    o.test_run_id IS NOT NULL AND
                    tdad.test_run_id IS NULL
                LIMIT
                    ${limit}
                """, {
                    "objectstore":SQL(settings.objectstore.schema),
                    "perftest":SQL(settings.database.schema),
                    "pushlog":SQL(settings.pushlog.schema),
                    "limit":BATCH_SIZE
                },
                execute=lambda x: objectstore_to_cube(db, x)
            )
            db.flush()

        if num==0: return




settings=startup.read_settings()

with DB(settings.database) as db:
#    db.debug=settings.debug is not None
    main_loop(db, settings)





