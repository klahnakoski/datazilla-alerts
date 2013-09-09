################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## Author: Kyle Lahnakoski (kyle@lahnakoski.com)
################################################################################


from dzAlerts.util.cnv import CNV
from dzAlerts.util.db import SQL, DB
from dzAlerts.util.debug import D
from dzAlerts.util.startup import startup
from dzAlerts.util.stats import z_moment2stats, Z_moment
from dzAlerts.util.timer import Timer
from dzAlerts.util.query import Q


BATCH_SIZE=1000  #SMALL, SO IT DOES NOT LOCK UP DB FOR LONG
TEST_RESULTS_PER_RUN=10000


def objectstore_to_cube(db, r):
    try:
        json=CNV.JSON2object(r.json_blob)

        if len(json.results.keys())==0:
            #DUMMY RECORD SO FUTURE QUERIES KNOW THIS objectstore HAS BEEN PROCESSED
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

        
def main(settings):

    with DB(settings.destination.objectstore, settings.destination.perftest.schema) as db:
        with DB(settings.destination.objectstore, settings.destination.perftest.schema) as write_db:
            missing_ids=get_missing_ids(db, settings)

            for group, values in Q.groupby(missing_ids, size=BATCH_SIZE):
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
                            {{objectstore}}.objectstore o
                        LEFT JOIN
                            {{pushlog}}.changesets AS ch ON ch.revision=o.revision
                        LEFT JOIN
                            {{pushlog}}.pushlogs AS pl ON pl.id = ch.pushlog_id
                        LEFT JOIN
                            {{pushlog}}.branches AS br ON pl.branch_id = br.id
                        LEFT JOIN
                            {{pushlog}}.branch_map AS bm ON br.name = bm.name
                        WHERE
                            (bm.alt_name=o.branch OR br.name=o.branch) AND
                            o.test_run_id IN {{values}}
                        """, {
                            "objectstore":SQL(settings.destination.objectstore.schema),
                            "perftest":SQL(settings.destination.perftest.schema),
                            "pushlog":SQL(settings.destination.pushlog.schema),
                            "values":values
                        },
                        execute=lambda x: objectstore_to_cube(write_db, x)
                    )
                    db.flush()



def get_missing_ids(db, settings):

    missing=db.query("""
        SELECT STRAIGHT_JOIN
            o.test_run_id
        FROM
            {{objectstore}}.objectstore o
        LEFT JOIN
            {{perftest}}.test_data_all_dimensions AS tdad
        ON
            tdad.test_run_id=o.test_run_id AND
            tdad.revision=o.revision AND
            tdad.branch=o.branch
        LEFT JOIN
            {{pushlog}}.changesets AS ch ON ch.revision=o.revision
        LEFT JOIN
            {{pushlog}}.pushlogs AS pl ON pl.id = ch.pushlog_id
        LEFT JOIN
            {{pushlog}}.branches AS br ON pl.branch_id = br.id
        LEFT JOIN
            {{pushlog}}.branch_map AS bm ON br.name = bm.name
        WHERE
            o.test_run_id IS NOT NULL AND
            tdad.test_run_id IS NULL AND
            (bm.alt_name=o.branch OR br.name=o.branch)
        ORDER BY
            o.test_run_id DESC
        LIMIT
            {{limit}}
        """, {
            "objectstore":SQL(settings.destination.objectstore.schema),
            "perftest":SQL(settings.destination.perftest.schema),
            "pushlog":SQL(settings.destination.pushlog.schema),
            "limit":TEST_RESULTS_PER_RUN
        })

    missing_ids=Q.select(missing, field_name="test_run_id")
    D.println("{{num}} objectstore records to be processed into cube", {"num":len(missing_ids)})
    return missing_ids


try:
    settings=startup.read_settings()
    D.start(settings.debug)
    main(settings)
except Exception, e:
    D.error("Problem", e)
finally:
    D.stop()




