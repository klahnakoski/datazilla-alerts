# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import unicode_literals
from dzAlerts.util.cnv import CNV
from dzAlerts.util.db import SQL, DB
from dzAlerts.util.logs import Log
from dzAlerts.util import startup
from dzAlerts.util.stats import z_moment2stats, Z_moment, median
from dzAlerts.util.struct import nvl
from dzAlerts.util.timer import Timer
from dzAlerts.util.queries import Q


BATCH_SIZE = 1000  #SMALL, SO IT DOES NOT LOCK UP DB FOR LONG
TEST_RESULTS_PER_RUN = 10000


def objectstore_to_cube(r):
    try:
        json = CNV.JSON2object(r.json_blob)

        if len(json.results.keys()) == 0:
            #DUMMY RECORD SO FUTURE QUERIES KNOW THIS objectstore HAS BEEN PROCESSED
            return [{
                "test_run_id": r.test_run_id,
                "product_id": 0,
                "operating_system_id": 0,
                "test_id": 0,
                "page_id": 0,
                "date_received": json.testrun.date,
                "revision": json.test_build.revision,

                "product": json.test_build.name,
                "branch": json.test_build.branch,
                "branch_version": json.test_build.version,
                "operating_system_name": json.test_machine.os,
                "operating_system_version": json.test_machine.osversion,
                "processor": json.test_machine.platform,

                "build_type": r.build_type,
                "machine_name": json.test_machine.name,
                "pushlog_id": r.pushlog_id,
                "push_date": nvl(r.push_date, json.testrun.date),
                "test_name": json.testrun.suite[6:] if json.testrun.suite.startswith("Talos ") else json.testrun.suite,
                "page_url": None,
                "mean": None,
                "std": None,
                "n_replicates": None
            }]

        output = []
        for p, m in json.results.items():
            S = z_moment2stats(Z_moment.new_instance(m[5:]))
            output.append({
                "test_run_id": r.test_run_id,
                "product_id": 0,
                "operating_system_id": 0,
                "test_id": 0,
                "page_id": 0,
                "date_received": json.testrun.date,
                "revision": json.test_build.revision,

                "product": json.test_build.name,
                "branch": json.test_build.branch,
                "branch_version": json.test_build.version,
                "operating_system_name": json.test_machine.os,
                "operating_system_version": json.test_machine.osversion,
                "processor": json.test_machine.platform,

                "build_type": r.build_type,
                "machine_name": json.test_machine.name,
                "pushlog_id": nvl(r.pushlog_id, 0),
                "push_date": nvl(r.push_date, json.testrun.date),
                "test_name": json.testrun.suite[6:] if json.testrun.suite.startswith("Talos ") else json.testrun.suite,
                "page_url": p[:255],
                "mean": median(m),
                "std": S.std,
                "n_replicates": S.count
            })
        return output
    except Exception, e:
        Log.error("Conversion failed", e)


def get_missing_ids(db, settings):
    missing = db.query("""
        SELECT
            o.test_run_id
        FROM
            ekyle_objectstore_1.objectstore o
        WHERE
            o.processed_cube = 'ready' AND
            o.test_run_id IS NOT NULL
        LIMIT
            {{limit}}
        """, {
        "objectstore": SQL(settings.destination.objectstore.schema),
        "limit": TEST_RESULTS_PER_RUN
    })

    missing_ids = Q.select(missing, field_name="test_run_id")
    Log.note("{{num}} objectstore records to be processed into cube", {"num": len(missing_ids)})
    return missing_ids


def main(settings):
    with DB(settings.destination.objectstore, settings.destination.perftest.schema) as db:
        missing_ids = get_missing_ids(db, settings)

        with DB(settings.destination.objectstore, settings.destination.perftest.schema) as write_db:
            if missing_ids:
                write_db.execute(
                    "DELETE FROM test_data_all_dimensions WHERE {{where}}", {
                        "where": db.esfilter2sqlwhere({"terms": {"test_run_id": missing_ids}})
                    })

            for group, values in Q.groupby(missing_ids, size=BATCH_SIZE):
                values = set(values)

                with Timer("Process {{num}} objectstore", {"num": len(values)}) as t:
                    ## GET EVERYTHING MISSING FROM tdad (AND JOIN IN PUSHLOG)
                    blobs = db.query("""
                        SELECT
                            o.test_run_id `test_run_id`,
                            'non' build_type,
                            json_blob
                        FROM
                            {{objectstore}}.objectstore o
                        WHERE
                            {{where}}
                        """, {
                        "objectstore": SQL(settings.destination.objectstore.schema),
                        "where": db.esfilter2sqlwhere({"and": [
                            {"exists": "o.test_run_id"},
                            {"terms": {"o.test_run_id": values}}
                        ]})
                    })

                    tdads = []
                    for b in blobs:
                        tdads.extend(objectstore_to_cube(b))
                    write_db.insert_list("test_data_all_dimensions", tdads)

                    #MARK WE ARE DONE HERE
                    db.execute("""
                        UPDATE {{objectstore}}.objectstore o
                        SET o.processed_cube = 'done'
                        WHERE {{where}}
                    """, {
                        "objectstore": SQL(settings.destination.objectstore.schema),
                        "values": values,
                        "where": db.esfilter2sqlwhere({"terms": {"test_run_id": values}})
                    })
                    db.flush()


try:
    settings = startup.read_settings()
    Log.start(settings.debug)
    main(settings)
except Exception, e:
    Log.error("Problem", e)
finally:
    Log.stop()
