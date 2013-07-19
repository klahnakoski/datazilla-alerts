import string
import sys
from util import strings
from util.cnv import CNV
from util.db import SQL, DB
from util.debug import D
from util.startup import startup
from util.stats import z_moment2stats, Z_moment
from util.timer import Timer

BATCH_SIZE=100  #SMALL, SO IT DOES NOT LOCK UP DB FOR LONG 



# I AM VERY SORRY I DID THIS
def macro_expand_fast(sql):
    while True:
        params=strings.between(sql, "JSON(", ")")
        if params is None: return sql
        call="JSON("+params+")"
        params=params.split(",")
        new_call="json(substring("+params[0]+", 0, 6000), "+params[1]+")"
        sql=sql.replace(call, new_call)


def macro_expand_slow(sql):
    def quote(val):
        return "\""+val.replace("\\", "\\\\").replace("\"", "\\\"")+"\""

    # json(json_blob, "test_build")

    while True:
        params=strings.between(sql, "JSON(", ")")
        if params is None: return sql
        call="JSON("+params+")"
        params=params.split(",")
        new_call="json(substring("+params[0]+", locate(concat("+quote("\"")+","+params[1]+","+quote("\":")+"), "+params[0]+"), 65000), "+params[1]+")"
        sql=sql.replace(call, new_call)


#
#SELECT STRAIGHT_JOIN
#    o.test_run_id `test_run_id`,
#	CASE WHEN instr(bm.alt_name, "Non-PGO") THEN "non" ELSE "opt" END build_type,
#	pl.id `pushlog_id`,
#	pl.date `push_date`,
#    json_blob
#FROM
#    ${objectstore}.objectstore o
#LEFT JOIN
#    ${perftest}.test_data_all_dimensions AS tdad ON tdad.test_run_id=o.test_run_id
#LEFT JOIN
#    ${pushlog}.changesets AS ch ON ch.revision=o.revision
#LEFT JOIN
#    ${pushlog}.pushlogs AS pl ON pl.id = ch.pushlog_id
#LEFT JOIN
#    ${pushlog}.branches AS br ON pl.branch_id = br.id
#LEFT JOIN
#    ${pushlog}.branch_map AS bm ON br.name = bm.name
#WHERE
#    o.test_run_id IS NOT NULL AND
#    tdad.test_run_id IS NULL





def etl(db, settings, batch_size):

    db.execute(macro_expand_fast("""
        INSERT INTO ${perftest}.test_data_all_dimensions (
            `test_run_id`,
            `product_id`,
            `operating_system_id`,
            `test_id`,
            `page_id`,
            `date_received`,
            `revision`,
            `product`,
            `branch`,
            `branch_version`,
            `operating_system_name`,
            `operating_system_version`,
            `processor`,
            `build_type`,
            `machine_name`,
            `pushlog_id`,
            `push_date`,
            `test_name`,
            `page_url`,
            `mean`,
            `std`,
            `n_replicates`
        )
        SELECT STRAIGHT_JOIN
            a.test_run_id,
            0 `product_id`,
            0 `operating_system_id`,
            0 `test_id`,
            0 `page_id`,
            a.date_received,
            a.revision,
            a.product,
            a.branch,
            a.branch_version,
            a.operating_system_name,
            a.operating_system_version,
            a.processor,
            a.build_type,
            a.machine_name,
            a.`pushlog_id`,
            a.`push_date`,
            a.test_name,
            string_between(                                string_get_word(results, "]", d1.digit*10+d2.digit), "\\"", "\\"", 1) page_url,
                  json_an(math_stats(json_substring(concat(string_get_word(results, "]", d1.digit*10+d2.digit), "]"),5,100)), 1) mean,
            round(json_an(math_stats(json_substring(concat(string_get_word(results, "]", d1.digit*10+d2.digit), "]"),5,100)), 2), 2) std,
                  json_an(math_stats(json_substring(concat(string_get_word(results, "]", d1.digit*10+d2.digit), "]"),5,100)), 0) `n_replicates`
        FROM ( #RECORDS FOR THE BATTERY OF TESTS
            SELECT STRAIGHT_JOIN
                o.test_run_id `test_run_id`,
            -- 	b.product_id `product_id`,
            -- 	o.id `operating_system_id`,
            -- 	tr.test_id `test_id`,
            -- 	tpm.page_id `page_id`,

                json_n(JSON(json_blob, "testrun"), "date") date_received,

                json_s(JSON(json_blob, "test_build"), "revision") revision,
                json_s(JSON(json_blob, "test_build"), "name") product,
                json_s(JSON(json_blob, "test_build"), "branch") branch,
                json_s(JSON(json_blob, "test_build"), "version") branch_version,
                json_s(JSON(json_blob, "test_machine"), "os") operating_system_name,
                json_s(JSON(json_blob, "test_machine"), "osversion") operating_system_version,
                json_s(JSON(json_blob, "test_machine"), "platform") processor,
                CASE WHEN instr(bm.alt_name, "Non-PGO") THEN "non" ELSE "opt" END build_type,
                json_s(JSON(json_blob, "test_machine"), "name") machine_name,
                pl.id `pushlog_id`,
                pl.date `push_date`,
                json_s(JSON(json_blob, "testrun"), "suite") test_name,
                ## PREPROCESSING FOR NEXT RUN
                string_word_count(JSON(json_blob, "results"), "],") num_results,
                JSON(json_blob, "results") results
            FROM
                ${objectstore}.objectstore o
            LEFT JOIN
                ${perftest}.test_data_all_dimensions AS tdad ON tdad.test_run_id=o.test_run_id
            LEFT JOIN
                ${pushlog}.changesets AS ch ON ch.revision=o.revision
            LEFT JOIN
                ${pushlog}.pushlogs AS pl ON pl.id = ch.pushlog_id
            LEFT JOIN
                ${pushlog}.branches AS br ON pl.branch_id = br.id
            LEFT JOIN
                ${pushlog}.branch_map AS bm ON br.name = bm.name
            WHERE
                o.test_run_id IS NOT NULL AND
                tdad.test_run_id IS NULL AND
                #instr(json_blob, "\\"suite\\": \\"tp5")>0 #AND
                left(json_s(JSON(json_blob, "testrun"), "suite"), 3)="tp5"
            LIMIT
                ${limit}
            ) a
        JOIN
            util_digits d1 ON 1=1
        JOIN
            util_digits d2 ON 1=1
        WHERE
            d1.digit*10+d2.digit<a.num_results
    """), {
        "objectstore":SQL(settings.objectstore.schema),
        "perftest":SQL(settings.database.schema),
        "pushlog":SQL(settings.pushlog.schema),
        "limit":batch_size
    })

    db.flush()




def objectstore_to_cube(db, r):
    try:
        json=CNV.JSON2object(r.json_blob)
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
        db.flush()
    except Exception, e:
        D.error("Conversion failed", e)

        
def main_loop2(db, settings):

    ## GET EVERYTHING MISSING FROM tdad (AND JOIN IN PUSHLOG)
    db.foreach("""
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
            ${pushlog}.changesets AS ch ON ch.revision=o.revision
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






def main_loop(db, settings):
    #ENSURE objectstore HAS test_run_ids SET
    db.execute("""
        UPDATE ${objectstore}.objectstore
        SET test_run_id=${perftest}.util_newid()
        WHERE test_run_id IS NULL
        """, {
        "objectstore":SQL(settings.objectstore.schema),
        "perftest":SQL(settings.database.schema)
    })


    #GET SIZE OF JOB
    num=db.query("""
        SELECT
            count(1) num
        FROM
            ${objectstore}.objectstore o
        LEFt JOIN
            ${perftest}.test_data_all_dimensions AS tdad ON tdad.test_run_id=o.test_run_id
        WHERE
            o.test_run_id IS NOT NULL AND
            tdad.test_run_id IS NULL #AND
            #left(ltrim(json(o.json_blob, "results")),2)<>"{}"  #DO NOT ADD EMPTY RESULTS
    """, {
        "objectstore":SQL(settings.objectstore.schema),
        "perftest":SQL(settings.database.schema)
    })[0].num

    D.println("Number of objectstore records to process: ${num_records}", {"num_records":num})


    #UPDATE IN BATCHES
    for i in range(0, num+BATCH_SIZE, BATCH_SIZE):
        with Timer("update test_data_all_dimensions ("+str(BATCH_SIZE)+")"):
            etl(db, settings, BATCH_SIZE)



settings=startup.read_settings()

D.println(CNV.object2JSON(sys.path))

with DB(settings.database) as db:
    db.debug=settings.debug is not None
    main_loop2(db, settings)





