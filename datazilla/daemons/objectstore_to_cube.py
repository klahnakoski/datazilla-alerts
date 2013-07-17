from util.db import SQL, DB
from util.startup import startup
from util.timer import Timer

BATCH_SIZE=100  #SMALL, SO IT DOES NOT LOCK UP DB FOR LONG 


def etl(db, settings, batch_size):

    db.execute("""
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

                json_n(json(json_blob, "testrun"), "date") date_received,

                json_s(json(json_blob, "test_build"), "revision") revision,
                json_s(json(json_blob, "test_build"), "name") product,
                json_s(json(json_blob, "test_build"), "branch") branch,
                json_s(json(json_blob, "test_build"), "version") branch_version,
                json_s(json(json_blob, "test_machine"), "os") operating_system_name,
                json_s(json(json_blob, "test_machine"), "osversion") operating_system_version,
                json_s(json(json_blob, "test_machine"), "platform") processor,
                CASE WHEN instr(bm.alt_name, "Non-PGO") THEN "non" ELSE "opt" END build_type,
                json_s(json(json_blob, "test_machine"), "name") machine_name,
                pl.id `pushlog_id`,
                pl.date `push_date`,
                json_s(json(json_blob, "testrun"), "suite") test_name,
                ## PREPROCESSING FOR NEXT RUN
                string_word_count(json(json_blob, "results"), "],") num_results,
                json(json_blob, "results") results
            FROM
                ${objectstore}.objectstore o
            LEFt JOIN
                ${perftest}.test_data_all_dimensions AS tdad ON tdad.test_run_id=o.test_run_id
            LEFT JOIN
                ${pushlog}.changesets AS ch ON ch.revision=json_s(json(o.json_blob, "test_build"), "revision")
            LEFT JOIN
                ${pushlog}.pushlogs AS pl ON pl.id = ch.pushlog_id
            LEFT JOIN
                ${pushlog}.branches AS br ON pl.branch_id = br.id
            LEFT JOIN
                ${pushlog}.branch_map AS bm ON br.name = bm.name
            WHERE
                o.test_run_id IS NOT NULL AND
                tdad.test_run_id IS NULL AND
                left(json_s(json(json_blob, "testrun"), "suite"), 3)="tp5"
            LIMIT
                ${limit}
            ) a
        JOIN
            util_digits d1 ON (d1.digit+1)*10<a.num_results
        JOIN
            util_digits d2 ON 1=1
        WHERE
            d1.digit*10+d2.digit<a.num_results
    """, {
        "objectstore":SQL(settings.objectstore.schema),
        "perftest":SQL(settings.database.schema),
        "pushlog":SQL(settings.pushlog.schema),
        "limit":batch_size
    })

    db.flush()



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
            tdad.test_run_id IS NULL 
    """, {
        "objectstore":SQL(settings.objectstore.schema),
        "perftest":SQL(settings.database.schema)
    })[0].num

    #UPDATE IN BATCHES
    for i in range(0, num+BATCH_SIZE, BATCH_SIZE):
        with Timer("update test_data_all_dimensions ("+str(BATCH_SIZE)+")"):
            etl(db, settings, BATCH_SIZE)



settings=startup.read_settings()

with DB(settings.database) as db:
    db.debug=settings.debug is not None
    main_loop(db, settings)





