################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## Author: Kyle Lahnakoski (kyle@lahnakoski.com)
################################################################################


## I WANT TO REFER TO "scipy.stats" TO BE EXPLICIT
import scipy
from scipy import stats
scipy.stats = stats


from dzAlerts.util import struct
from dzAlerts.util.maths import Math
from dzAlerts.util.queries import windows
from dzAlerts.util.struct import nvl
from dzAlerts.util.db import SQL
from dzAlerts.util.queries import Q


BASE_REASON = "alert_exception"     #name of the reason in alert_reason
WINDOW_SIZE = 100
DEBUG = True


def page_summary(settings, db, new_test_points):
    """
    SUMMARIZE REVISIONS BASED ON PAGE STATS
    new_test_points IS EVERYTHING IN THE tdad TABLE THAT IS NEW (NOT PROCESSED YET)
    """

    debug = nvl(settings.param.debug, DEBUG)

    setup(settings, db)

    platform = {
        "operating_system_name",
        "operating_system_version",
        "processor"
    }
    product = {
        "product",
        "branch",
        "branch_version"
    }
    test = {
        "test_name"
    }

    page = {
        "test_name",
        "page_url"
    }

    # THE EVENTUAL GOAL IS TO PARAMETRIZE THE SQL, WHICH REALLY IS
    # SIMPLE EDGE SETS
    query = struct.wrap({
        "from": "test_data_all_dimensions",
        "select": [
            {"value": "id", "name": "tdad_id"},
            "test_run_id",
            "revision",
            {"value": "n_replicates", "name": "count"},
            "mean",
            "std"
        ]
    })

    for g, points in Q.groupby(new_test_points, test | product):
        min_date = Math.min(Q.select(points, "min_push_date"))

        # FOR THIS g, HOW FAR BACK IN TIME MUST WE GO TO COVER OUR WINDOW_SIZE?
        first_in_window = db.query("""
            SELECT
                min(min_push_date) min_date
            FROM (
                SELECT
                    test_run_id,
                    min(push_date) min_push_date
                FROM
                    test_data_all_dimensions t
                WHERE
                    {{where}}
                GROUP BY
                    test_run_id
                ORDER BY
                    min(push_date) DESC
                LIMIT
                    {{window_size}}
            ) t
        """, {
            "perftest": db.quote_column(settings.perftest.schema),
            "pushlog": db.quote_column(settings.pushlog.schema),
            "edges": db.quote_column(query.edges),
            "where": db.esfilter2sqlwhere({"and": [
                {"term": g},
                {"exists": "n_replicates"},
                {"range": {"push_date": {"lt": min_date}}}
            ]}),
            "window_size": SQL(WINDOW_SIZE + 1)
        })[0]

        # GET THE TEST RESULTS FOR EVERYTHING IN THE WINDOW
        alert_results = db.query("""
            SELECT
                {{select}},
                {{edges}},
                a.status
            FROM
                test_data_all_dimensions t
            LEFT JOIN
                alert_exception a
            ON
                a.tdad_id=t.id AND
                a.reason={{reason}} AND
                a.status <> 'obsolete'
            WHERE
                {{where}}
            GROUP BY
                {{edges}}
            ORDER BY
                push_date
        """, {
            "select": query.select,
            "edges": db.quote_column(page | product),
            "reason": BASE_REASON,
            "where": db.esfilter2sqlwhere({"and": [
                {"term": g},
                {"exists": "t.n_replicates"},
                {"range": {"push_date": {"gte": first_in_window.min_date}}}
            ]})
        })

        windowed = Q.run({
            "from": alert_results,
            "window": [{
                "name": "sample_fail",
                "value": "status",
                "edges": page | product,
                "sort": ["push_date", "test_run_id"],
                "aggregate": windows.Count,
                "range": {"min": -WINDOW_SIZE, "max": 0}
            }, {
                "name": "sample_count",
                "value": "1",
                "edges": page | product,
                "sort": ["push_date", "test_run_id"],
                "aggregate": windows.Count,
                "range": {"min": -WINDOW_SIZE, "max": 0}
            }, {
                "name": "sample_min_date",
                "value": "push_date",
                "edges": page | product,
                "sort": ["push_date", "test_run_id"],
                "aggregate": windows.Min,
                "range": {"min": -WINDOW_SIZE, "max": 0}
            }, {
                "name": "sample_max_date",
                "value": "push_date",
                "edges": page | product,
                "sort": ["push_date", "test_run_id"],
                "aggregate": windows.Max,
                "range": {"min": -WINDOW_SIZE, "max": 0}
            }, {
                "name": "sample_pass",
                "value": lambda (r): r.sample_count - r.sample_fail
            }, {
                # TODO: INCREASE sample_fail TO 70% CONFIDENCE
                # "+2" DOES THE JOB OF AVOIDING ZEROS FOR NOW
                "name": "failure_probability",
                "value": lambda (r): (r.sample_fail + 2) / r.sample_count
            }]
        })

        #KEEP ONLY THE COLUMNS WE NEED
        clean_list = Q.run({
            "from": windowed,
            "select":
                page |
                product | {
                "revision",
                "sample_fail",
                "sample_count",
                "sample_min_date",
                "sample_max_date",
                "sample_pass",
                "failure_probability"
            },
            "where": {"term": {"sample_count": WINDOW_SIZE}}
        })

        for g, d in Q.groupby(clean_list, page | product | {"revision"}):
            db.execute("DELETE FROM alert_page_summary WHERE {{where}}", {
                "where": db.esfilter2sqlwhere({"term": g})
            })

        db.insert("alert_page_summary", page | product | {"revision"}, clean_list)



def setup(settings, db):

    #TEST IF SETUP HAS BEEN RUN ALREADY
    try:
        db.query("""SELECT count(1) FROM alert_page_summary""")
        return
    except Exception, e:
        pass

    db.execute("""
        CREATE TABLE alert_page_summary (
            revision        VARCHAR(16) COLLATE utf8_bin DEFAULT NULL,
            test_name       VARCHAR(128) COLLATE utf8_bin NOT NULL,
            page_url        VARCHAR(255) COLLATE utf8_bin NOT NULL,
            sample_min_date INT(11) unsigned NOT NULL,
            sample_max_date INT(11) unsigned NOT NULL,
            sample_count    INTEGER NOT NULL,
            sample_pass     INTEGER NOT NULL,
            sample_fail     INTEGER NOT NULL
        )
    """)



