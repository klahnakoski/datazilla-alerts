
################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## Author: Kyle Lahnakoski (kyle@lahnakoski.com)
################################################################################
from datetime import datetime, timedelta
from math import sqrt
import pytest
from dzAlerts.daemons.alert_exception import alert_exception, REASON, MIN_CONFIDENCE

from dzAlerts.util.cnv import CNV
from dzAlerts.util.db import SQL, DB
from dzAlerts.util.debug import D
from dzAlerts.util.startup import startup
from dzAlerts.util.struct import Struct
from dzAlerts.util.query import Q
from dzAlerts.util.stats import closeEnough
from util.testing import make_test_database


EXPECTED_SEVERITY=0.9

class test_alert_exception():

    def __init__(self, db):
        self.db=db
        self.db.debug=True
        self.url="mozilla.com"


    def test_alert_generated(self, test_data):
        self._setup(test_data)

        alert_exception (
            db=self.db,
            debug=True
        )

        ## VERIFY AN ALERT IS GENERATED
        alert=self.db.query("""
            SELECT
                id,
                status,
                create_time,
                tdad_id,
                reason,
                details,
                severity,
                confidence
            FROM
                alerts
            WHERE
                reason={{reason}}
            """, {
                "reason":REASON
        })

        assert len(alert)==1
        assert alert[0].status=='new'
        assert closeEnough(alert[0].severity, EXPECTED_SEVERITY)
        assert alert[0].confidence>MIN_CONFIDENCE

        #VERIFY last_run HAS BEEEN UPDATED
        last_run=self.db.query(
            "SELECT last_run FROM alert_reasons WHERE code={{type}}",
            {"type":REASON}
        )[0].last_run
        expected_run_after=datetime.utcnow()+timedelta(minutes=-1)
        assert last_run>=expected_run_after

        #REMEMEBER id FOR CHECKING OBSOLETE
        self.alert_id=alert[0].id

        #VERIFY test_data_all_dimensions HAS BEEN MARKED PROPERLY
        h0_rejected = self.db.query("""
            SELECT
                h0_rejected
            FROM
                test_data_all_dimensions t
            JOIN
                alerts a ON a.tdad_id=t.id
            WHERE
                a.id={{alert_id}}
            """,
            {"alert_id":alert[0].id}
        )

        assert len(h0_rejected)==1
        assert h0_rejected[0].h0_rejected==1





    def _setup(self, test_data):
        uid=self.db.query("SELECT util_newid() uid FROM DUAL")[0].uid

        ## VERFIY THE alert_reason EXISTS
        exists=self.db.query("""
            SELECT
                count(1) num
            FROM
                alert_reasons
            WHERE
                code={{reason}}
            """,
            {"reason":REASON}
        )[0].num
        if exists==0:
            D.error("Expecting the database to have an alert_reason={{reason}}", {"reason":REASON})

        ## MAKE A 'PAGE' TO TEST
        self.db.execute("DELETE FROM pages")
        self.db.insert("pages", {
            "test_id":0,
            "url":self.url
        })
        self.page_id=self.db.query("SELECT id FROM pages")[0].id

        ## ENSURE THERE ARE NO ALERTS IN DB
        self.db.execute("DELETE FROM alerts WHERE reason={{reason}}", {"reason":REASON})
        self.insert_test_results(test_data)


    def insert_test_results(self, test_data):
        ## diff_time IS REQUIRED TO TRANSLATE THE TEST DATE DATES TO SOMETHING MORE CURRENT
        now_time=CNV.datetime2unix(datetime.utcnow())
        max_time=max(Q.select(test_data, "timestamp"))
        diff_time=now_time-max_time


        ## INSERT THE TEST RESULTS
        for t in test_data:
            time=t.timestamp
            time+=diff_time

            self.db.insert("test_data_all_dimensions",{
                "id":SQL("util_newid()"),
                "test_run_id":SQL("util_newid()"),
                "product_id":0,
                "operating_system_id":0,
                "test_id":0,
                "page_id":self.page_id,
                "date_received":time,
                "revision":"ba928cbd5191",
                "product":"Firefox",
                "branch":"Mozilla-Inbound",
                "branch_version":"23.0a1",
                "operating_system_name":"mac",
                "operating_system_version":"OS X 10.8",
                "processor":"x86_64",
                "build_type":"opt",
                "machine_name":"talos-mtnlion-r5-049",
                "pushlog_id":19998363,
                "push_date":time,
                "test_name":"tp5o",
                "page_url":self.url,
                "mean":float(t.mean),
                "std":sqrt(t.variance),
                "h0_rejected":None,
                "p":None,
                "n_replicates":t.count,
                "fdr":0,
                "trend_mean":None,
                "trend_std":None,
                "test_evaluation":0,
                "status":1
            })




@pytest.fixture()
def settings(request):
    settings=startup.read_settings(filename="test_settings.json")
    D.start(settings.debug)
    make_test_database(settings)

    def fin():
        D.stop()
    request.addfinalizer(fin)

    return settings



def test_1(settings):
    test_data1=Struct(**{
        "header":("date", "count", "mean-std", "mean", "mean+std", "reject"),
        "rows":[
            ("2013-Apr-05 13:55:00", "23", "655.048136994614", "668.5652173913044", "682.0822977879948"),
            ("2013-Apr-05 13:59:00", "23", "657.8717192954238", "673.3478260869565", "688.8239328784892"),
            ("2013-Apr-05 14:05:00", "23", "658.3247270429598", "673", "687.6752729570402"),
            ("2013-Apr-05 14:08:00", "23", "658.5476631609771", "673.6521739130435", "688.7566846651099"),
            ("2013-Apr-05 14:16:00", "23", "653.2311994952266", "666.1739130434783", "679.1166265917299"),
            ("2013-Apr-05 14:26:00", "23", "659.5613845589426", "671.8260869565217", "684.0907893541009"),
            ("2013-Apr-05 14:42:00", "23", "662.3517791831357", "677.1739130434783", "691.9960469038208"),
            ("2013-Apr-05 15:26:00", "23", "659.8270045518033", "672", "684.1729954481967"),
            ("2013-Apr-05 15:30:00", "23", "659.4023663187861", "674", "688.5976336812139"),
            ("2013-Apr-05 15:32:00", "23", "652.8643631817508", "666.9565217391304", "681.0486802965099"),
            ("2013-Apr-05 15:35:00", "23", "661.6037178485499", "675.1739130434783", "688.7441082384066"),
            ("2013-Apr-05 15:39:00", "23", "658.0124378440726", "670.1304347826087", "682.2484317211449"),
            ("2013-Apr-05 16:20:00", "46", "655.9645219644624", "667.4782608695652", "678.9919997746681"),
            ("2013-Apr-05 16:30:00", "23", "660.2572506418051", "671.8695652173913", "683.4818797929775"),
            ("2013-Apr-05 16:31:00", "23", "661.011102554583", "673.4347826086956", "685.8584626628083"),
            ("2013-Apr-05 16:55:00", "23", "655.9407699325201", "671.304347826087", "686.6679257196539"),
            ("2013-Apr-05 17:07:00", "23", "657.6412277100247", "667.5217391304348", "677.4022505508448"),
#        ("2013-Apr-05 17:12:00", "23", "598.3432138277318", "617.7391304347826", "637.1350470418334"),   # <--DIP IN DATA
            ("2013-Apr-05 17:23:00", "23", "801.0537973113723", "822.1739130434783", "843.2940287755843", 1)  # <--SPIKE IN DATA
        ]
    })
    test_data1=[
        Struct(**{
            "timestamp":CNV.datetime2unix(CNV.string2datetime(t.date, "%Y-%b-%d %H:%M:%S")),
            "datetime":CNV.string2datetime(t.date, "%Y-%b-%d %H:%M:%S"),
            "count":int(t.count),
            "mean":float(t.mean),
            "variance":pow(float(t["mean+std"])-float(t.mean), 2),
            "reject":t.reject
        })
        for t in CNV.table2list(test_data1.header, test_data1.rows)
    ]

    with DB(settings.database) as db:
        tester=test_alert_exception(db)
        tester.test_alert_generated(test_data1)
        db.rollback()  #REMEMBER NOTHING
        db.begin()


        
def test_2(settings):
    test_data2=Struct(**{
        "header":("timestamp", "mean", "std", "h0_rejected", "count"),
        "rows":[
            (1366388389, 295.36, 32.89741631, 0, 25),
            (1366387915, 307.92, 32.86198412, 0, 25),
            (1366390777, 309, 41.22802445, 0, 25),
            (1366398771, 309.24, 34.18488945, 0, 25),
            (1366401499, 308.2, 30.36170834, 0, 25),
            (1366412504, 192.8, 46.27634385, 1, 25),    # Should be an alert
            (1366421699, 298.04, 29.09249617, 0, 25),
            (1366433920, 324.52, 28.13378752, 0, 25),
            (1366445744, 302.2, 28.19131072, 0, 25),
            (1366455408, 369.96, 31.25363979, 0, 25),
            (1366474119, 313.12, 33.66541252, 0, 25),
            (1366483789, 369.96, 30.81460693, 0, 25),
            (1366498412, 311.76, 36.02462121, 0, 25),
            (1366507773, 291.08, 27.86562996, 0, 25)
        ]
    })
    test_data2=[
        Struct(**{
            "timestamp":t.timestamp,
            "datetime":CNV.unix2datetime(t.timestamp),
            "count":t.count,
            "mean":t.mean,
            "variance":pow(t.std, 2),
            "reject":t.h0_rejected
        })
        for t in CNV.table2list(test_data2.header, test_data2.rows)
    ]



    with DB(settings.database) as db:
        tester=test_alert_exception(db)
        tester.test_alert_generated(test_data2)
        db.rollback()  #REMEMBER NOTHING
        db.begin()

#ADD TEST TO DECREASE TOLERANCE AND PROVE ALERTS OR obsoleted

