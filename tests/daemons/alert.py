################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################

from datetime import datetime, timedelta
from string import Template
from datazilla import daemons
from datazilla.daemons.alert import send_alerts
from datazilla.util.cnv import CNV
from datazilla.util.db import DB
from datazilla.util.debug import D
from datazilla.util.map import Map
from datazilla.util.maths import bayesian_add
from datazilla.util.query import Q
from datazilla.util.strings import between
from tests.util.testing import settings, make_test_database


class test_alert:
# datazilla.daemons.alert.py IS A *FUNCTION* WITH DOMAIN alert_* AND CODOMAIN email_*
# self.test_data HAS THE DOMAIN VALUES TO TEST
#
# self.test_data[].details INCLUDES THE pass/fail INDICATION SO THE VERIFICATION
# LOGIC KNOWS WHAT SHOULD BE IN THOSE email_* TABLES


    def __init__(self, db):
        self.now=datetime.utcnow()-timedelta(seconds=1)
        self.recent_past=self.now-timedelta(hours=1)
        self.far_past=self.now-timedelta(days=2)

        self.db=db
#        self.uid=None
        self.series=0
        self.reason="used for testing 1"

        self.high_severity=0.7
        self.high_confidence=0.9
        self.important=bayesian_add(self.high_severity, self.high_confidence)
        self.low_severity=0.5
        self.low_confidence=0.7



    def setup(self, to_list):
        self.uid=self.db.query("SELECT util_newid() uid FROM DUAL")[0].uid

        #CLEAR EMAILS
        self.db.execute("DELETE FROM email_delivery")
        self.db.execute("DELETE FROM email_attachment")
        self.db.execute("DELETE FROM email_content")

        #TEST NUMBER OF LISTENERS IN alert_listeners TABLE
        self.db.execute("DELETE FROM alert_listeners")
        for l in to_list:
            self.db.insert("alert_listeners", {"email":l})


        #MAKE A REASON FOR USE IN THIS TESTING
        self.db.execute("DELETE FROM alert_mail WHERE reason=${reason}", {"reason":self.reason})
        self.db.execute("DELETE FROM alert_reasons WHERE code=${reason}", {"reason":self.reason})
        self.db.insert("alert_reasons", {
            "code":self.reason,
            "description":">>>>${id}<<<<",
            "config":None,
            "last_run":self.now-timedelta(days=1)
        })


        #MAKE SOME TEST DATA (AND GET ID)
        all_dim=Map(**{
            "header":
                ("id","test_run_id","product_id","operating_system_id","test_id","page_id","date_received","revision","product","branch","branch_version","operating_system_name","operating_system_version","processor","build_type","machine_name","pushlog_id","push_date","test_name","page_url","mean","std","h0_rejected","p","n_replicates","fdr","trend_mean","trend_std","test_evaluation","status"),
            "data":[
                (0,117679,65,20,64,860,1366261267,"d6b34be6fb4c","Firefox","Mozilla-Inbound","23.0a1","win","6.2.9200","x86_64","opt","t-w864-ix-022","19801727","1366245741","tp5o","bbc.co.uk",138.8,40.5257120028,0,0.650194865224,25,0,144.37333333365,12.96130778322,1,1)
            ]})
        self.db.insert_list("test_data_all_dimensions", CNV.table2list(all_dim.header, all_dim.data))
        self.series=self.db.query("SELECT min(id) id FROM test_data_all_dimensions")[0].id


        # WE INJECT THE EXPECTED TEST RESULTS RIGHT INTO THE DETAILS, THAT WAY
        # WE CAN SEE THEM IN THE EMAIL DELIVERED
        test_data=Map(**{
            "header":
                ("id",      "status",  "create_time", "last_updated", "last_sent",        "test_series", "reason",    "details",                 "severity",         "confidence",        "solution"),
            "data":[
                #TEST last_sent IS NOT TOO YOUNG
                (self.uid+0,"new",      self.far_past, self.far_past,  self.recent_past, self.series,   self.reason, CNV.object2JSON({"id":0, "expect":"fail"}),  self.high_severity, self.high_confidence, None),
                #TEST last_sent IS TOO OLD, SHOULD BE (RE)SENT
                (self.uid+1,"new",      self.far_past, self.now,       None,             self.series,   self.reason, CNV.object2JSON({"id":1, "expect":"pass"}),  self.high_severity, self.high_confidence, None),
                (self.uid+2,"new",      self.far_past, self.now,       self.far_past,    self.series,   self.reason, CNV.object2JSON({"id":2, "expect":"pass"}),  self.high_severity, self.high_confidence, None),
                (self.uid+3,"new",      self.now,      self.now,       self.recent_past, self.series,   self.reason, CNV.object2JSON({"id":3, "expect":"pass"}),  self.high_severity, self.high_confidence, None),
                #TEST obsolete ARE NOT SENT
                (self.uid+4,"obsolete", self.now,      self.now,       self.far_past,    self.series,   self.reason, CNV.object2JSON({"id":4, "expect":"fail"}),  self.high_severity, self.high_confidence, None),
                #TEST ONLY IMPORTANT ARE SENT
                (self.uid+5,"new",      self.now,      self.now,       None,             self.series,   self.reason, CNV.object2JSON({"id":5, "expect":"pass"}),  self.important,     0.5,                  None),
                (self.uid+6,"new",      self.now,      self.now,       None,             self.series,   self.reason, CNV.object2JSON({"id":6, "expect":"fail"}),  self.low_severity,  self.high_confidence, None),
                (self.uid+7,"new",      self.now,      self.now,       None,             self.series,   self.reason, CNV.object2JSON({"id":7, "expect":"fail"}),  self.high_severity, self.low_confidence,  None),
                #TEST ONES WITH SOLUTION ARE NOT SENT
                (self.uid+8,"new",      self.now,      self.now,       None,             self.series,   self.reason, CNV.object2JSON({"id":8, "expect":"fail"}),  self.high_severity, self.high_confidence, "a solution!")
                ]
        })

        self.test_data=CNV.table2list(test_data.header, test_data.data)
        self.db.insert_list("alert_mail", self.test_data)



        

    def test_send_zero_alerts(self):
        to_list=[]
        self.help_send_alerts(to_list)

    def test_send_one_alert(self):
        to_list=["klahnakoski@mozilla.com"]

        self.help_send_alerts(to_list)

    def test_send_many_alerts(self):
        to_list=["_"+str(i)+"@mozilla.com" for i in range(0,10)]
        self.help_send_alerts(to_list)


    def help_send_alerts(self, to_list):
        try:
            self.db.begin()
            self.setup(to_list)

            ########################################################################
            # TEST
            ########################################################################
            send_alerts(
                db=self.db,
                debug=True
            )

            ########################################################################
            # VERIFY
            ########################################################################
            emails=self.get_new_emails() # id, to, body

            if len(to_list)==0:
                assert len(emails)==0
                self.db.commit()
                return
            
            #VERIFY ONE MAIL SENT
            assert len(emails)==1
            #VERIFY to MATCHES WHAT WAS PASSED TO THIS FUNCTION
            assert set(emails[0].to) == set(to_list), "email_delivery not matching what's send"

            #VERIFY last_sent IS WRITTEN
            alert_state=self.db.query("""
                SELECT
                    id
                FROM
                    alert_mail
                WHERE
                    reason=${reason} AND
                    last_sent>=${send_time}
            """, {
                "reason":self.reason,
                "send_time":self.now
            })
            expected_marked=set([d.id for d in self.test_data if CNV.JSON2object(d.details).expect=='pass'])
            actual_marked=set(Q.select(alert_state, "id"))
            assert expected_marked == actual_marked, Template(
                "Expecting only id in ${expected}, but instead got ${actual}").substitute(
                    {"expected":str(expected_marked),
                     "actual":str(actual_marked)
                })

            #VERIFY BODY HAS THE CORRECT ALERTS
            expecting_alerts=set([d.id for d in map(lambda d: CNV.JSON2object(d.details), self.test_data) if d.expect=='pass'])
            actual_alerts_sent=set([int(between(b, ">>>>", "<<<<")) for b in emails[0].body.split(daemons.alert.SEPARATOR)])
            assert expecting_alerts == actual_alerts_sent
            self.db.commit()
        except Exception, e:
            self.db.rollback()
            D.error("Test failure", e)



    def get_new_emails(self):
        emails=self.db.query("""
            SELECT
                c.id,
                group_concat(d.deliver_to SEPARATOR ',') `to`,
                c.body
            FROM
                email_content c
            LEFT JOIN
                email_delivery d ON d.content=c.id
            WHERE
                c.date_sent IS NULL
            GROUP BY
                c.id
            """)
        for e in emails:
            if e.to is None:
                e.to=[]
            else:
                e.to=e.to.split(",")

        return emails


make_test_database(settings)

with DB(settings.database) as db:
    test_alert(db).test_send_zero_alerts()
    test_alert(db).test_send_one_alert()
    test_alert(db).test_send_many_alerts()
