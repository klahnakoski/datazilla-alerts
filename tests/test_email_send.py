# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import unicode_literals
import pytest
from dzAlerts.daemons.email_send import email_send
from dzAlerts.util.env import startup
from dzAlerts.util.sql.db import DB
from dzAlerts.util.env.logs import Log
from dzAlerts.util.queries import Q
from util import testing


class test_email_send():
    """
    VERIFY ENTRIES IN THE email_* TABLES ACTUALLY GET SENT AS INTENDED:
    TO THE CORRECT EMAIL ADDRESSES AND HAVE THE CORRECT CONTENT
    """

    def __init__(self, db, settings):
        self.db = db
        self.settings = settings
        self.emailer = None
        self.uid = None


    def test_zero_receivers(self):
        self.uid = str(self.db.query("SELECT util_newID() uid FROM DUAL")[0].uid)
        self.setup([])
        self.help_test_emailer([])

    def test_one_receivers(self):
        self.uid = str(self.db.query("SELECT util_newID() uid FROM DUAL")[0].uid)
        to_list = [self.uid + "@mozilla.com"]
        self.setup(to_list)
        self.help_test_emailer(to_list)

    def test_many_receivers(self):
        self.uid = str(self.db.query("SELECT util_newID() uid FROM DUAL")[0].uid)
        to_list = [self.uid + "_" + str(i) + "@mozilla.com" for i in range(0, 10)]
        self.setup(to_list)
        self.help_test_emailer(to_list)


    def help_test_emailer(self, to_list):
        ########################################################################
        # TEST
        ########################################################################
        email_send(
            db=self.db,
            emailer=self.emailer,
            debug=True
        )

        ########################################################################
        # VERIFY
        ########################################################################
        self.verify_notify_is_cleared()
        if len(to_list) == 0:
            #ENSURE NOTHING SENT
            mail_content = self.db.query("SELECT id, subject, date_sent, body FROM email_content WHERE subject={{subject}}", {"subject": "subject" + self.uid})
            assert len(mail_content) == 0
        else:
            mail_content = self.verify_content()
            self.verify_delivery(mail_content, to_list)
            self.verify_sent(to_list)


    def setup(self, to_list):
        self.emailer = testing.Emailer(self.settings.email)

        self.db.call("email_send", (
            ";".join(to_list), #to
            "subject" + self.uid, #title
            "body" + self.uid, #body
            None
        ))


    def verify_notify_is_cleared(self):
        #THE NOTIFY FLAG IS PROPERLY CLEARED
        notify = self.db.query("SELECT new_mail FROM email_notify")[0].new_mail
        assert notify == 0


    def verify_content(self):
        #MAIL DOES EXIST
        mail_content = self.db.query("SELECT id, subject, date_sent, body FROM email_content WHERE subject={{subject}}", {"subject": "subject" + self.uid})
        assert len(mail_content) == 1
        assert mail_content[0].date_sent != None
        assert mail_content[0].body == "body" + self.uid
        return mail_content[0]


    def verify_delivery(self, mail_content, to_list):
        #VERIFY DELIVERY IN DATABASE IS SAME AS LIST
        mail_delivery = self.db.query("SELECT id, deliver_to FROM email_delivery WHERE content={{content_id}}", {"content_id": mail_content.id})
        mail_delivery = set(Q.select(mail_delivery, "deliver_to"))
        assert mail_delivery == set(to_list)


    def verify_sent(self, to_list):
        assert len(self.emailer.sent) == 1
        assert self.emailer.sent[0].from_address == None
        assert self.emailer.sent[0].subject == "subject" + self.uid
        assert self.emailer.sent[0].text_data == None
        assert self.emailer.sent[0].html_data == "body" + self.uid

        #THE EMAIL SHOULD HAVE BEEN SENT TO EVERYONE IN to_list. NO MORE, NO LESS
        to_list = set(to_list)
        to_addr = set(self.emailer.sent[0].to_addrs)
        assert to_list == to_addr


@pytest.fixture()
def settings(request):
    settings = startup.read_settings(filename="test_settings.json")
    Log.start(settings.debug)
    testing.make_test_database(settings)

    def fin():
        Log.stop()

    request.addfinalizer(fin)

    return settings


def test_1(settings):
    with DB(settings.perftest) as db:
        test_email_send(db, settings).test_zero_receivers()


def test_2(settings):
    with DB(settings.perftest) as db:
        test_email_send(db, settings).test_one_receivers()


def test_3(settings):
    with DB(settings.perftest) as db:
        test_email_send(db, settings).test_many_receivers()

