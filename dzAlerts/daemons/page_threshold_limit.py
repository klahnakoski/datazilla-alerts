# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import unicode_literals
from datetime import timedelta, datetime

from dzAlerts.daemons.alert import update_h0_rejected
from dzAlerts.util.queries import Q
from dzAlerts.util.queries.db_query import esfilter2sqlwhere
from dzAlerts.util.struct import nvl
from dzAlerts.util.cnv import CNV
from dzAlerts.util.sql.db import SQL
from dzAlerts.util.env.logs import Log


REASON = "page_threshold_limit"     #name of the reason in alert_reason
LOOK_BACK = timedelta(weeks=4)


def page_threshold_limit(db, debug):
    """
    simplest of rules to test the dataflow from test_run, to alert, to email
    may prove slightly useful also!
    #point out any pages that are breaking human-set threshold limits
    """
    db.debug = debug

    try:
        #CALCULATE HOW FAR BACK TO LOOK
        lasttime = db.query("SELECT last_run, description FROM alert_reasons WHERE code={{type}}", {"type": REASON})[0]
        lasttime = nvl(lasttime.last_run, datetime.utcnow())
        min_date = lasttime + LOOK_BACK

        #FIND ALL PAGES THAT HAVE LIMITS TO TEST
        #BRING BACK ONES THAT BREAK LIMITS
        #BUT DO NOT ALREADY HAVE AN ALERTS EXISTING
        pages = db.query("""
            SELECT
                t.id tdad_id,
                t.n_replicates,
                t.mean,
                t.std,
                h.threshold,
                h.severity,
                h.reason,
                m.id alert_id
            FROM
                alert_page_thresholds h
            JOIN
                test_data_all_dimensions t ON t.page_id=h.page
            LEFT JOIN
                alerts m on m.tdad_id=t.test_run_id AND m.reason={{type}}
            WHERE
                h.threshold<t.mean AND
                t.push_date>{{min_date}} AND
                (m.id IS NULL OR m.status='obsol11ete')
        """, {
            "type": REASON, "min_date": min_date
        })

        #FOR EACH PAGE THAT BREAKS LIMITS
        for page in pages:
            if page.alert_id != None: break

            alert = {
                "id": SQL("util_newID()"),
                "status": "new",
                "create_time": datetime.utcnow(),
                "last_updated": datetime.utcnow(),
                "tdad_id": page.tdad_id,
                "reason": REASON,
                "details": CNV.object2JSON({"expected": float(page.threshold), "actual": float(page.mean), "reason": page.reason}),
                "severity": page.severity,
                "confidence": 1.0    # USING NORMAL DIST ASSUMPTION WE CAN ADJUST
                # CONFIDENCE EVEN BEFORE THRESHOLD IS HIT!
                # FOR NOW WE KEEP IT SIMPLE
            }

            db.insert("alerts", alert)

        for page in pages:
            if page.alert_id == None: break
            db.update("alerts", None)  #ERROR FOR NOW


        #OBSOLETE THE ALERTS THAT SHOULD NO LONGER GET SENT
        obsolete = db.query("""
            SELECT
                m.id,
                m.tdad_id
            FROM
                alerts m
            JOIN
                test_data_all_dimensions t ON m.tdad_id=t.id
            JOIN
                alert_page_thresholds h on t.page_id=h.page
            WHERE
                m.reason={{reason}} AND
                h.threshold>=t.mean AND
                t.push_date>{{time}}
        """, {
            "reason": REASON,
            "time": min_date
        })

        if obsolete:
            db.execute("UPDATE alerts SET status='obsolete' WHERE {{where}}", {"where": esfilter2sqlwhere(db, {"terms": {"id": Q.select(obsolete, "id")}})})

        db.execute(
            "UPDATE alert_reasons SET last_run={{now}} WHERE code={{reason}}",
            {"now": datetime.utcnow(), "reason": REASON}
        )

        update_h0_rejected(db, min_date, set(Q.select(pages, "tdad_id")) | set(Q.select(obsolete, "tdad_id")))

    except Exception, e:

        Log.error("Could not perform threshold comparisons", e)



