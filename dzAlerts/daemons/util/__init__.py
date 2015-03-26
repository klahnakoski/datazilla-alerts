# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import unicode_literals
from __future__ import division

from datetime import datetime
from pyLibrary.collections import OR
from pyLibrary.debugs.logs import Log
from pyLibrary.maths import Math


# ARE THESE SEVERITY OR CONFIDENCE NUMBERS SIGNIFICANTLY DIFFERENT TO WARRANT AN
# UPDATE?
from pyLibrary.queries import qb
from pyLibrary.sql import SQL
from pyLibrary.dot import nvl

SIGNIFICANT = 0.2
DEBUG_TOUCH_ALL_ALERTS = False
VERBOSE = True
NOW = datetime.utcnow()


def significant_difference(a, b):
    try:
        if a in (0.0, 1.0) or b in (0.0, 1.0):
            return True
        if a / b < (1 - SIGNIFICANT) or (1 + SIGNIFICANT) < a / b:
            return True
        b_diff = Math.bayesian_subtract(a, b)
        if 0.3 < b_diff < 0.7:
            return False
        return True
    except Exception, e:
        Log.error("Problem", e)


def significant_score_difference(a, b):
    return abs(a - b) > 0.5


def update_alert_status(settings, alerts_db, found_alerts, old_alerts):
    verbose = nvl(settings.param.verbose, VERBOSE)

    found_alerts = qb.unique_index(found_alerts, "tdad_id", fail_on_dup=False)
    old_alerts = qb.unique_index(old_alerts, "tdad_id")

    new_alerts = found_alerts - old_alerts
    changed_alerts = found_alerts & old_alerts
    obsolete_alerts = old_alerts - found_alerts

    if verbose:
        Log.note("Update Alerts: ({{num_new}} new, {{num_change}} changed, {{num_delete}} obsoleted)", {
            "num_new": len(new_alerts),
            "num_change": len(changed_alerts),
            "num_delete": len(obsolete_alerts)
        })
    if new_alerts:
        for a in new_alerts:
            a.id = SQL("util.newid()")
            a.last_updated = NOW
        try:
            #TODO: MySQL APPEARS TO HAVE A SIZE LIMIT
            for _, na in qb.groupby(new_alerts, size=100):
                alerts_db.insert_list("alerts", na)
        except Exception, e:
            Log.error("problem with insert", e)

    # CURRENT ALERTS, UPDATE IF DIFFERENT
    for new_alert in changed_alerts:
        old_alert = old_alerts[new_alert]
        if len(nvl(old_alert.comment, "").strip()) != 0:
            continue  # DO NOT TOUCH SOLVED ALERTS

        if new_alert == None:
            Log.error("Programmer error, changed_alerts must have {{key_value}}", {"key_value": old_alert.tdad.id})

        if OR(
            DEBUG_TOUCH_ALL_ALERTS,
            old_alert.status == 'obsolete',
            significant_difference(new_alert.severity, old_alert.severity),
            significant_score_difference(new_alert.confidence, old_alert.confidence)
        ):
            new_alert.last_updated = NOW
            alerts_db.update("alerts", {"id": old_alert.id}, new_alert)

    # OBSOLETE THE ALERTS THAT ARE NO LONGER VALID
    for old_alert in qb.filter(obsolete_alerts, {"not": {"term": {"status": "obsolete"}}}):
        alerts_db.update("alerts", {"id": old_alert.id}, {"status": "obsolete", "last_updated": NOW})

    alerts_db.flush()

