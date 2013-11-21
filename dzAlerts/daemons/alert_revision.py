
################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## Author: Kyle Lahnakoski (kyle@lahnakoski.com)
################################################################################



from datetime import datetime, timedelta
from dzAlerts.daemons import alert_exception
from dzAlerts.util.cnv import CNV
from dzAlerts.util.db import DB, SQL
from dzAlerts.util.queries import Q
from scipy.stats import binom
from dzAlerts.util.struct import nvl
from dzAlerts.daemons.alert import significant_difference


FALSE_POSITIVE_RATE=.95
REASON="alert_revision"     #name of the reason in alert_reason
LOOK_BACK=timedelta(days=41)
SEVERITY=0.9
TEMPLATE=[
    """
    <div><h2>{{score}} - {{revision}}</h2>
    {{num_exceptions}} exceptional events<br>
    <a href="https://datazilla.mozilla.org/talos/summary/{{branch}}/{{revision}}">Datazilla</a><br>
    <a href="https://bugzilla.mozilla.org/show_bug.cgi?id={{bug_id}}">Bugzilla - {{bug_description}}</a><br>
    """,{
        "from":"pages",
        "template":"""
            <hr>
            On page {{.page_url}}<br>
            <a href="https://tbpl.mozilla.org/?tree={{.branch}}&rev={{.revision}}">TBPL</a><br>
            <a href="https://hg.mozilla.org/rev/{{.revision}}">Mercurial</a><br>
            <a href="https://datazilla.mozilla.org/talos/summary/{{.branch}}/{{.revision}}">Datazilla</a><br>
            <a href="http://people.mozilla.com/~klahnakoski/test/es/DZ-ShowPage.html#page={{.page_url}}&sampleMax={{.push_date}}000&sampleMin={{.push_date_min}}000&branch={{.branch}}">Kyle's ES</a><br>
            Raw data: {{.raw_data}}
            """,
        "between":"<hr>"
    }
]

#GET ACTIVE ALERTS
# assumes there is an outside agent corrupting our test results
# this will look at all alerts on a revision, and figure out the probability there is an actual regression

def alert_revision(settings):
    assert settings.db != Null
    settings.db.debug=settings.debug
    db=DB(settings.db)

    #ALL EXISTING ALERTS
    single_exceptions=db.query("""
        SELECT
            id,
            tdad_id,
            details,
            severity,
            t.revision,
            t.page_url,
            t.branch,
            t.product,
            t.operating_system_version,
            t.machine_name,
            t.test_name
        FROM
            alerts a
        LEFT JOIN
            test_data_all_dimensions t on t.id=a.tdad_id
        WHERE
            reason={{reason}} AND
            status<>'obsolete' AND
            t.push_date>{{min_time}} and
            h0_rejected=1
    """, {
        "min_time":CNV.datetime2unix(datetime.utcnow()-LOOK_BACK),
        "reason":alert_exception.REASON
    })
    exception_lookup=Q.index(single_exceptions, ["test_name", "revision"])


    #EXISTING REVISION-LEVEL ALERTS
    existing=db.query("""
        SELECT
            id,
            tdad_id,
            details,
            severity,
            solution
        FROM
            alerts a
        WHERE
            reason={{reason}} AND
            t.push_date>{{min_time}}
    """, {
        "min_time":CNV.datetime2unix(datetime.utcnow()-LOOK_BACK),
        "reason":REASON
    })
    for e in existing:
        e.details=CNV.JSON2object(e.details)

    existing=Q.unique_index(existing, ["details.test_name", "details.revision"])

    #FIND TOTAL TDAD FOR EACH INTERESTING REVISION
    revisions=db.query("""
        SELECT
            revision,
            test_name,
            sum(CASE WHEN t.status='obsolete' THEN 0 ELSE h0_rejected)) num_exceptions,
            count(1) num_tdad
        FROM
            test_data_all_dimensions t
        GROUP BY
            t.revision,
            t.test_name
        WHERE
            revision IN {{revisions}}
    """, {
        "revisions":set(Q.select(single_exceptions, "revision")),
        "min_time":CNV.datetime2unix(datetime.utcnow()-LOOK_BACK)
    })


    #SUMMARIZE
    new_alerts=[{
        "status":"new",
        "create_time":datetime.utcnow(),
        "reason":REASON,
        "details":{
            "revision":r.revision,
            "test_name":r.test_name,
            "num_exceptions":r.num_exceptions,
            "num_tests":r.num_tdad,
            "branch":Null,
            "bug_id":Null,
            "pages":Q.sort(exception_lookup[r.revision], {"value":"severity", "sort":-1})
        },
        "severity":SEVERITY,
        "confidence":binom(r.num_tdad, FALSE_POSITIVE_RATE).sf(r.num_exceptions)  #sf=(1-cdf)
    } for r in revisions]

    new_alerts=Q.unique_index(new_alerts, ["details.test_name", "details.revision"])


    #NEW ALERTS, JUST INSERT
    for r in new_alerts - existing:
        r.id=SQL("util_newid()")
        r.last_updated=datetime.utcnow()
        db.insert("alerts", r)

    #CURRENT ALERTS, UPDATE IF DIFFERENT
    for r in new_alerts & existing:
        if len(nvl(r.solution, "").strip())!=0: continue  # DO NOT TOUCH SOLVED ALERTS

        e=existing[r.id]
        if significant_difference(r.severity, e.severity) or \
            significant_difference (r.confidence, e.confidence) \
        :
            r.last_updated=datetime.utcnow()
            db.update("alerts", {"id":r.id}, r)

    #OLD ALERTS, OBSOLETE
    for e in existing-new_alerts:
        e.status='obsolete'
        e.last_updated=datetime.utcnow()
        db.update("alerts", {"id":e.id}, e)