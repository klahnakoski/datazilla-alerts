
################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## Author: Kyle Lahnakoski (kyle@lahnakoski.com)
################################################################################


from datetime import timedelta, datetime
from numpy.lib.scimath import power, sqrt

## I WANT TO REFER TO "scipy.stats" TO BE EXPLICIT
import scipy
from scipy import stats
scipy.stats = stats

from dzAlerts.util.timer import Timer
from dzAlerts.daemons.alert import update_h0_rejected, significant_difference
from dzAlerts.util.basic import nvl
from dzAlerts.util.cnv import CNV
from dzAlerts.util.db import SQL
from dzAlerts.util.logs import Log
from dzAlerts.util.struct import Struct
from dzAlerts.util.query import Q
from dzAlerts.util.stats import Z_moment, stats2z_moment, Stats, z_moment2stats
from dzAlerts.util.db import DB
from dzAlerts.util.startup import startup


SEVERITY = 0.6              #THERE ARE MANY FALSE POSITIVES
MIN_CONFIDENCE = 0.99
REASON="alert_exception"     #name of the reason in alert_reason
LOOK_BACK=timedelta(days = 41)
WINDOW_SIZE = 10

def alert_exception (db, debug):
    """
    find single points that deviate from the trend
    """
    db.debug = debug
    

    #LOAD CONFIG

    #CALCULATE HOW FAR BACK TO LOOK
    #BRING IN ALL NEEDED DATA
    start_time = datetime.utcnow()-LOOK_BACK
    if debug: Log.note("Pull all data")

    test_results = db.query("""
        SELECT
            id tdad_id,
            test_name,
            product,
            branch,
            branch_version,
            revision,
            operating_system_name,
            operating_system_version,
            processor,
            page_id,
            page_url,
            test_run_id,
            coalesce(push_date, date_received) push_date,
            n_replicates `count`,
            mean,
            std
        FROM
            test_data_all_dimensions t
        WHERE
            test_name="tp5o" AND
            coalesce(push_date, date_received)>unix_timestamp({{begin_time}}) AND
            n_replicates IS NOT NULL
        ORDER BY   #THE ONLY IMPORTANT ORDERING IS THE push_date, THE REST JUST CLUSTER THE RESULTS
            test_name,
            product,
            branch,
            branch_version,
            operating_system_name,
            operating_system_version,
            processor,
            page_url,
            coalesce(push_date, date_received)
        """,
        {"begin_time":start_time}
    )

    alerts=[]   #PUT ALL THE EXCEPTION ITEM HERE

    Log.note("{{num}} test results found", {"num":len(test_results)})
    if debug: Log.note("Find exceptions")

    for keys, values in Q.groupby(test_results, [
        "test_name",
        "product",
        "branch",
        "branch_version",
        "operating_system_name",
        "operating_system_version",
        "processor",
        "page_url"
    ]):
        total = Z_moment()                #total ROLLING STATS ACCUMULATION
        if len(values)<=1: continue     #CAN DO NOTHING WITH THIS ONE SAMPLE
        num_new = 0

        with Timer("stats on revisions"):
            for count, v in enumerate(values):
                s = Stats(
                    count = 1,  #THE INTER-TEST VARIANCE IS SIGNIFICANT AND CAN
                              #NOT BE EXPLAINED.  WE SIMPLY CONSIDER TEST SERIES
                              #A SINGLE SAMPLE
                    mean = v.mean,
                    biased = True
                )
                if count>1: #NEED AT LEAST 2 SAMPLES TO GET A VARIANCE
                    #SEE HOW MUCH THE CURRENT STATS DEVIATES FROM total
                    t=z_moment2stats(total, unbiased = False)
                    confidence, diff=single_ttest(s.mean, t, min_variance = 1.0/12.0) #ASSUME UNIFORM DISTRIBUTION IF VARIANCE IS TOO SMALL
                    if MIN_CONFIDENCE < confidence and diff>0:
                        num_new+=1
                        v.stddev=diff
                        v.confidence=confidence
                        v.push_date_min=values[max(0, count-WINDOW_SIZE)].push_date #FOR VISUALIZATION 

                        alerts.append(Struct(
                            status="new",
                            create_time=datetime.utcnow(),
                            tdad_id=v.tdad_id,
                            reason=REASON,
                            details=CNV.object2JSON(v),
                            severity=SEVERITY,
                            confidence=confidence
                        ))
                #accumulate v
                m=stats2z_moment(s)
                v.m=m
                total=total+m
                if count>=WINDOW_SIZE:
                    total=total-values[count-WINDOW_SIZE].m  #WINDOW LIMITED TO 5 SAMPLES

            if debug: Log.note(
                "Testing {{num_tests}} samples, {{num_alerts}} alerts, on group  {{key}}",
                {"key":keys, "num_tests":len(values), "num_alerts":num_new}
            )

    if debug: Log.note("Get Current Alerts")

    #CHECK THE CURRENT ALERTS
    current_alerts=db.query("""
        SELECT
            a.id,
            a.tdad_id,
            a.status,
            a.last_updated,
            a.severity,
            a.confidence,
            a.details,
            a.solution
        FROM
            alerts a
        LEFT JOIN
            test_data_all_dimensions t on t.id=a.tdad_id
        WHERE
            coalesce(t.push_date, t.date_received)>unix_timestamp({{begin_time}}) AND
            reason={{type}}
        """, {
            "begin_time":start_time,
            "list":Q.select(alerts, "tdad_id"),
            "type":REASON
        }
    )

# NEED BETTER WAY TO SAY THIS
#      new_alerts=set(Q.select(alerts, "tdad_id")) #dict([(a.tdad_id, a) for a in alerts])
#    current=set(Q.select(current_alerts, "tdad_id"))
#    lookup_current=dict([(c.tdad_id, c) for c in current_alerts])
#
#    if debug: Log.note("Update alerts")
#
#    for new_alert in new_alerts-current:
#        new_alert.id=SQL("util_newid()")
#        new_alert.last_updated=datetime.utcnow()
#        db.insert("alerts", new_alert)
#
#    for existing_alert in new_alerts & current:
#        if len(nvl(existing_alert.solution, "").trim())==0: continue  # DO NOT TOUCH SOLVED ALERTS
#
#        c=lookup_current[existing_alert.tdad_id]
#        if round(existing_alert.severity, 5)!=round(c.severity, 5) or round(existing_alert.confidence, 5)!=round(c.confidence, 5):
#            existing_alert.last_updated=datetime.utcnow()
#            db.update("alerts", {"id":existing_alert.id}, existing_alert)
#
#    for expired_alert in current - new_alerts:
#        if len(nvl(expired_alert.solution, "").trim())==0: continue  # DO NOT TOUCH SOLVED ALERTS
#        expired_alert.status="obsolete"
#        expired_alert.last_updated=datetime.utcnow()
#        db.update("alerts", {"id":expired_alert.id}, expired_alert)





    lookup_alert=Q.unique_index(alerts, "tdad_id")
    lookup_current=Q.unique_index(current_alerts, "tdad_id")

    if debug: Log.note("Update alerts")

    for a in alerts:
        #CHECK IF ALREADY AN ALERT
        if a.tdad_id in lookup_current:
            if len(nvl(a.solution, "").strip())!=0: continue  # DO NOT TOUCH SOLVED ALERTS

            c=lookup_current[a.tdad_id]
            if significant_difference(a.severity, c.severity) or \
               significant_difference(a.confidence, c.confidence) or \
                a.reason!=c.reason \
            :
                a.last_updated=datetime.utcnow()
                db.update("alerts", {"id":c.id}, a)
        else:
            a.id=SQL("util_newid()")
            a.last_updated=datetime.utcnow()
            db.insert("alerts", a)

    #OBSOLETE THE ALERTS THAT ARE NO LONGER VALID
    for c in current_alerts:
        if c.tdad_id not in lookup_alert and c.status!="obsolete":
            c.status="obsolete"
            c.last_updated=datetime.utcnow()
            db.update("alerts", {"id":c.id}, c)

    db.execute(
        "UPDATE alert_reasons SET last_run={{run_time}} WHERE code={{reason}}", {
        "run_time":datetime.utcnow(),
        "reason":REASON
    })

    if debug: Log.note("Reviewing h0")

    update_h0_rejected(db, start_time)


def welchs_ttest(stats1, stats2):
    """
    SNAGGED FROM https://github.com/mozilla/datazilla-metrics/blob/master/dzmetrics/ttest.py#L56
    Execute one-sided Welch's t-test given pre-calculated means and stddevs.

    Accepts summary data (N, stddev, and mean) for two datasets and performs
    one-sided Welch's t-test, returning p-value.

    """

    n1=stats1.count
    m1=stats1.mean
    v1=stats1.variance

    n2=stats2.count
    m2=stats2.mean
    v2=stats2.variance


    vpooled        = v1/n1 + v2/n2
    tt             = abs(m1-m2)/sqrt(vpooled)

    df_numerator   = power(vpooled, 2)
    df_denominator = power(v1/n1, 2)/(n1-1) + power(v2/n2, 2)/(n2-1)
    df             = df_numerator / df_denominator

    t_distribution = scipy.stats.distributions.t(df)
    return t_distribution.cdf(tt), tt


def single_ttest(point, stats, min_variance=0):
    n1=stats.count
    m1=stats.mean
    v1=stats.variance


################################################################################
## Gamma Distribution better models wait times, but large k makes it almost normal
## This current code does not compensate for unknown variance in the sample
## https://en.wikipedia.org/wiki/Gamma_distribution
## http://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.gamma.html
##
#    v1=stats.variance*n1/(n1-1)
#    k = (m1**2)/v1
#    theta=v1/m1
#    g=scipy.stats.gamma(k, scale=1/theta)
#    return abs(g.cdf(point)-0.5)+0.5, abs(point-m1)/sqrt(v1)

    try:
        tt=(point-m1)/max(min_variance, sqrt(v1))    #WE WILL IGNORE UNUSUALLY GOOD TIMINGS
        t_distribution = scipy.stats.distributions.t(n1-1)
        return t_distribution.cdf(tt), tt
    except Exception, e:
        Log.error("error with t-test", e)



def main():
    settings=startup.read_settings()
    Log.start(settings.debug)
    try:
        Log.note("Finding exceptions in schema {{schema}}", {"schema":settings.database.schema})

        with DB(settings.database) as db:
            alert_exception(
                db=db,
                debug=settings.debug is not None
            )
    except Exception, e:
        Log.warning("Failure to find exceptions", cause=e)
    finally:
        Log.stop()



if __name__ == '__main__':
    main()