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
from math import sqrt
import datetime
from dzAlerts.imports.repos.revisions import Revision

import pyLibrary
from pyLibrary import convert
from pyLibrary.collections import MIN, MAX
from pyLibrary.env.profiles import Profiler
from pyLibrary.maths import Math
from pyLibrary.maths.stats import Stats, ZeroMoment2Stats, ZeroMoment
from pyLibrary.structs import literal_field, Struct, nvl
from pyLibrary.structs.lists import StructList
from pyLibrary.thread.threads import Lock
from pyLibrary.env.logs import Log
from pyLibrary.queries import Q


DEBUG = False
ARRAY_TOO_BIG = 1000
NOW = datetime.datetime.utcnow()
TOO_OLD = NOW - datetime.timedelta(days=30)
PUSHLOG_TOO_OLD = NOW - datetime.timedelta(days=7)


class Talos2ES():
    def __init__(self, hg):
        self.repo = hg
        self.locker = Lock()
        self.unknown_branches = set()

    def __del__(self):
        try:
            Log.println("Branches missing from pushlog:\n{{list}}", {"list": self.unknown_branches})
        except Exception, e:
            pass


    # CONVERT THE TESTS (WHICH ARE IN A dict) TO
    def transform(self, uid, talos_test_result):
        try:
            r = talos_test_result

            #CONVERT UNIX TIMESTAMP TO MILLISECOND TIMESTAMP
            r.testrun.date *= 1000

            def mainthread_transform(r):
                if r == None:
                    return None

                output = Struct()

                for i in r.mainthread_readbytes:
                    output[literal_field(i[1])].name = i[1]
                    output[literal_field(i[1])].readbytes = i[0]
                r.mainthread_readbytes = None

                for i in r.mainthread_writebytes:
                    output[literal_field(i[1])].name = i[1]
                    output[literal_field(i[1])].writebytes = i[0]
                r.mainthread_writebytes = None

                for i in r.mainthread_readcount:
                    output[literal_field(i[1])].name = i[1]
                    output[literal_field(i[1])].readcount = i[0]
                r.mainthread_readcount = None

                for i in r.mainthread_writecount:
                    output[literal_field(i[1])].name = i[1]
                    output[literal_field(i[1])].writecount = i[0]
                r.mainthread_writecount = None

                r.mainthread = output.values()

            mainthread_transform(r.results_aux)
            mainthread_transform(r.results_xperf)

            #ADD PUSH LOG INFO
            try:
                branch = r.test_build.branch
                if branch.endswith("-Non-PGO"):
                    r.test_build.branch = branch
                    r.test_build.pgo = False
                    branch = branch[0:-8]
                else:
                    r.test_build.pgo = True

                with Profiler("get from pushlog"):
                    with self.locker:
                        revision = self.repo.get_node(Revision(**{"branch": {"name":branch}, "changeset": {"id": r.test_build.revision}}))
                    if revision:
                        with self.locker:
                            push = self.repo.get_push(revision)

                        if push:
                            r.test_build.push_date = int(Math.round(push.date * 1000))
                        else:
                            if r.test_build.revision == 'NULL':
                                r.test_build.no_pushlog = True  # OOPS! SOMETHING BROKE
                            elif convert.milli2datetime(r.testrun.date) < PUSHLOG_TOO_OLD:
                                Log.note("{{branch}} @ {{revision}} has no pushlog, transforming anyway", r.test_build)
                                r.test_build.no_pushlog = True
                            else:
                                Log.note("{{branch}} @ {{revision}} has no pushlog, try again later", r.test_build)
                                return []  # TRY AGAIN LATER
                    else:
                        with self.locker:
                            if branch not in self.unknown_branches:
                                Log.note("Whole branch {{branch}} has no pushlog", {"branch":branch})
                                self.unknown_branches.add(branch)
                            if convert.milli2datetime(r.testrun.date) < PUSHLOG_TOO_OLD:
                                r.test_build.no_pushlog = True
                            else:
                                r.test_build.no_pushlog = True
                                #return [r]  #TODO: DO THIS IF WE FIGURE OUT HOW TO HANDLE THE VERY LARGE NUMBER OF RESULTS WITH NO PUSHLOG

            except Exception, e:
                Log.warning("{{branch}} @ {{revision}} has no pushlog", r.test_build, e)

            new_records = []

            # RECORD THE UNKNOWN PART OF THE TEST RESULTS
            remainder = r.copy()
            remainder.results = None
            if len(remainder.keys()) > 3:
                new_records.append(remainder)

            #RECORD TEST RESULTS
            total = StructList()
            if r.testrun.suite in ["dromaeo_css", "dromaeo_dom"]:
                #dromaeo IS SPECIAL, REPLICATES ARE IN SETS OF FIVE
                #RECORD ALL RESULTS
                for i, (test_name, replicates) in enumerate(r.results.items()):
                    for g, sub_results in Q.groupby(replicates, size=5):
                        new_record = Struct(
                            test_machine=r.test_machine,
                            treeherder=r.treeherder,
                            testrun=r.testrun,
                            test_build=r.test_build,
                            result={
                                "test_name": unicode(test_name) + "." + unicode(g),
                                "ordering": i,
                                "samples": sub_results
                            }
                        )
                        try:
                            s = stats(sub_results)
                            new_record.result.stats = s
                            total.append(s)
                        except Exception, e:
                            Log.warning("can not reduce series to moments", e)
                        new_records.append(new_record)
            else:
                for i, (test_name, replicates) in enumerate(r.results.items()):
                    new_record = Struct(
                        test_machine=r.test_machine,
                        treeherder=r.treeherder,
                        testrun=r.testrun,
                        test_build=r.test_build,
                        result={
                            "test_name": test_name,
                            "ordering": i,
                            "samples": replicates
                        }
                    )
                    try:
                        s = stats(replicates)
                        new_record.result.stats = s
                        total.append(s)
                    except Exception, e:
                        Log.warning("can not reduce series to moments", e)
                    new_records.append(new_record)

            if len(total) > 1:
                # ADD RECORD FOR GEOMETRIC MEAN SUMMARY

                new_record = Struct(
                    test_machine=r.test_machine,
                    treeherder=r.treeherder,
                    testrun=r.testrun,
                    test_build=r.test_build,
                    result={
                        "test_name": "SUMMARY",
                        "ordering": -1,
                        "stats": geo_mean(total)
                    }
                )
                new_records.append(new_record)

                # ADD RECORD FOR GRAPH SERVER SUMMARY
                new_record = Struct(
                    test_machine=r.test_machine,
                    treeherder=r.treeherder,
                    testrun=r.testrun,
                    test_build=r.test_build,
                    result={
                        "test_name": "summary_old",
                        "ordering": -1,
                        "stats": Stats(samples=Q.sort(total.mean)[:len(total)-1:])
                    }
                )
                new_records.append(new_record)

            return new_records
        except Exception, e:
            Log.error("Transformation failure on id={{uid}}", {"uid": uid}, e)


def stats(values):
    """
    RETURN LOTS OF AGGREGATES
    """
    if values == None:
        return None

    values = values.map(float, includeNone=False)

    z = ZeroMoment.new_instance(values)
    s = Struct()
    for k, v in z.dict.items():
        s[k] = v
    for k, v in ZeroMoment2Stats(z).items():
        s[k] = v
    s.max = MAX(values)
    s.min = MIN(values)
    s.median = pyLibrary.stats.median(values, simple=False)
    s.last = values.last()
    s.first = values[0]
    if Math.is_number(s.variance) and not Math.is_nan(s.variance):
        s.std = sqrt(s.variance)

    return s


def geo_mean(values):
    """
    GIVEN AN ARRAY OF dicts, CALC THE GEO-MEAN ON EACH ATTRIBUTE
    """
    agg = Struct()
    for d in values:
        for k, v in d.items():
            if v != 0:
                agg[k] = nvl(agg[k], ZeroMoment.new_instance()) + Math.log(Math.abs(v))
    return {k: Math.exp(v.stats.mean) for k, v in agg.items()}


