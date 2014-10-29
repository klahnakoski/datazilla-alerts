# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals

import functools
import hashlib
import requests
from dzAlerts.imports.mozilla_hg import MozillaGraph
from pyLibrary.collections import MAX
from pyLibrary.env.elasticsearch import Cluster
from pyLibrary.env.files import File
from pyLibrary.env.profiles import Profiler
from pyLibrary.queries import Q
from pyLibrary.queries.es_query import ESQuery
from pyLibrary.strings import expand_template
from pyLibrary.struct import nvl, Struct, StructList, set_default
from pyLibrary.env.logs import Log
from pyLibrary.env import startup
from pyLibrary.cnv import CNV
from pyLibrary.structs.wraps import wrap, unwrap
from pyLibrary.thread.threads import ThreadedQueue
from transform import DZ_to_ES
from pyLibrary.times.timer import Timer
from pyLibrary.thread.multithread import Multithread


NUM_PER_BATCH = 1000
JOB_ID_MODULO = 10000
COUNTER = Struct(count=0)


def etl(es_sink, file_sink, settings, transformer, max_id, job_id, branch):
    """
    PULL FROM DZ AND PUSH TO es AND file_sink
    """
    test_results = StructList()
    url = expand_template(settings.treeherder.blob_url, {"min": job_id * settings.treeherder.step, "max": (job_id + 1) * settings.treeherder.step - 1, "branch": branch})
    try:
        with Timer("read {{id}} for branch {{branch}}", {"id": job_id, "branch": branch}):
            content = requests.get(url, timeout=nvl(settings.treeherder.timeout, 30)).content
            data = CNV.JSON2object(content.decode('utf8'))
            if not data:
                #ADD PLACEHOLDERS FOR JOB-IDS WITH NO DATA
                test_results.append({
                    "treeherder": {
                        "branch": branch,
                        "job_id": job_id
                    }
                })
            else:
                for d in data:
                    for t in d.blob.talos_data:
                        try:
                            if isinstance(t, unicode):
                                t = CNV.JSON2object(t)
                            elif isinstance(t, str):
                                t = CNV.JSON2object(t.decode("utf8"))
                            else:
                                pass
                            uid = test_result_to_uid(t)
                            t.treeherder = {
                                "branch": branch,
                                "job_id": job_id,
                                "uid": uid
                            }
                            test_results.append(t)
                        except Exception, e:
                            Log.note("Corrupted test results for job_id {{job_id}}  reason={{reason}}", {"job_id": d.job_id, "reason":e.message})

    except Exception, e:
        Log.warning("Failure to read from {{url}}", {"url": url}, e)
        return False

    try:
        num_results = 0
        for t in test_results:
            id = (branch, t.treeherder.job_id, t.treeherder.uid)
            content = CNV.object2JSON(t)  #ENSURE content HAS NO crlf

            if not t.treeherder.uid:
                es_sink.add({"value": t})
                file_sink.add(CNV.object2JSON(id) + "\t" + content + "\n")
                continue

            num_results += 1

            Log.println("Add {{id}} for revision {{revision}} ({{bytes}} bytes)", {
                "id": id,
                "revision": t.test_build.revision,
                "bytes": len(content)
            })
            with Profiler("transform"):
                result = transformer.transform(id, t)

            if result:
                Log.println("{{num}} records to add", {
                    "num": len(result)
                })
                es_sink.extend({"value": d} for d in result)

            file_sink.add(CNV.object2JSON(id) + "\t" + content + "\n")

        if num_results == 0:
            return False
        return True
    except Exception, e:
        Log.warning("Failure to etl (content length={{length}})", {"length": len(content)}, e)
        return False


def test_result_to_uid(test_result):
    return hashlib.sha1(CNV.object2JSON(test_result, pretty=True).encode("utf8")).hexdigest()


def get_existing_ids(es, settings, branch):
    #FIND WHAT'S IN ES
    bad_ids = []
    int_ids = set()

    demand_pushlog = {"and": [
        {"term": {"treeherder.branch": branch}},
        {"or": [
            {"exists": {"field": "test_build.push_date"}},
            {"exists": {"field": "test_build.no_pushlog"}}
        ]}
    ]}

    if settings.elasticsearch.debug and settings.treeherder.step < 10:
        # SIMPLY RELOAD THIS SMALL NUMBER
        return set([])

    with ESQuery(es) as esq:
        try:
            max_id = esq.query({
                "from": es.settings.alias,
                "select": {"value": "treeherder.job_id", "aggregate": "max"}
            })
        except Exception, e:
            if e.contains("failed to find mapping for treeherder.job_id"):  # HAPPENS DURING NEW INDEX AND NO TEST DATA
                max_id = settings.treeherder.min
            elif e.contains("No mapping found for field [treeherder.job_id]"):
                max_id = settings.treeherder.min
            else:
                raise e

        es_interval_size = 200000
        for mini, maxi in Q.intervals(settings.treeherder.min, max_id + es_interval_size, es_interval_size):
            existing_ids = es.search({
                "query": {
                    "filtered": {
                        "query": {"match_all": {}},
                        "filter": {"and": [
                            {"range": {"treeherder.job_id": {"gte": mini, "lt": maxi}}},
                            demand_pushlog
                        ]}
                    }
                },
                "from": 0,
                "size": 0,
                "sort": [],
                "facets": {
                    "ids": {"terms": {"field": "treeherder.job_id", "size": es_interval_size}}
                }
            })

            for t in existing_ids.facets.ids.terms:
                try:
                    int_ids.add(int(t.term))
                except Exception, e:
                    bad_ids.append(t.term)

        existing_ids = int_ids
        Log.println("Number of ids in ES: " + str(len(existing_ids)))
        Log.println("BAD ids in ES: " + str(bad_ids))
        return existing_ids


def extract_from_datazilla_using_id(es, settings, transformer, branch):
    existing_ids = get_existing_ids(es, settings, branch)
    max_existing_id = nvl(MAX(existing_ids), settings.treeherder.min)
    holes = set(range(settings.treeherder.min, max_existing_id)) - existing_ids
    missing_ids = set(range(settings.treeherder.min, max_existing_id + settings.treeherder.step)) - existing_ids

    Log.note("Max Existing ID: {{max}}", {"max": max_existing_id})
    Log.note("Number missing: {{num}}", {"num": len(missing_ids)})
    Log.note("Number in holes: {{num}}", {"num": len(holes)})
    #FASTER IF NO INDEXING IS ON
    es.set_refresh_interval(-1)

    #FILE IS FASTER THAN NETWORK
    if (len(holes) > 10000 or settings.args.scan_file or settings.args.restart) and File(settings.param.output_file).exists:
        load_from_file(settings, es, existing_ids, transformer)
        missing_ids = missing_ids - existing_ids

    #COPY MISSING DATA TO ES
    try:
        with ThreadedQueue(es, size=nvl(es.settings.batch_size, 100)) as es_sink:
            with ThreadedQueue(File(settings.param.output_file), size=50) as file_sink:
                simple_etl = functools.partial(etl, *[es_sink, file_sink, settings, transformer, max_existing_id])

                num_not_found = 0
                with Multithread(simple_etl, threads=settings.treeherder.threads) as many:
                    results = many.execute([
                        {"job_id": job_id, "branch": branch}
                        for job_id in Q.sort(missing_ids)
                    ])
                    for result in results:
                        if not result:
                            num_not_found += 1
                            if num_not_found > nvl(settings.treeherder.max_tries, 10):
                                many.inbound.pop_all()  # CLEAR THE QUEUE OF OTHER WORK
                                many.stop()
                                break
                        else:
                            num_not_found = 0
    except (KeyboardInterrupt, SystemExit):
        Log.println("Shutdown Started, please be patient")
    except Exception, e:
        Log.error("Unusual shutdown!", e)

    #FINISH ES SETUP SO IT CAN BE QUERIED
    es.set_refresh_interval(1)
    es.delete_all_but_self()
    es.add_alias()


def load_from_file(settings, es, existing_ids, transformer):
    #ASYNCH PUSH TO ES IN BLOCKS OF 1000
    with Timer("Scan file for missing ids"):
        with ThreadedQueue(es, size=nvl(es.settings.batch_size, 100)) as json_for_es:
            num = 0
            for line in File(settings.param.output_file):
                try:
                    if len(line.strip()) == 0:
                        continue
                    col = line.split("\t")
                    id = int(col[0])
                    # if id==3003529:
                    #     Log.debug()
                    if id < settings.treeherder.min:
                        continue
                    if id in existing_ids:
                        continue

                    if num > settings.treeherder.step:
                        return
                    num += 1

                    with Profiler("decode and transform"):
                        data = CNV.JSON2object(col[-1])
                        if data.test_run_id:
                            with Profiler("transform"):
                                data = transformer.transform(id, data)
                            json_for_es.extend({"value": d} for d in data)
                            Log.note("Added {{id}} from file", {"id": id})

                            existing_ids.add(id)
                        else:
                            Log.note("Skipped {{id}} from file (no test_run_id)", {"id": id})
                            num -= 1

                except Exception, e:
                    Log.warning("Bad line id={{id}} ({{length}}bytes):\n\t{{prefix}}", {
                        "id": id,
                        "length": len(CNV.object2JSON(line)),
                        "prefix": CNV.object2JSON(line)[0:130]
                    }, e)


def get_branches(settings):
    response = requests.get(settings.branches.url)
    branches = CNV.JSON2object(CNV.utf82unicode(response.content))
    return wrap({branch.name: unwrap(branch) for branch in branches if branch.name == "mozilla-inbound"})


def main():
    try:
        settings = startup.read_settings(defs=[{
            "name": ["--no_restart", "--no_reset", "--no_redo", "--norestart", "--noreset", "--noredo"],
            "help": "do not allow creation of new index (for debugging rouge resets)",
            "action": "store_true",
            "dest": "no_restart"
        }, {
            "name": ["--restart", "--reset", "--redo"],
            "help": "force a reprocessing of all data",
            "action": "store_true",
            "dest": "restart"
        }, {
            "name": ["--file", "--scan_file", "--scanfile", "--use_file", "--usefile"],
            "help": "scan file for missing ids",
            "action": "store_true",
            "dest": "scan_file"
        }, {
            "name": ["--nofile", "--no_file", "--no-file"],
            "help": "do not scan file for missing ids",
            "action": "store_false",
            "dest": "scan_file"
        }])
        Log.start(settings.debug)

        with startup.SingleInstance(flavor_id=settings.args.filename):
            settings.treeherder.step = nvl(settings.treeherder.step, NUM_PER_BATCH)
            settings.treeherder.threads = nvl(settings.treeherder.threads, 1)
            settings.param.output_file = nvl(settings.param.output_file, "./results/raw_json_blobs.tab")

            #GET BRANCHES
            branches = get_branches(settings.treeherder)
            #SETUP PUSHLOG PULLER
            hg = MozillaGraph(set_default(settings.mozillaHG, {"branches": branches}))
            transformer = DZ_to_ES(hg)

            #RESET ONLY IF NEW Transform IS USED
            if settings.args.restart:
                es = Cluster(settings.elasticsearch).create_index(settings.elasticsearch)
                es.add_alias()
                es.delete_all_but_self()
            else:
                es = Cluster(settings.elasticsearch).get_or_create_index(settings.elasticsearch)

            for b in branches.keys():
                extract_from_datazilla_using_id(es, settings, transformer, b)

    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


main()
