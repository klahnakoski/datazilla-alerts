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
import requests
from dzAlerts.imports.mozilla_hg import MozillaGraph
from pyLibrary.collections import MAX
from pyLibrary.env.elasticsearch import Cluster
from pyLibrary.env.files import File
from pyLibrary.env.profiles import Profiler
from pyLibrary.queries import Q
from pyLibrary.queries.es_query import ESQuery
from pyLibrary.struct import nvl, Struct
from pyLibrary.env.logs import Log
from pyLibrary.env import startup
from pyLibrary.cnv import CNV
from pyLibrary.structs.wraps import wrap, unwrap
from pyLibrary.thread.threads import ThreadedQueue
from transform import DZ_to_ES
from pyLibrary.times.timer import Timer
from pyLibrary.thread.multithread import Multithread



NUM_PER_BATCH = 1000
COUNTER = Struct(count=0)
# GC_LOCKER = Lock()

def etl(es_sink, file_sink, settings, transformer, max_id, id):
    """
    PULL FROM DZ AND PUSH TO es AND file_sink
    """

    url = settings.production.blob_url + "/" + str(id)
    try:
        with Timer("read {{id}} from DZ", {"id": id}):
            content = requests.get(url, timeout=nvl(settings.production.timeout, 30)).content
    except Exception, e:
        Log.warning("Failure to read from {{url}}", {"url": url}, e)
        return False

    try:
        if content.startswith("Id not found"):
            if id < max_id:
                return True
            else:
                Log.note("{{id}} not found {{url}}", {"id": id, "url": url})
                return False

        data = CNV.JSON2object(content.decode('utf-8'))
        content = CNV.object2JSON(data)  #ENSURE content HAS NO crlf

        if data.test_run_id:
            Log.println("Add {{id}} for revision {{revision}} ({{bytes}} bytes)", {
                "id": id,
                "revision": data.json_blob.test_build.revision,
                "bytes": len(content)
            })
            with Profiler("transform"):
                result = transformer.transform(id, data)

            if result:
                Log.println("{{num}} records to add", {
                    "num": len(result)
                })
                es_sink.extend({"value": d} for d in result)

            file_sink.add(str(id) + "\t" + content + "\n")
        elif data.error_flag == 'Y':
            error = data.json_blob
            error.datazilla = data
            error.results = None
            data.json_blob = None
            es_sink.add({"value": error})
        else:
            Log.println("No test run id for {{id}}", {"id": id})

        del data
        return True
    except Exception, e:
        Log.warning("Failure to etl (content length={{length}})", {"length": len(content)}, e)
        return False


def get_existing_ids(es, settings, branches):
    #FIND WHAT'S IN ES
    bad_ids = []
    int_ids = set()

    demand_pushlog = {"or": [
        {"exists": {"field": "test_build.push_date"}},
        {"exists": {"field": "test_build.no_pushlog"}}
    ]}

    if settings.elasticsearch.debug and settings.production.step < 10:
        # SIMPLY RELOAD THIS SMALL NUMBER
        return set([])

    with ESQuery(es) as esq:
        max_id = esq.query({
            "from": es.settings.alias,
            "select": {"name": "max_id", "value": "treeherder.job_id", "aggregate": "max"},
            "edges": [
                {"value": "test_build.branch"}
            ]
        })

        interval_size = 200000
        for mini, maxi in Q.intervals(settings.production.min, max_id+interval_size, interval_size):
            existing_ids = es.search({
                "query": {
                    "filtered": {
                        "query": {"match_all": {}},
                        "filter": {"and": [
                            {"range": {"datazilla.id": {"gte": mini, "lt": maxi}}},
                            demand_pushlog
                        ]}
                    }
                },
                "from": 0,
                "size": 0,
                "sort": [],
                "facets": {
                    "ids": {"terms": {"field": "datazilla.id", "size": interval_size}}
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


def extract_from_datazilla_using_id(es, settings, transformer):

    existing_ids = get_existing_ids(es, settings, transformer.pushlog.settings.branches)
    max_existing_id = nvl(MAX(existing_ids), settings.production.min)
    holes = set(range(settings.production.min, max_existing_id)) - existing_ids
    missing_ids = set(range(settings.production.min, max_existing_id+nvl(settings.production.step, NUM_PER_BATCH))) - existing_ids

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
                with Multithread(simple_etl, threads=settings.production.threads) as many:
                    results = many.execute([
                        {"id": id}
                        for id in Q.sort(missing_ids)[:nvl(settings.production.step, NUM_PER_BATCH):]
                    ])
                    for result in results:
                        if not result:
                            num_not_found += 1
                            if num_not_found > nvl(settings.production.max_tries, 10):
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
                    if id < settings.production.min:
                        continue
                    if id in existing_ids:
                        continue

                    if num > settings.production.step:
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
    return wrap({branch.name:unwrap(branch) for branch in branches})


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
            settings.production.threads = nvl(settings.production.threads, 1)
            settings.param.output_file = nvl(settings.param.output_file, "./results/raw_json_blobs.tab")

            #GET BRANCHES
            branches = get_branches(settings.treeherder)
            #SETUP PUSHLOG PULLER
            hg = MozillaGraph({"branches": branches})
            transformer = DZ_to_ES(hg)

            #RESET ONLY IF NEW Transform IS USED
            if settings.args.restart:
                es = Cluster(settings.elasticsearch).create_index(settings.elasticsearch)
                es.add_alias()
                es.delete_all_but_self()
                extract_from_datazilla_using_id(es, settings, transformer)
            else:
                es = Cluster(settings.elasticsearch).get_or_create_index(settings.elasticsearch)
                extract_from_datazilla_using_id(es, settings, transformer)
    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


main()
