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
import json
import requests
from dzAlerts.imports.mozilla_graph import MozillaGraph

from pyLibrary import convert
from pyLibrary.collections import MAX
from pyLibrary.env.elasticsearch import Cluster
from pyLibrary.env.files import File
from pyLibrary.debugs.profiles import Profiler
from pyLibrary.jsons import json_scrub
from pyLibrary.maths import Math
from pyLibrary.queries import Q
from pyLibrary.queries.es_query import ESQuery
from pyLibrary.strings import expand_template
from pyLibrary.debugs.logs import Log
from pyLibrary.debugs import startup
from pyLibrary.structs.dicts import Struct
from pyLibrary.structs import nvl, set_default, literal_field
from pyLibrary.structs.lists import StructList
from pyLibrary.structs.wraps import wrap, unwrap
from pyLibrary.thread.threads import ThreadedQueue, Lock
from transform import Talos2ES
from pyLibrary.times.timer import Timer
from pyLibrary.thread.multithread import Multithread


DEBUG = False
NUM_PER_BATCH = 1000
JOB_ID_MODULO = 10000
COUNTER = Struct(count=0)


uid_json_encoder = json.JSONEncoder(
    skipkeys=False,
    ensure_ascii=False, # DIFF FROM DEFAULTS
    check_circular=True,
    allow_nan=True,
    indent=None,
    separators=None,
    encoding='utf-8',
    default=None,
    sort_keys=True   # <-- SEE?!  sort_keys==True
)



class TreeHerderImport(object):
    def __init__(self, settings):
        self.settings = settings
        self.perf_signatures = {}
        self.current_branch = None
        # self.properties_lock = Lock()


    def treeherder2talos(self, r, url):

        def convert_properties(output, sig_properties):
            """
            MAP PROPERTIES BACK TO ORIGINAL STRUCTURE
            """
            output.test_machine.os = sig_properties.machine_os_name
            output.test_machine.osversion = sig_properties.machine_platform
            output.test_machine.platform = sig_properties.machine_architecture
            output.test_machine.device_name = sig_properties.device_name

            output.test_build.branch = sig_properties.repository
            output.test_build.os = sig_properties.build_os_name
            output.test_build.osversion = sig_properties.build_platform
            output.test_build.platform = sig_properties.build_architecture
            output.test_build.build_system = sig_properties.build_system_type

            output.testrun.suite = sig_properties.suite
            output.testrun.job_group = sig_properties.job_group_name
            output.testrun.job_type = sig_properties.job_type_name

        th = convert.JSON2object(r.blob).blob

        if not th.metadata.test_build.revision:
            Log.error("missing revision")

        talos = Struct()
        talos.treeherder = {
            "branch": self.current_branch,
            "perf_id": r.id,
            "url": url
        }
        talos.test_build = th.metadata.test_build
        convert_properties(talos, th.signature_properties)

        talos.testrun.options = th.metadata.options
        talos.testrun.date = th.date
        talos.results_aux = talos.metatdata.results_aux
        talos.results_xperf = talos.metatdata.results_xperf
        talos.results[literal_field(th.test)] = th.replicates
        return talos

    def etl(self, es_sink, file_sink, transformer, min_job_id, max_job_id):
        """
        PULL FROM DZ AND PUSH TO es AND file_sink

        job_id IS FIRST ID IN BLOCK OF job_ids OF SIZE settings.treeherder.step
        """
        perf_results = StructList()
        num_results = 0

        # FIRST, DEAL WITH MISSING AND CORRUPT JSON
        try:
            with Timer("read {{id}} for branch {{branch}}", {"id": min_job_id, "branch": self.current_branch}):
                url = expand_template(self.settings.treeherder.blob_url, {
                    "min": min_job_id,
                    "max": max_job_id - 1,
                    "branch": self.current_branch,
                    "count": self.settings.treeherder.step
                })
                content = requests.get(url, timeout=self.settings.treeherder.timeout).content
                data = convert.JSON2object(content.decode('utf8'))
                for job_id in range(min_job_id, max_job_id):
                    d = wrap([d for d in data if d.id == job_id])[0]
                    if not d:
                        #ADD PLACEHOLDERS FOR NO DATA
                        es_sink.add({"value":{
                            "treeherder": {
                                "branch": self.current_branch,
                                "perf_id": job_id,
                                "url": url,
                                "reason": "missing data"
                            }
                        }})
                    elif not d.id or d.id!=job_id or not d.blob:
                        #ADD PLACEHOLDERS FOR NO DATA
                        id = (self.current_branch, job_id)
                        file_sink.add(convert.object2JSON(id) + "\t" + content + "\n")
                        num_results += 1  #WE WANT TO COUNT THIS
                        es_sink.add({"value": {
                            "treeherder": {
                                "branch": self.current_branch,
                                "perf_id": job_id,
                                "corrupt_json": d,
                                "url": url,
                                "reason": "missing id or blob"
                            }
                        }})
                    else:
                        id = (d.id, self.current_branch)
                        file_sink.add(convert.object2JSON(id) + "\t" + convert.object2JSON(d) + "\n")
                        num_results += 1
                        perf_results.append(d)
        except Exception, e:
            Log.warning("Failure to GET from {{url}}", {"url": url}, e)
            return False

        # THIS STEP FORMATS DATA BACK TO TALOS ORIGINAL FORMAT
        try:
            for r in perf_results:
                try:
                    t = self.treeherder2talos(r, url)
                except Exception, e:
                    Log.note("CORRUPTED: perf_id {{perf_id}}  reason={{reason}}", {"perf_id": r.id, "reason": e.message})
                    es_sink.add({"value":{
                        "treeherder": {
                            "branch": self.current_branch,
                            "perf_id": r.id,
                            "corrupt_json": convert.object2JSON(r),
                            "url": url,
                            "reason": e.message
                        }
                    }})
                    continue

                id = (t.treeherder.perf_id, t.treeherder.branch)
                if DEBUG:
                    Log.println("Add {{id}} for revision {{revision}} ({{bytes}} bytes)", {
                        "id": id,
                        "revision": t.test_build.revision,
                        "bytes": len(convert.object2JSON(r))
                    })
                with Profiler("transform"):
                    result = transformer.transform(id, t)

                if result:
                    es_sink.extend({"value": d} for d in result)

            if num_results == 0:
                return False

            Log.println("{{num}} records added", {"num": num_results})

            return True
        except Exception, e:
            Log.warning("Failure to etl (content length={{length}})", {"length": len(content)}, e)
            return False

    def test_result_to_uid(self, test_result):
        return hashlib.sha1(uid_json_encoder.encode(json_scrub(test_result))).hexdigest()

    def get_existing_ids(self, es):
        #FIND WHAT'S IN ES
        int_ids = set()

        if self.settings.elasticsearch.debug and self.settings.treeherder.step < 10:
            # SIMPLY RELOAD THIS SMALL NUMBER
            return set([])

        with ESQuery(es) as esq:
            try:
                max_id = esq.query({
                    "from": es.settings.alias,
                    "select": {"value": "treeherder.perf_id", "aggregate": "max"},
                    "where": {"term": {"treeherder.branch": self.current_branch}}
                })
            except Exception, e:
                if e.contains("failed to find mapping for treeherder.perf_id"):  # HAPPENS DURING NEW INDEX AND NO TEST DATA
                    max_id = self.settings.treeherder.min
                elif e.contains("No mapping found for field [treeherder.perf_id]"):
                    max_id = self.settings.treeherder.min
                elif e.contains("does not have type"):
                    max_id = self.settings.treeherder.min
                else:
                    raise e

            max_id = MAX(max_id, self.settings.treeherder.min)

            es_interval_size = 200000
            for mini, maxi in Q.intervals(self.settings.treeherder.min, max_id, es_interval_size):
                existing_ids = es.search({
                    "query": {"match_all": {}},
                    "from": 0,
                    "size": 0,
                    "sort": [],
                    "facets": {
                        "ids": {
                            "terms": {
                                "field": "treeherder.perf_id",
                                "size": es_interval_size
                            },
                            "facet_filter": {"and": [
                                {"range": {"treeherder.perf_id": {"gte": mini, "lt": maxi}}},
                                {"term": {"treeherder.branch": self.current_branch}}
                            ]}
                        }
                    }
                })

                int_ids.update(existing_ids.facets.ids.terms.select("term"))

            existing_ids = int_ids
            Log.println("Number of ids in ES: {{num}}", {"num": len(existing_ids)})
            return existing_ids


    def extract_from_treeherder(self, es, transformer):
        existing_ids = self.get_existing_ids(es)

        #GET RANGE IN TREEHERDER
        url = expand_template(self.settings.treeherder.max_id_url, {"branch": self.current_branch})
        response = requests.get(url, timeout=self.settings.treeherder.timeout).content
        treeherder_max = convert.JSON2object(convert.utf82unicode(response)).max_performance_artifact_id
        treeherder_max = Math.min(treeherder_max, self.settings.treeherder.max)
        treeherder_min = Math.max(self.settings.treeherder.min, 0)
        holes = set(range(treeherder_min, nvl(Math.max(*existing_ids), treeherder_min))) - existing_ids
        missing_ids = set(range(treeherder_min, treeherder_max)) - existing_ids

        # https://treeherder.mozilla.org/api/project/mozilla-inbound/project_info
        # url = settings.treeherder.url+"/api/project/"+branch
        # project = requests.get(url).content

        Log.note("Max TreeHerder ID: {{max}}", {"max": treeherder_max})
        Log.note("Number missing: {{num}}", {"num": len(missing_ids)})
        Log.note("Number in holes: {{num}}", {"num": len(holes)})

        #FASTER IF NO INDEXING IS ON
        es.set_refresh_interval(-1)

        #COPY MISSING DATA TO ES
        try:
            with ThreadedQueue(es, size=nvl(es.settings.batch_size, 100)) as es_sink:
                with ThreadedQueue(File(self.settings.param.output_file), size=50) as file_sink:
                    simple_etl = functools.partial(self.etl, *[es_sink, file_sink, transformer])

                    num_not_found = 0
                    with Multithread(simple_etl, threads=self.settings.treeherder.threads) as many:
                        results = many.execute([
                            {"min_job_id": min_job_id, "max_job_id": max_job_id}
                            for i, (min_job_id, max_job_id) in cluster(missing_ids, self.settings.treeherder.step)
                        ])
                        for result in results:
                            if not result:
                                num_not_found += 1
                                if num_not_found > nvl(self.settings.treeherder.max_tries, 10):
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


def get_branches(settings):
    response = requests.get(settings.branches.url, timeout=nvl(settings.treeherder.timeout, 30))
    branches = convert.JSON2object(convert.utf82unicode(response.content))
    return wrap({branch.name: unwrap(branch) for branch in branches})


def cluster(values, max_size):
    """
    return min/max pairs (up to max_size) for contiguous integers
    """
    if not values:
        return

    count = 0
    mini = None
    maxi = None

    for v in Q.sort(values):
        if mini is None:
            mini = v
            maxi = v + 1
        elif maxi - mini >= max_size:
            yield count, (mini, maxi)
            mini = v
            maxi = v + 1
            count += 1
        elif v == maxi:
            maxi = v + 1
        else:
            yield count, (mini, maxi)
            mini = v
            maxi = v + 1
            count += 1
    yield count, (mini, maxi)



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
            settings.treeherder.timeout = nvl(settings.treeherder.timeout, 30)
            settings.treeherder.step = nvl(settings.treeherder.step, NUM_PER_BATCH)
            settings.treeherder.threads = nvl(settings.treeherder.threads, 1)
            settings.param.output_file = nvl(settings.param.output_file, "./results/raw_json_blobs.tab")

            worker = TreeHerderImport(settings)

            #GET BRANCHES
            branches = get_branches(settings.treeherder)
            #SETUP PUSHLOG PULLER
            hg = MozillaGraph(set_default(settings.mozillaHG, {"branches": branches}))
            transformer = Talos2ES(hg)

            #RESET ONLY IF NEW Transform IS USED
            if settings.args.restart:
                es = Cluster(settings.elasticsearch).create_index(settings.elasticsearch)
                es.add_alias()
                es.delete_all_but_self()
            else:
                es = Cluster(settings.elasticsearch).get_or_create_index(settings.elasticsearch)

            for b in branches.keys():
                if branches[b].dvcs_type != "hg":
                    continue

                try:
                    worker.current_branch = b
                    worker.extract_from_treeherder(es, transformer)
                except Exception, e:
                    Log.warning("Problem with import of {{branch}}", {"branch": b}, e)

    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


main()
