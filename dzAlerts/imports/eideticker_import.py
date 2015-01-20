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
import requests

from pyLibrary.debugs import startup
from pyLibrary.env.elasticsearch import Cluster

from pyLibrary.debugs.logs import Log
from pyLibrary.parsers import URL
from pyLibrary.queries.es_query import ESQuery
from pyLibrary.strings import expand_template
from pyLibrary.dot import nvl, Dict, wrap
from pyLibrary.dot.lists import DictList
from pyLibrary.thread.multithread import Multithread
from pyLibrary.times.timer import Timer


DEBUG_SHOW_METADATA = False


def get_all_uuid(settings):
    # SNAGGED FROM https://bug985985.bugzilla.mozilla.org/attachment.cgi?id=8415562
    num_requests = 0
    baseurl = settings.url

    output = DictList()
    devices = requests.get(baseurl + '/devices.json').json()['devices']
    num_requests+=1
    for device_name, device_info in devices.items():
        for branch in device_info["branches"]:
            url = "/".join((baseurl, device_name, branch, "tests.json"))
            tests = requests.get(url)
            num_requests+=1
            if tests.status_code != 200:
                Log.warning("Can not find test for {{device}} because of {{response.status_code}} {{response.reason}} (url={{url}})", {
                    "device": device_name,
                    "url": url,
                    "response": tests
                })
                continue
            for testname, test_info in tests.json()['tests'].items():
                testdata = requests.get(baseurl + '/%s/%s/%s.json' % (device_name, branch, testname))
                num_requests+=1
                try:
                    apps = testdata.json()['testdata']
                    for appname, appdata in apps.items():
                        for date, date_data in appdata.items():
                            if DEBUG_SHOW_METADATA:
                                Log.note("Pull data for device={{device}}, test={{test}}, app={{app}}, date={{date}}, num={{num}}", {
                                    "device": device_name,
                                    "branch": branch,
                                    "test": testname,
                                    "app": appname,
                                    "date": int(date)*1000,
                                    "num": len(date_data)
                                })

                            for d in date_data:
                                metadata = {
                                    "device": device_name,
                                    "branch": branch,
                                    "test": testname,
                                    "app": appname,
                                    "date": int(date)*1000,
                                    "uuid": d["uuid"],
                                    "path": URL(settings.url).path.rstrip("/")
                                }
                                output.append(metadata)
                except Exception, e:
                    Log.warning("problem with json", e)
    Log.note("{{num}} requests to pull metadata", {"num": num_requests})
    return output


def etl(settings):
    es = Cluster(settings.elasticsearch).get_or_create_index(settings.elasticsearch)
    counter = Dict(num_requests=0)

    with Timer("get all uuid"):
        all_tests = get_all_uuid(settings)

    # FILTER EXISTING IDS
    with ESQuery(es) as esq:
        existing = set()
        batch = esq.query({
            "from": settings.elasticsearch,
            "select": "metadata.uuid",
            "limit": 200000
        })
        existing.update(batch)

    new_stuff = set(all_tests.uuid) - existing
    Log.note("{{total}} tests: {{new}} new, {{old}} exist", {
        "total": len(all_tests),
        "new": len(new_stuff),
        "old": len(existing)
    })

    # PULL ANY NEW STUFF
    with es.threaded_queue(size=100) as sink:
        def get_uuid(metadata):
            response = None
            url = expand_template(settings.uuid_url, {"uuid": metadata.uuid})
            try:
                response = requests.get(url)
                counter.num_requests += 1
                try:
                    data = wrap(response.json())
                except Exception, e:
                    Log.note("Calling\n{{url|indent}}\nfor\n{{slice|indent}}\ngives {{status_code}} {{reason}}", {
                        "uuid": metadata.uuid,
                        "slice": metadata,
                        "status_code": response.status_code,
                        "reason": response.reason,
                        "url": url
                    })
                    return
                data.metadata = metadata
                # FIND DEFAULT VALUE TO TRACK
                if data.test_info.defaultMeasureId:
                    pass
                elif data.metrics.timetostableframe:
                    data.test_info.defaultMeasureId = "timetostableframe"
                elif data.metrics.checkerboard:
                    data.test_info.defaultMeasureId = "checkerboard"
                elif data.metrics.overallentropy:
                    data.test_info.defaultMeasureId = "overallentropy"
                elif data.metrics.fps:
                    data.test_info.defaultMeasureId = "fps"
                elif len(data.metrics.keys()) == 1:
                    data.test_info.defaultMeasureId = data.metrics.keys()[0]
                data.metadata.value = data.metrics[data.test_info.defaultMeasureId]

                sink.add({"id": metadata.uuid, "value": data})
            except Exception, e:
                Log.warning("problem getting details for {{slice}} url = {{url}} reason = {{response}}", {
                    "slice": metadata,
                    "url": url,
                    "response": response
                }, e)

        with Timer("get data") as timer:
            with Multithread(get_uuid, threads=nvl(settings.threads, 10), outbound=False, silent_queues=True) as multi:
                for metadata in all_tests:
                    if metadata.uuid in existing:
                        continue  # EXISTING STUFF IS SKIPPED
                    multi.inbound.add({"metadata": metadata})

        Log.note("Got data: {{num}} requests made over {{duration}}", {
            "num": counter.num_requests,
            "duration": timer.duration
        })


if __name__ == '__main__':
    settings = startup.read_settings()
    Log.start(settings.debug)

    # FIX ANY POSSIBLE SETTINGS PROBLEMS
    settings.param.url = settings.param.url.rstrip("/")

    try:
        Log.note("Running alerts off of schema {{schema}}", {"schema": settings.perftest.schema})
        etl(settings.param)
    except Exception, e:
        Log.warning("Failure to run alerts", cause=e)
    finally:
        Log.stop()

