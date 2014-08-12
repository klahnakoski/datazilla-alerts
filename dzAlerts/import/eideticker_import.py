# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import unicode_literals
import requests

from dzAlerts.util.env import startup
from dzAlerts.util.env.elasticsearch import ElasticSearch
from dzAlerts.util.env.logs import Log
from dzAlerts.util.parsers import URL
from dzAlerts.util.queries.es_query import ESQuery
from dzAlerts.util.strings import expand_template
from dzAlerts.util.struct import wrap, StructList
from dzAlerts.util.thread.multithread import Multithread
from dzAlerts.util.times.timer import Timer


def get_all_uuid(settings):
    # SNAGGED FROM https://bug985985.bugzilla.mozilla.org/attachment.cgi?id=8415562
    baseurl = settings.url

    output = StructList()
    devices = requests.get(baseurl + '/devices.json').json()['devices']
    for device_name, device_info in devices.items():
        for branch in device_info["branches"]:
            url = "/".join((baseurl, device_name, branch, "tests.json"))
            tests = requests.get(url)
            if tests.status_code != 200:
                Log.warning("Can not find test for {{device}} (url={{url}})", {"device": device_name, "url": url})
                continue
            for testname, test_info in tests.json()['tests'].items():
                testdata = requests.get(baseurl + '/%s/%s/%s.json' % (device_name, branch, testname))
                for appname, appdata in testdata.json()['testdata'].items():
                    for date, date_data in appdata.items():
                        Log.note("Pull data for device={{device}}, test={{test}}, app={{app}}, date={{date}}", {
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

    return output


def etl(settings):
    es = ElasticSearch.get_or_create_index(settings.elasticsearch)

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
            try:
                response = requests.get(expand_template(settings.uuid_url, {"uuid": metadata.uuid}))
                data = wrap(response.json())
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
                Log.warning("problem getting details for {{slice}} reason = {{response}}", {
                    "slice": metadata,
                    "response": response
                }, e)

        with Multithread(get_uuid, threads=10, outbound=False, silent_queues=True) as multi:
            for metadata in all_tests:
                if metadata.uuid in existing:
                    continue  # EXISTING STUFF IS SKIPPED
                multi.inbound.add({"metadata": metadata})


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

