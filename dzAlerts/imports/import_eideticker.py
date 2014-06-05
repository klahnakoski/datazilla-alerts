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
from dzAlerts.util.cnv import CNV

from dzAlerts.util.env import startup
from dzAlerts.util.env.elasticsearch import ElasticSearch
from dzAlerts.util.env.logs import Log
from dzAlerts.util.queries import Q
from dzAlerts.util.queries.es_query import ESQuery
from dzAlerts.util.strings import expand_template
from dzAlerts.util.struct import wrap, StructList
from dzAlerts.util.times.timer import Timer


def get_all_uuid(settings):
    # SNAGGED FROM https://bug985985.bugzilla.mozilla.org/attachment.cgi?id=8415562
    baseurl = settings.url

    output = StructList()
    devices = requests.get(baseurl + '/devices.json')
    device_names = devices.json()['devices'].keys()
    for device_name in device_names:
        tests = requests.get(baseurl + '/%s/tests.json' % device_name)
        for testname, dummy in tests.json()['tests'].items():
            testdata = requests.get(baseurl + '/%s/%s.json' % (device_name, testname))
            for appname, appdata in testdata.json()['testdata'].items():
                for date, datedata in appdata.items():
                    Log.note("Pull data for device={{device}}, test={{test}}, app={{app}}, date={{date}}", {
                        "device": device_name,
                        "test": testname,
                        "app": appname,
                        "date": date,
                        "num": len(datedata)
                    })

                    for d in datedata:
                        metadata = {
                            "device": device_name,
                            "test": testname,
                            "app": appname,
                            "date": date,
                            "uuid": d["uuid"]
                        }
                        output.append(metadata)

    return output


def etl(settings):
    with Timer("get all uuid"):
        uuids = get_all_uuid(settings)

    es = ElasticSearch(settings.elasticsearch)
    sink = es.threaded_queue(size=100)

    #FILTER EXISTING IDS
    with ESQuery(es) as esq:
        existing = set()
        try:
            for g, metadata in Q.groupby(wrap(uuids), size=1000):
                batch = esq.query({
                    "from": settings.elasticsearch,
                    "select": "metadata.uuid",
                    "where": {"terms": {"metatdata.uuid": metadata.uuid}}
                })
                existing.update(batch)
        except Exception, e:
            Log.warning("can not access ES", e)

    #PULL ANY NEW STUFF
    for metadata in uuids:
        if metadata.uuid in existing:
            continue  # EXISTING STUFF IS SKIPPED
        result = requests.get(expand_template(settings.uuid_url, {"uuid": metadata.uuid}))
        data = wrap(result.json())
        data.metadata = metadata
        sink.add({"id": metadata.uuid, "value": data})

    sink.close()


if __name__ == '__main__':
    settings = startup.read_settings()
    Log.start(settings.debug)

    #FIX ANY POSSIBLE SETTINGS PROBLEMS
    settings.param.url = settings.param.url.rstrip("/")

    try:
        Log.note("Running alerts off of schema {{schema}}", {"schema": settings.perftest.schema})
        etl(settings.param)
    except Exception, e:
        Log.warning("Failure to run alerts", cause=e)
    finally:
        Log.stop()

