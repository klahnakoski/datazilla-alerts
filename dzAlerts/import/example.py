#!/usr/bin/env python

import optparse
import requests
import sys

usage = "usage: %prog [options] <url> <output directory>"
parser = optparse.OptionParser(usage)
parser.add_option("--full-mirror", action="store_true",
                  default=False, dest="full_mirror",
                  help="Download videos, profiles to disk")
parser.add_option("--skip-metadata", action="store_false",
                  dest="download_metadata", default=True,
                  help="Skip downloading metadata JSON files")
options, args = parser.parse_args()

if len(sys.argv) != 2:
    print "Usage: %s <url>" % sys.argv[0]
    parser.print_usage()
    sys.exit(1)

baseurl = sys.argv[1]
if baseurl[-1] != '/':
    baseurl += '/'

devices = requests.get(baseurl + 'devices.json')

device_names = devices.json()['devices'].keys()

for device_name in device_names:
    print device_name
    tests = requests.get(baseurl + '%s/tests.json' % device_name)
    testnames = tests.json()['tests'].keys()
    for testname in testnames:
        print testname
        r = requests.get(baseurl + '%s/%s.json' % (device_name, testname))
        testdata = r.json()['testdata']
        for appname in testdata.keys():
            print appname
            for date in sorted(testdata[appname].keys()):
                print date
                for datapoint in testdata[appname][date]:
                    print datapoint['uuid']
                    # time to stable frame for startup tests
                    if datapoint.get('timetostableframe'):
                        print "stableframe: %s" % datapoint['timetostableframe']
                    # overall entropy for scrolling tests
                    if datapoint.get('overallentropy'):
                        print "overall entropy: %s" % datapoint['overallentropy']
