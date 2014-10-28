# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copied (October 2014) from http://hg.mozilla.org/graphs/file/393fa09a1b1b/server/analysis/analyze_talos.py
#
# Modifications: Kyle Lahnakoski (kyle@lahnakoski.com)
import json
import os
import urllib2
from pyLibrary.env.logs import Log


class PushLog:
    def __init__(self, filename, base_url):
        self.filename = filename
        self.base_url = base_url
        self.pushes = {}   # EG self.pushes[branch][shortrev]

    def load(self):
        try:
            if not os.path.exists(self.filename):
                self.pushes = {}
                return
            self.pushes = json.load(open(self.filename))
        except:
            Log.error("Couldn't load push dates from %s", self.filename)
            self.pushes = {}

    def save(self):
        tmp = self.filename + ".tmp"
        json.dump(self.pushes, open(tmp, "w"), indent=2, sort_keys=True)
        os.rename(tmp, self.filename)

    def _handleJson(self, branch, data):
        if isinstance(data, dict):
            for push in data.values():
                pusher = push['user']
                for change in push['changesets']:
                    shortrev = change["node"][:12]
                    self.pushes[branch][shortrev] = {
                        "date": push['date'],
                        "comments": change['desc'],
                        "author": change['author'],
                        "pusher": pusher,
                    }

    def getPushDates(self, branch, repo_path, changesets):
        to_query = []
        retval = {}
        if branch not in self.pushes:
            self.pushes[branch] = {}

        for c in changesets:
            # Pad with zeros to work around bug where revisions with leading
            # zeros have it stripped
            while len(c) < 12:
                c = "0" + c
            shortrev = c[:12]
            if shortrev not in self.pushes[branch]:
                to_query.append(c)
            else:
                retval[c] = self.pushes[branch][shortrev]['date']

        if len(to_query) > 0:
            Log.note("Fetching %i changesets", len(to_query))
            for i in range(0, len(to_query), 50):
                chunk = to_query[i:i + 50]
                changesets = ["changeset=%s" % c for c in chunk]
                base_url = self.base_url
                url = "%s/%s/json-pushes?full=1&%s" % (base_url, repo_path, "&".join(changesets))
                try:
                    raw_data = urllib2.urlopen(url, timeout=300).read()
                except:
                    Log.error("Error fetching %s", url)
                    continue

                try:
                    data = json.loads(raw_data)
                    self._handleJson(branch, data)
                except:
                    Log.error("Error parsing %s", raw_data)
                    raise

                for c in chunk:
                    shortrev = c[:12]
                    try:
                        retval[c] = self.pushes[branch][shortrev]['date']
                    except KeyError:
                        Log.note("%s not found in push data", shortrev)
                        continue
        return retval

    def getPushRange(self, branch, repo_path, from_, to_):
        key = "%s-%s" % (from_, to_)
        if branch not in self.pushes:
            self.pushes[branch] = {"ranges": {}}
        elif "ranges" not in self.pushes[branch]:
            self.pushes[branch]["ranges"] = {}
        elif key in self.pushes[branch]["ranges"]:
            return self.pushes[branch]["ranges"][key]

        Log.note("Fetching changesets from %s to %s", from_, to_)
        base_url = self.base_url
        url = "%s/%s/json-pushes?full=1&fromchange=%s&tochange=%s" % (base_url, repo_path, from_, to_)
        try:
            raw_data = urllib2.urlopen(url, timeout=300).read()
        except:
            Log.error("couldn't fetch %s", url)
            return []

        try:
            data = json.loads(raw_data)
            self._handleJson(branch, data)
            retval = []
            pushes = data.items()
            pushes.sort(key=lambda p: p[1]['date'])
            for push_id, push in pushes:
                for c in push['changesets']:
                    retval.append(c['node'][:12])
            self.pushes[branch]["ranges"][key] = retval
            return retval
        except:
            Log.error("Error parsing %s", raw_data)
            return []

    def getChange(self, branch, rev):
        shortrev = rev[:12]
        return self.pushes[branch][rev]





