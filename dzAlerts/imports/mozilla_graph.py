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
from dzAlerts.imports.repos.changesets import Changeset
from dzAlerts.imports.repos.pushs import Push
from dzAlerts.imports.repos.revisions import Revision
from pyLibrary.cnv import CNV
from pyLibrary.env.logs import Log
from pyLibrary.struct import nvl
from pyLibrary.structs.wraps import unwrap, wrap
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import Duration


class MozillaGraph(object):
    """
    VERY SLOW, PURE hg.moziila.org GRAPH IMPLEMENTATION
    """

    def __init__(self, settings):
        self.settings = wrap(settings)
        self.settings.timeout = Duration(nvl(self.settings.timeout, "30second"))
        self.nodes = {}  # DUMB CACHE FROM (branch, changeset_id) TO REVISOIN
        self.pushes = {}  # MAP FROM (branch, changeset_id) TO Push

    def get_node(self, revision):
        """
        EXPECTING INCOMPLETE revision
        RETURNS revision
        """
        revision.branch = self.settings.branches[revision.branch.name.lower()]
        if revision in self.nodes:
            return self.nodes[revision]

        try:
            url = revision.branch.url + "/json-info?node=" + revision.changeset.id
            Log.note("Reading details for from {{url}}", {"url": url})

            response = requests.get(url, timeout=self.settings.timeout.seconds)
            revs = CNV.JSON2object(response.content.decode("utf8"))

            if len(revs.keys()) != 1:
                Log.error("Do not know how to handle")

            r = list(revs.values())[0]
            output = Revision(
                branch=revision.branch,
                index=r.rev,
                changeset=Changeset(
                    id=r.node,
                    author=r.user,
                    description=r.description,
                    date=Date(r.date).milli
                ),
                parents=r.parents,
                children=r.children,
                files=r.files
            )
            self.nodes[revision]=revision
            return output
        except Exception, e:
            Log.error("Can not get revision info", e)

    def get_push(self, revision):
        # http://hg.mozilla.org/mozilla-central/json-pushes?full=1&changeset=57c461500a0c
        if revision not in self.pushes:
            Log.note("Reading pushlog for revision ({{branch}}, {{changeset}})", {
                "branch": revision.branch.name,
                "changeset": revision.changeset.id
            })

            url = revision.branch.url + "/json-pushes?full=1&changeset=" + revision.changeset.id
            response = requests.get(url, timeout=self.settings.timeout.seconds).content
            data = CNV.JSON2object(response.decode("utf8"))
            for index, _push in data.items():
                push = Push(index, revision.branch, _push.date, _push.user)
                for c in _push.changesets:
                    changeset = Changeset(id=c.node, **unwrap(c))
                    rev = Revision(branch=revision.branch, changeset=changeset)
                    self.pushes[rev] = push
                    push.changesets.append(changeset)

        push = self.pushes[revision]
        revision.push = push
        return push

    def get_children(self, revision):
        return self._get_adjacent(revision, "children")

    def get_parents(self, revision):
        return self._get_adjacent(revision, "parents")

    def get_edges(self, revision):
        output = []
        for c in self.get_children(revision):
            output.append((revision, c))
        for p in self.get_parents(revision):
            output.append((p, revision))
        return output

    def get_family(self, revision):
        return set(self.get_children(revision) + self.get_parents(revision))

    def _get_adjacent(self, revision, subset):
        revision = self.get_node(revision)
        if not revision[subset]:
            return []
        elif len(revision[subset]) == 1:
            return [self.get_node(Revision(branch=revision.branch, changeset=c)) for c in revision[subset]]
        else:
            #MULTIPLE BRANCHES ARE A HINT OF A MERGE BETWEEN BRANCHES
            output = []
            for branch in self.settings.branches.values():
                for c in revision[subset]:
                    node = self.get_node(Revision(branch=branch, changeset=c))
                    if node:
                        output.append(node)
            return output

