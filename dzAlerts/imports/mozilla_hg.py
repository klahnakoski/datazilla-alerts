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

from test.test_deque import Deque
import requests
from dzAlerts.imports.repos.changesets import Changeset
from dzAlerts.imports.repos.pushs import Push
from dzAlerts.imports.repos.revisions import Revision
from pyLibrary.cnv import CNV
from pyLibrary.collections import UNION
from pyLibrary.env.logs import Log
from pyLibrary.queries import Q
from pyLibrary.struct import Struct, nvl
from pyLibrary.structs.wraps import unwrap, wrap
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import Duration


class MozillaHG(object):
    def __init__(self, repo_settings):
        self.repo_graph = MozillaGraph(repo_settings)
        self.test_graph = Graph()
        self.pushlog = {}   # MAP FROM changeset_id TO Push

    def add_test_result(self, test_result):
        # ANNOTATE test_result WITH UNIQUE CHANGESETS AND
        # TOTAL ORDERING
        previous_tests = self._refresh(test_result)

        for t in previous_tests:
            self.test_graph.add_edge((test_result, t))

        #LOOK FOR LATER TESTS (SHOULD THIS BE A RE-TRIGGER?)
        future_tests = []

        def g(revision, path, graph):
            if revision in self.test_graph.nodes:
                future_tests.append(self.test_graph.nodes[revision])
                return False
            else:
                return True

        dfs(self.repo_graph, g, test_result.revision, reverse=True)

        for f in future_tests:
            self.test_graph.remove_children(f)
            self._refresh(f)

    def _refresh(self, test_result):
        #ANNOTATE test_result WITH UNIQUE CHANGESETS
        previous_tests = []   # TESTS THAT DOMINATE THIS REVISION
        revisions = []      # ALL THE REVISIONS

        def f(revision, path, graph):
            revisions.append(revisions)
            if revision in self.test_graph.nodes:
                previous_tests.append(self.test_graph.nodes[revision])
                return False
            else:
                return True

        dfs(self.repo_graph, f, test_result.revision)
        test_result.changesets = set([r.changeset for r in revisions]) - UNION(p.changesets for p in previous_tests)
        for t in previous_tests:
            self.test_graph.add_edge((test_result, t))
        test_result.ordering = self.repo_graph.node({""})

        return previous_tests

    def get_ordering(self, test_result):
        revision = self.repo_graph.get_node(test_result.revision)
        children = self.repo_graph.get_children(revision)
        children = Q.sort(children, ["push.date", "index"])


class Graph(object):
    def __init__(self, node_type=None):
        self.nodes = []
        self.edges = []
        self.node_type = node_type


    def add_edge(self, edge):
        self.edges.append(edge)

    def remove_children(self, node):
        self.edges = [e for e in self.edges if e[0] != node]

    def get_children(self, node):
        #FIND THE REVISION
        #
        return [c for p, c in self.edges if p == node]

    def get_parents(self, node):
        return [p for p, c in self.edges if c == node]

    def get_edges(self, node):
        return [(p, c) for p, c in self.edges if p == node or c == node]

    def get_family(self, node):
        """
        RETURN ALL ADJACENT NODES
        """
        return set([p if c == node else c for p, c in self.edges])

# class CacheGraph(object):
#     def __init__(self, fast, slow):
#         """
#         INVARIANTS:
#         ANY NODE IN fast HAS ALL ITS IMMEDIATE FAMILY IN fast, OR THE frontier
#         ANY NODE IN THE frontier MAY HAVE FAMILY IN slow AND EDGES MISSING IN fast
#
#         """
#         self.frontier = set()    # FAST NODES WHO'S NEIGHBOURS MAY NOT BE FAST
#         self.fast = fast
#         self.slow = slow
#
#     def get_children(self, node):
#         if node not in self.fast.nodes:
#             self._add_fast_node(node)
#         return self.fast.get_children(node)
#
#     def get_parents(self, node):
#         if node not in self.fast.nodes:
#             self._add_fast_node(node)
#         return self.fast.get_parents(node)
#
#     def _add_fast_node(self, node):
#         # MAKE A FAST NODE FROM THE SLOW NODE INFO
#         # ENSURE ALL ADJACENT NODES ARE ALSO BROUGHT IN
#         if node in self.fast.nodes:
#             Log.error("should not have called")
#
#         if node not in self.frontier:
#             Log.error("should not have called")
#
#         #SINCE IT IS IN THE FRONTIER, THE FAST FAMILY MAY NOT BE COMPLETE
#         slow_node_info = self.slow.get_node(node)
#         i = node.changeset.id
#         all_family = set(slow_node_info.children + slow_node_info.parents)
#         fast_family = {r.changeset.id: r for r in self.fast.get_family(node)}
#         slow_family = all_family - set(fast_family.keys())
#
#         self.frontier.discard(node)
#         self.fast.nodes.add(node)
#         for p in slow_node_info.parents:
#             if p in slow_family:
#                 new_node = self.fast.get_node(p)
#                 self.frontier.add(new_node)
#                 self.fast.edges.append((new_node, node))
#             else:
#                 self.fast.edges.append((fast_family[p], node))
#         for p, c in slow_node_info.children:
#             if c in slow_family:
#                 new_node = self.fast.get_node(c)
#                 self.fast.edges.append((node, new_node))
#                 self.frontier.add(new_node)
#             else:
#                 self.fast.edges.append((node, fast_family[c]))
#


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


def dfs(graph, func, head, reverse=None):
    """
    RUN DFS SEARCH, IF func RETURNS FALSE, THEN CONSIDER node AT THE END
    OF THE LINE

    IT'S EXPECTED func TAKES 3 ARGUMENTS
    node - THE CURRENT NODE IN THE
    path - PATH FROM head TO node
    graph - THE WHOLE GRAPH
    """
    todo = Deque()
    todo.append(head)
    path = Deque()
    done = set()
    while todo:
        node = todo.popleft()
        if node in done:
            path.pop()
            continue

        done.add(node)
        path.append(node)
        result = func(node, path, graph)
        if result:
            if reverse:
                children = graph.get_parents(node)
            else:
                children = graph.get_children(node)
            todo.extend(children)


def dominator(graph, head):
    # WE WOULD NEED DOMINATORS IF WE DO NOT KNOW THE TOPOLOGICAL ORDERING
    # DOMINATORS ALLOW US TO USE A REFERENCE TEST RESULT: EVERYTHING BETWEEN
    # dominator(node) AND head CAN BE TREATED AS PARALLEL-APPLIED CHANGESETS
    #
    # INSTEAD OF DOMINATORS, WE COULD USE MANY PERF RESULTS, FROM EACH OF THE
    # PARENT BRANCHES, AND AS LONG AS THEY ALL ARE PART OF A LONG LINE OF
    # STATISTICALLY IDENTICAL PERF RESULTS, WE CAN ASSUME THEY ARE A DOMINATOR
    pass

