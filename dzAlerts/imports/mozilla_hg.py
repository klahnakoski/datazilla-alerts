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
from BeautifulSoup import BeautifulSoup
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

    NODES ARE Structs
    """

    def __init__(self, settings):
        self.settings = wrap(settings)
        self.settings.timeout = Duration(nvl(self.settings.timeout, "30second"))
        self.nodes = {}  # DUMB CACHE FROM (branch, changeset_id) TO REVISOIN
        self.pushes = {}  # MAP FROM (branch, changeset_id) TO Push

    def get_node(self, revision):
        """
        EXPECTING revision TO BE A TUPLE OF (branch_name, changeset_id)
        """
        revision.branch = self.settings.branches[revision.branch.name.lower()]
        if revision in self.nodes:
            return self.nodes[revision]

        try:
            details = _read_revision(revision.branch.url + "/rev/" + revision.changeset.id, self.settings)
            details.branch = revision.branch

            self.nodes[revision] = details
            return details
        except Exception, e:
            return None

    def get_push(self, revision):
        # http://hg.mozilla.org/mozilla-central/json-pushes?full=1&changeset=57c461500a0c
        if revision in self.pushes:
            return self.pushes[revision]

        url = revision.branch.url + "/json-pushes?full=1&changeset=" + revision.changeset.id
        response = requests.get(url, timeout=self.settings.timeout.seconds).content
        data = CNV.JSON2object(response.decode("utf8"))
        for index, _push in data.items():
            push = Push(index, revision.branch, _push.date, _push.user)
            for c in _push.changesets:
                changeset = Changeset(id=c.node, **unwrap(c))
                rev = self.get_node(Revision(revision.branch, changeset))
                rev.push = push
                self.pushes[rev] = push
                push.changesets.append(changeset)

        return self.pushes[revision]


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


def _read_revision(url, settings):
    """
    READ THE HTML OF THE REVISION

    <div class="page_header">
    <a href="http://developer.mozilla.org/en/docs/Mercurial" title="Mercurial" style="float: right;">Mercurial</a><a href="/integration/mozilla-inbound/summary">mozilla-inbound</a> - changeset - 211512:cbfb7abec255
    </div>

    <div class="title_text">
    <table cellspacing="0">
    <tr><td>author</td><td>&#65;&#97;&#114;&#111;&#110;&#32;&#75;&#108;&#111;&#116;&#122;&#32;&#60;&#97;&#107;&#108;&#111;&#116;&#122;&#64;&#109;&#111;&#122;&#105;&#108;&#108;&#97;&#46;&#99;&#111;&#109;&#62;</td></tr>
    <tr><td></td><td>Tue Oct 21 12:18:27 2014 -0600 (at Tue Oct 21 12:18:27 2014 -0600)</td></tr>

    <tr><td>changeset 211512</td><td style="font-family:monospace">cbfb7abec255</td></tr>
    <tr><td>parent 211511</td><td style="font-family:monospace"><a class="list" href="/integration/mozilla-inbound/rev/d6721fea9ad9">d6721fea9ad9</a></td></tr>
    <tr><td>child 211513</td><td style="font-family:monospace"><a class="list" href="/integration/mozilla-inbound/rev/911b01751ad5">911b01751ad5</a></td></tr>
    <tr><td>pushlog:</td><td><a href="/integration/mozilla-inbound/pushloghtml?changeset=cbfb7abec255">cbfb7abec255</a></td></tr>
    </table></div>
    """
    try:
        children = []
        parents = []
        index = None
        changeset_id = None
        author = None

        Log.note("Reading details for from {{url}}", {"url": url})

        response = requests.get(url, timeout=settings.timeout.seconds)
        html = BeautifulSoup(response.content)

        branch = html.find(**{"class": "page_header"}).findAll("a")[1].string
        message = html.find(**{"class": "page_body"}).getText()

        rows = html.find(**{"class": "title_text"}).findAll("tr")
        for r in rows:
            tds = r.findAll("td")
            name = tds[0].getText()
            if name.startswith("parent"):
                link = tds[1].a["href"]
                parents.append(link.split("/")[-1])
            elif name.startswith("child"):
                link = tds[1].a["href"]
                children.append(link.split("/")[-1])
            elif name.startswith("changeset"):
                index = int(name.split(" ")[-1])
                changeset_id = tds[1].getText()
            elif name.startswith("author"):
                author = CNV.html2unicode(tds[1].getText())

        return Revision(
            branch=branch,
            index=index,
            changeset={
                "id": changeset_id,
                "author": author,
                "message": message
            },
            parents=parents,
            children=children
        )
    except Exception, e:
        Log.error("Can not get revision info", e)


