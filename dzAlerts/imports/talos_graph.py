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

from dzAlerts.imports.mozilla_graph import MozillaGraph
from pyLibrary.collections import UNION
from pyLibrary.graphs import Graph
from pyLibrary.graphs.algorithms import dfs
from pyLibrary.queries import Q


class TalosGraph(object):
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


