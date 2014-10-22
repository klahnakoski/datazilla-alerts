from test.test_deque import Deque
from BeautifulSoup import BeautifulSoup
import requests
from dzAlerts.util.collections import UNION
from dzAlerts.util.env.logs import Log
from dzAlerts.util.parsers import URL
from dzAlerts.util.struct import nvl, Struct


class MozillaHG(object):

    def __init__(self, repo_settings):
        self.repo_graph=MozillaGraph(repo_settings)
        self.test_graph=Graph()

    def add_test_result(self, test_result):
        #ANNOTATE test_result WITH UNIQUE CHANGESETS
        previous_tests=self._refresh(test_result)

        for t in previous_tests:
            self.test_graph.add_edge((test_result, t))

        #LOOK FOR LATER TESTS (SHOULD THIS BE A RE-TRIGGER)
        future_tests=[]
        def g(revision, path, graph):
            if revision in self.test_graph.nodes:
                future_tests.append(self.test_graph.nodes[revision])
                return False
            else:
                return True
        self.repo_graph.dfs(g, test_result.revision, reverse=True)

        for f in future_tests:
            self.test_graph.remove_children(f)
            self._refresh(f)

    def _refresh(self, test_result):
        #ANNOTATE test_result WITH UNIQUE CHANGESETS
        previous_tests=[]   # TESTS THAT DOMINATE THIS REVISION
        revisions = []      # ALL THE REVISIONS
        def f(revision, path, graph):
            revisions.append(revisions)
            if revision in self.test_graph.nodes:
                previous_tests.append(self.test_graph.nodes[revision])
                return False
            else:
                return True

        self.repo_graph.dfs(f, test_result.revision)
        test_result.changesets = set([r.changeset for r in revisions]) - UNION(p.changesets for p in previous_tests)
        for t in previous_tests:
            self.test_graph.add_edge((test_result, t))
        return previous_tests



class Graph(object):

    def __init__(self, node_type=None):
        self.nodes=[]
        self.edges=[]
        self.node_type = node_type


    def add_edge(self, edge):
        self.edges.append(edge)

    def remove_children(self, node):
        self.edges = [e for e in self.edges if e[0]!=node]

    def get_children(self, node):
        #FIND THE REVISION
        #
        return [c for p,c in self.edges if p==node]

    def get_parents(self, node):
        return [p for p,c in self.edges if c==node]


    def dfs(self, func, head, reverse=None):
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
            result = func(node, path, self)
            if result:
                if reverse:
                    children = self.get_parents(node)
                else:
                    children = self.get_children(node)
                todo.extend(children)




class CacheGraph(object):

    def __init__(self, fast, slow):
        self.fast=fast
        self.slow=slow

    def get_children(self, node):
        if node not in self.fast.nodes:
            self._add_fast_node(node)
        return self.fast.get_children(node)

    def _add_fast_node(self, slow_node):
        # MAKE A FAST NODE FROM THE SLOW NODE INFO
        slow_node_info = self.slow.get_node(slow_node)
        fast_node = self.fast.node_type(**slow_node_info)
        self.fast.add_node(fast_node)
        for e in slow_node_info.edges:
            self.fast.add_edge((
                nvl(self.fast.get_node(e[0]), e[0]),
                nvl(self.fast.get_node(e[1]), e[1]),
            ))

    def get_parents(self, node):
        if node not in self.fast.nodes:
            self._add_fast_node(node)
        return self.fast.get_parents(node)



class MozillaGraph(object):

    def __init__(self, settings):
        self.settings = settings


    def get_children(self, revision):
        #FIND THE REVISION

        url = self.settings.branch[revision.branch].url
        revision = _read_revision(url+"/rev/"+revision.changeset.id)
        +-


    def get_parents(self, revision):
        pass







class TestResult(object):

    def __init__(self, revision, test_results):
        self.revision = revision
        self.test_results = test_results
        self.changesets = []    # CHANGESETS THAT ARE UNIQUE TO THIS TEST RESULT


class Revision(object):

    def __init__(self, index, changeset_id, branch):
        self.index = index
        self.changeset_id = id
        self.branch = branch



class Changeset(object):
    def __init__(self, changeset_id, push_date, branch):
        self.id = changeset_id
        self.push_date = push_date
        self.branch = branch






def _read_revision(self, url):
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
        parents = []
        children = []
        index = None
        changeset_id = None
        author = None

        response = requests.get(url, timeout=self.settings.timeout)
        html = BeautifulSoup(response)
        rows = html.find_all(class_="title_text").tr
        for r in rows:
            name = r.td[0].get_text()
            link = r.td[1].a["href"]
            if name.startswith("parent"):
                parents.append(link.split("/")[-1])
            elif name.startswith("child"):
                children.append(link.split("/")[-1])
            elif name.startswith("changeset"):
                index = int(name.split(" ")[-1])
                changeset_id = r.td[1].get_text()
            elif name.startswith("author"):
                author = r.td[1].get_text()

        branch = html.find(class_="page_header").a.get_text()
        message = html.find_all(class_="page_body")[0].get_text()

        return Struct(
            branch=branch,
            index=index,
            changeset_id=changeset_id,
            author=author,
            parents=parents,
            children=children,
            changeset_id=changeset_id,
            message=message
        )
    except Exception, e:
        Log.error("Can not get revision info", e)
