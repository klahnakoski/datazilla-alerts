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
from copy import deepcopy

from datetime import datetime
import re
import time
import requests

from pyLibrary.collections import OR
from pyLibrary import convert
from pyLibrary.env.logs import Log
from pyLibrary.maths.randoms import Random
from pyLibrary.maths import Math
from pyLibrary.queries import Q
from pyLibrary.strings import utf82unicode
from pyLibrary.structs import nvl, Null, Struct
from pyLibrary.structs.lists import StructList
from pyLibrary.structs.wraps import wrap, unwrap
from pyLibrary.thread.threads import ThreadedQueue


DEBUG = False


class Index(object):
    """
    AN ElasticSearch INDEX LIFETIME MANAGEMENT TOOL

    ElasticSearch'S REST INTERFACE WORKS WELL WITH PYTHON AND JAVASCRIPT
    SO HARDLY ANY LIBRARY IS REQUIRED.  IT IS SIMPLER TO MAKE HTTP CALLS
    DIRECTLY TO ES USING YOUR FAVORITE HTTP LIBRARY.  I HAVE SOME
    CONVENIENCE FUNCTIONS HERE, BUT IT'S BETTER TO MAKE YOUR OWN.

    THIS CLASS IS TO HELP DURING ETL, CREATING INDEXES, MANAGING ALIASES
    AND REMOVING INDEXES WHEN THEY HAVE BEEN REPLACED.  IT USES A STANDARD
    SUFFIX (YYYYMMDD-HHMMSS) TO TRACK AGE AND RELATIONSHIP TO THE ALIAS,
    IF ANY YET.

    """

    def __init__(self, settings):
        """
        settings.explore_metadata == True - IF PROBING THE CLUSTER FOR METATDATA IS ALLOWED
        settings.timeout == NUMBER OF SECONDS TO WAIT FOR RESPONSE, OR SECONDS TO WAIT FOR DOWNLOAD (PASSED TO requests)
        """
        if settings.index == settings.alias:
            Log.error("must have a unique index name")

        settings = wrap(settings)
        assert settings.index
        assert settings.type
        settings.setdefault("explore_metadata", True)

        self.debug = nvl(settings.debug, DEBUG)
        globals()["DEBUG"] = OR(self.debug, DEBUG)
        if self.debug:
            Log.note("elasticsearch debugging is on")

        self.settings = settings
        self.cluster = Cluster(settings)

        try:
            index = self.get_index(settings.index)
            if index and settings.alias==None:
                settings.alias = settings.index
                settings.index = index
        except Exception, e:
            # EXPLORING (get_metadata()) IS NOT ALLOWED ON THE PUBLIC CLUSTER
            pass

        self.path = "/" + settings.index + "/" + settings.type


    def get_schema(self):
        if self.settings.explore_metadata:
            indices = self.cluster.get_metadata().indices
            index = indices[self.settings.index]
            if not index.mappings[self.settings.type]:
                Log.error("ElasticSearch index ({{index}}) does not have type ({{type}})", self.settings)
            return index.mappings[self.settings.type]
        else:
            mapping = self.cluster.get(self.path + "/_mapping")
            if not mapping[self.settings.type]:
                Log.error("{{index}} does not have type {{type}}", self.settings)
            return wrap({"mappings": mapping[self.settings.type]})

    def delete_all_but_self(self):
        """
        DELETE ALL INDEXES WITH GIVEN PREFIX, EXCEPT name
        """
        prefix = self.settings.alias
        name = self.settings.index

        if prefix == name:
            Log.note("{{index_name}} will not be deleted", {"index_name": prefix})
        for a in self.cluster.get_aliases():
            # MATCH <prefix>YYMMDD_HHMMSS FORMAT
            if re.match(re.escape(prefix) + "\\d{8}_\\d{6}", a.index) and a.index != name:
                self.cluster.delete_index(a.index)

    def add_alias(self):
        self.cluster_metadata = None
        self.cluster._post(
            "/_aliases",
            data=convert.unicode2utf8(convert.object2JSON({
                "actions": [
                    {"add": {"index": self.settings.index, "alias": self.settings.alias}}
                ]
            })),
            timeout=nvl(self.settings.timeout, 30)
        )

    def get_proto(self, alias):
        """
        RETURN ALL INDEXES THAT ARE INTENDED TO BE GIVEN alias, BUT HAVE NO
        ALIAS YET BECAUSE INCOMPLETE
        """
        output = sort([
            a.index
            for a in self.cluster.get_aliases()
            if re.match(re.escape(alias) + "\\d{8}_\\d{6}", a.index) and not a.alias
        ])
        return output

    def get_index(self, alias):
        """
        RETURN THE INDEX USED BY THIS alias
        """
        output = sort([
            a.index
            for a in self.cluster.get_aliases()
            if a.alias == alias or
                a.index == alias or
               (re.match(re.escape(alias) + "\\d{8}_\\d{6}", a.index) and a.index != alias)
        ])

        if len(output) > 1:
            Log.error("only one index with given alias==\"{{alias}}\" expected", {"alias": alias})

        if not output:
            return Null

        return output.last()

    def is_proto(self, index):
        """
        RETURN True IF THIS INDEX HAS NOT BEEN ASSIGNED ITS ALIAS
        """
        for a in self.cluster.get_aliases():
            if a.index == index and a.alias:
                return False
        return True

    def delete_record(self, filter):
        self.cluster.get_metadata()
        if self.cluster.node_metatdata.version.number.startswith("0.90"):
            query = filter
        elif self.cluster.node_metatdata.version.number.startswith("1.0"):
            query = {"query": filter}
        else:
            raise NotImplementedError

        if self.debug:
            Log.note("Delete bugs:\n{{query}}", {"query": query})

        self.cluster.delete(
            self.path + "/_query",
            data=convert.object2JSON(query),
            timeout=60
        )

    def extend(self, records):
        """
        records - MUST HAVE FORM OF
            [{"value":value}, ... {"value":value}] OR
            [{"json":json}, ... {"json":json}]
            OPTIONAL "id" PROPERTY IS ALSO ACCEPTED
        """
        lines = []
        try:
            for r in records:
                id = r.get("id", None)
                if id == None:
                    id = Random.hex(40)

                if "json" in r:
                    json = r["json"]
                elif "value" in r:
                    json = convert.object2JSON(r["value"])
                else:
                    json = None
                    Log.error("Expecting every record given to have \"value\" or \"json\" property")

                lines.append('{"index":{"_id": ' + convert.object2JSON(id) + '}}')
                lines.append(json)
            del records

            if not lines:
                return

            try:
                data_bytes = "\n".join(lines) + "\n"
                data_bytes = data_bytes.encode("utf8")
            except Exception, e:
                Log.error("can not make request body from\n{{lines|indent}}", {"lines": lines}, e)

            response = self.cluster._post(
                self.path + "/_bulk",
                data=data_bytes,
                headers={"Content-Type": "text"},
                timeout=self.settings.timeout
            )
            items = response["items"]

            for i, item in enumerate(items):
                if self.cluster.version.startswith("0.90."):
                    if not item.index.ok:
                        Log.error("{{error}} while loading line:\n{{line}}", {
                            "error": item.index.error,
                            "line": lines[i * 2 + 1]
                        })
                elif self.cluster.version.startswith("1.4."):
                    if not item.index.status==201:
                        Log.error("{{error}} while loading line:\n{{line}}", {
                            "error": item.index.error,
                            "line": lines[i * 2 + 1]
                        })
                else:
                    Log.error("version not supported {{version}}", {"version":self.cluster.version})

            if self.debug:
                Log.note("{{num}} items added", {"num": len(items)})
        except Exception, e:
            if e.message.startswith("sequence item "):
                Log.error("problem with {{data}}", {"data": repr(lines[int(e.message[14:16].strip())])}, e)
            Log.error("problem sending to ES", e)


    # RECORDS MUST HAVE id AND json AS A STRING OR
    # HAVE id AND value AS AN OBJECT
    def add(self, record):
        if isinstance(record, list):
            Log.error("add() has changed to only accept one record, no lists")
        self.extend([record])

    # -1 FOR NO REFRESH
    def set_refresh_interval(self, seconds):
        if seconds <= 0:
            interval = -1
        else:
            interval = unicode(seconds) + "s"

        response = self.cluster.put(
            "/" + self.settings.index + "/_settings",
            data='{"index":{"refresh_interval":' + convert.object2JSON(interval) + '}}'
        )

        result = convert.JSON2object(utf82unicode(response.content))
        # if self.cluster.version.startswith("0.90."):
        # if not result.ok:
        #     Log.error("Can not set refresh interval ({{error}})", {
        #         "error": utf82unicode(response.content)
        #     })
        # elif self.cluster.version.startswith("0.4."):
        #     Log.error("")
        # else:
        #     Log.error("Do not know how to handle ES version {{version}}", {"version":self.cluster.version})

    def search(self, query, timeout=None):
        query = wrap(query)
        try:
            if self.debug:
                if len(query.facets.keys()) > 20:
                    show_query = query.copy()
                    show_query.facets = {k: "..." for k in query.facets.keys()}
                else:
                    show_query = query
                Log.note("Query:\n{{query|indent}}", {"query": show_query})
            return self.cluster._post(
                self.path + "/_search",
                data=convert.object2JSON(query).encode("utf8"),
                timeout=nvl(timeout, self.settings.timeout)
            )
        except Exception, e:
            Log.error("Problem with search (path={{path}}):\n{{query|indent}}", {
                "path": self.path + "/_search",
                "query": query
            }, e)

    def threaded_queue(self, size=None, period=None):
        return ThreadedQueue(self, size=size, period=period)

    def delete(self):
        self.cluster.delete_index(index=self.settings.index)


class Cluster(object):
    def __init__(self, settings):
        """
        settings.explore_metadata == True - IF PROBING THE CLUSTER FOR METATDATA IS ALLOWED
        settings.timeout == NUMBER OF SECONDS TO WAIT FOR RESPONSE, OR SECONDS TO WAIT FOR DOWNLOAD (PASSED TO requests)
        """

        settings = wrap(settings)
        assert settings.host
        settings.setdefault("explore_metadata", True)

        self.cluster_metadata = None
        settings.setdefault("port", 9200)
        self.debug = nvl(settings.debug, DEBUG)
        self.settings = settings
        self.version = None
        self.path = settings.host + ":" + unicode(settings.port)

    def get_or_create_index(self, settings, schema=None, limit_replicas=None):
        settings = deepcopy(settings)
        aliases = self.get_aliases()

        indexes = Q.sort([
            a
            for a in aliases
            if (a.alias == settings.index and settings.alias == None) or
               (re.match(re.escape(settings.index) + "\\d{8}_\\d{6}", a.index) and settings.alias == None) or
               (a.index == settings.index and a.alias == settings.alias )
        ], "index")
        if not indexes:
            self.create_index(settings, schema, limit_replicas=limit_replicas)
        elif indexes.last().alias != None:
            settings.alias = indexes.last().alias
            settings.index = indexes.last().index
        elif settings.alias == None:
            settings.alias = settings.index
            settings.index = indexes.last().index
        return Index(settings)

    def get_index(self, settings):
        """
        TESTS THAT THE INDEX EXISTS BEFORE RETURNING A HANDLE
        """
        aliases = self.get_aliases()
        if settings.index in aliases.index:
            return Index(settings)
        if settings.index in aliases.alias:
            match = [a for a in aliases if a.alias == settings.index][0]
            settings.alias = match.alias
            settings.index = match.index
            return Index(settings)
        Log.error("Can not find index {{index_name}}", {"index_name": settings.index})

    def create_index(self, settings, schema=None, limit_replicas=None):
        if not settings.alias:
            settings.alias = settings.index
            settings.index = proto_name(settings.alias)

        if settings.alias == settings.index:
            Log.error("Expecting index name to conform to pattern")

        if settings.schema_file:
            Log.error('schema_file attribute not suported.  Use {"$ref":<filename>} instead')

        if isinstance(schema, basestring):
            schema = convert.JSON2object(schema, paths=True)
        else:
            schema = convert.JSON2object(convert.object2JSON(schema), paths=True)

        if not schema:
            schema = settings.schema

        limit_replicas = nvl(limit_replicas, settings.limit_replicas)

        if limit_replicas:
            # DO NOT ASK FOR TOO MANY REPLICAS
            health = self.get("/_cluster/health")
            if schema.settings.index.number_of_replicas >= health.number_of_nodes:
                Log.warning("Reduced number of replicas: {{from}} requested, {{to}} realized", {
                    "from": schema.settings.index.number_of_replicas,
                    "to": health.number_of_nodes - 1
                })
                schema.settings.index.number_of_replicas = health.number_of_nodes - 1

        self._post(
            "/" + settings.index,
            data=convert.object2JSON(schema).encode("utf8"),
            headers={"Content-Type": "application/json"}
        )
        time.sleep(2)
        es = Index(settings)
        return es

    def delete_index(self, index=None):
        self.delete("/" + index)

    def get_aliases(self):
        """
        RETURN LIST OF {"alias":a, "index":i} PAIRS
        ALL INDEXES INCLUDED, EVEN IF NO ALIAS {"alias":Null}
        """
        data = self.get_metadata().indices
        output = []
        for index, desc in data.items():
            if not desc["aliases"]:
                output.append({"index": index, "alias": None})
            else:
                for a in desc["aliases"]:
                    output.append({"index": index, "alias": a})
        return wrap(output)

    def get_metadata(self):
        if self.settings.explore_metadata:
            if not self.cluster_metadata:
                response = self.get("/_cluster/state")
                self.cluster_metadata = response.metadata
                self.node_metatdata = self.get("/")
                self.version = self.node_metatdata.version.number
        else:
            Log.error("Metadata exploration has been disabled")
        return self.cluster_metadata

    def _post(self, path, **kwargs):

        url = self.settings.host + ":" + unicode(self.settings.port) + path

        try:
            kwargs = wrap(kwargs)
            kwargs.setdefault("timeout", 600)
            kwargs.headers["Accept-Encoding"] = "gzip,deflate"
            kwargs = unwrap(kwargs)


            if "data" in kwargs and not isinstance(kwargs["data"], str):
                Log.error("data must be utf8 encoded string")

            if DEBUG:
                Log.note("{{url}}:\n{{data|left(300)|indent}}", {"url": url, "data": kwargs["data"]})

            response = requests.post(url, **kwargs)
            if self.debug:
                Log.note(utf82unicode(response.content)[:130])
            details = convert.JSON2object(utf82unicode(response.content))
            if details.error:
                Log.error(convert.quote2string(details.error))
            if details._shards.failed > 0:
                Log.error("Shard failure")
            return details
        except Exception, e:
            if url[0:4] != "http":
                suggestion = " (did you forget \"http://\" prefix on the host name?)"
            else:
                suggestion = ""

            Log.error("Problem with call to {{url}}" + suggestion + "\n{{body}}", {
                "url": url,
                "body": kwargs["data"] if DEBUG else kwargs["data"][0:100]
            }, e)

    def get(self, path, **kwargs):
        url = self.settings.host + ":" + unicode(self.settings.port) + path
        try:
            kwargs = wrap(kwargs)
            kwargs.setdefault("timeout", 600)
            response = requests.get(url, **kwargs)
            if self.debug:
                Log.note(utf82unicode(response.content)[:130])
            details = wrap(convert.JSON2object(utf82unicode(response.content)))
            if details.error:
                Log.error(details.error)
            return details
        except Exception, e:
            Log.error("Problem with call to {{url}}", {"url": url}, e)

    def put(self, path, **kwargs):
        url = self.settings.host + ":" + unicode(self.settings.port) + path

        if DEBUG:
            Log.note("PUT {{url}}:\n{{data|indent}}", {"url": url, "data": kwargs["data"]})
        try:
            kwargs = wrap(kwargs)
            kwargs.setdefault("timeout", 60)
            response = requests.put(url, **kwargs)
            if self.debug:
                Log.note(utf82unicode(response.content))
            return response
        except Exception, e:
            Log.error("Problem with call to {{url}}", {"url": url}, e)

    def delete(self, path, *args, **kwargs):
        url = self.settings.host + ":" + unicode(self.settings.port) + path
        try:
            kwargs.setdefault("timeout", 60)
            response = requests.delete(url, **kwargs)
            if self.debug:
                Log.note(utf82unicode(response.content))
            return response
        except Exception, e:
            Log.error("Problem with call to {{url}}", {"url": url}, e)


def proto_name(prefix, timestamp=None):
    if not timestamp:
        timestamp = datetime.utcnow()
    return prefix + convert.datetime2string(timestamp, "%Y%m%d_%H%M%S")


def sort(values):
    return wrap(sorted(values))


def scrub(r):
    """
    REMOVE KEYS OF DEGENERATE VALUES (EMPTY STRINGS, EMPTY LISTS, AND NULLS)
    CONVERT STRINGS OF NUMBERS TO NUMBERS
    RETURNS **COPY**, DOES NOT CHANGE ORIGINAL
    """
    return wrap(_scrub(r))


def _scrub(r):
    try:
        if r == None:
            return None
        elif isinstance(r, basestring):
            if r == "":
                return None
            return r
        elif Math.is_number(r):
            return convert.value2number(r)
        elif isinstance(r, dict):
            if isinstance(r, Struct):
                r = object.__getattribute__(r, "__dict__")
            output = {}
            for k, v in r.items():
                v = _scrub(v)
                if v != None:
                    output[k.lower()] = v
            if len(output) == 0:
                return None
            return output
        elif hasattr(r, '__iter__'):
            if isinstance(r, StructList):
                r = r.list
            output = []
            for v in r:
                v = _scrub(v)
                if v != None:
                    output.append(v)
            if not output:
                return None
            try:
                return sort(output)
            except Exception:
                return output
        else:
            return r
    except Exception, e:
        Log.warning("Can not scrub: {{json}}", {"json": r})


