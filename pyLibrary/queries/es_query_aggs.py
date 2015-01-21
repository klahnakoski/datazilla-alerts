# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals
from __future__ import division
from copy import copy

from pyLibrary.collections.matrix import Matrix
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import listwrap, Dict, wrap, literal_field, set_default
from pyLibrary.queries import es_query_util
from pyLibrary.queries.cube import Cube
from pyLibrary.queries.domains import PARTITION, SetDomain
from pyLibrary.queries.es_query_util import aggregates



# THE NEW AND FANTASTIC AGGS OPERATION IN ELASTICSEARCH!
# WE ALL WIN NOW!
from pyLibrary.queries.filters import simplify


def is_aggsop(es, query):
    if es.cluster.version.startswith("1.4") and query.edges:
        return True
    return False


def es_aggsop(es, mvel, query):
    select = listwrap(query.select)

    esQuery = Dict()
    for s in select:
        if s.aggregate == "count" and s.value:
            esQuery[s.name]["value_count"].field = s.value
        elif s.aggregate == "count":
            pass
        else:
            esQuery.aggs[s.name][aggregates[s.aggregate]].field = s.value
    esQuery.size = query.limit

    decoders = [AggsDecoder(e) for e in query.edges]
    for d in decoders:
        esQuery = d.append_query(esQuery)

    esQuery.size = 0
    esQuery.filter = simplify(query.where)
    data = es_query_util.post(es, esQuery, query.limit)

    new_edges = count_dims(data.aggregations, query.edges)
    dims = tuple(len(e.domain.partitions) for e in new_edges)
    matricies = {s.name:Matrix(*dims) for s in select}
    _sub_dim_pull(data.aggregations, select, new_edges, matricies, [None]*len(new_edges), len(new_edges)-1)

    cube = Cube(query.select, new_edges, matricies)
    cube.frum = query
    return cube


def count_dims(aggs, edges):
    new_edges = []
    for e in edges:
        if e.domain.type == "default":
            new_e = copy(e)
            new_e.domain = SetDomain(key="value", partitions=[])
            new_e.domain.type = "default"
            new_e.domain.partitions = set()
            new_edges.append(new_e)
        else:
            new_edges.append(e)

    _count_dims(aggs, new_edges, len(edges)-1)

    for e in new_edges:
        if e.domain.type == "default":
            e.domain = SetDomain(
                key="value",
                partitions= [{"value":v} for i, v in enumerate(e.domain.partitions)]
            )


    return new_edges


def _count_dims(aggs, edges, rem):
    domain = edges[rem].domain
    buckets = aggs[literal_field(edges[rem].name)].buckets
    if domain.type == "default":
        domain.partitions |= set(buckets.key)

    if rem>0:
        for b in buckets:
            _count_dims(b, edges, rem-1)


def _sub_dim_pull(aggs, select, decoders, matricies, coord, rem):
    for c, b in decoders[rem].get_parts(aggs):
        coord[rem] = c
        if rem == 0:
            for s in select:
                name = literal_field(s.name)
                if s.aggregate == "count" and not s.value:
                    matricies[name][coord] = b.doc_count
                else:
                    Log.error("Do not know how to handle")
        else:
            _sub_dim_pull(b, select, decoders, matricies, coord, rem - 1)

    if not e.value and e.domain.dimension.fields:
        fields = domain.dimension.fields
        if isinstance(fields, dict):
            for k, v in fields.items():
                esQuery.terms = {"field": v}
                esQuery = wrap({"aggs": {k: esQuery}})
        else:
            for v in fields:
                esQuery.terms = {"field": v}
                esQuery = wrap({"aggs": {v: esQuery}})

        esQuery.filter = simplify(e.domain.esfilter)
    else:
        Log.error("do not know how to handle type {{type}}", {"type":domain.type})


class AggsDecoder(object):

    def __new__(cls, *args, **kwargs):
        e=args[0]
        if e.value and (e.domain.type in PARTITION or e.domain.type=="default"):
            return object.__new__(SimpleDecoder, e)
        elif not e.value and e.domain.dimension.fields:
            # THIS domain IS FROM A dimension THAT IS A SIMPLE LIST OF fields
            # JUST PULL THE FIELDS
            fields = e.domain.dimension.fields
            if isinstance(fields, dict):
                return object.__new__(DimFieldDictDecoder, e)
            else:
                return object.__new__(DimFieldListDecoder, e)
        else:
            Log.error("domain type of {{type}} is not supported yet", {"type": e.domain.type})


    def __init__(self, edge):
        self.edge=edge

    def append_query(self, esQuery):
        Log.error("Not supported")

    def get_parts(self, aggs):
        Log.error("Not supported")


class SimpleDecoder(AggsDecoder):
    def append_query(self, esQuery):
        esQuery.terms = {"field": self.edge.value}
        return wrap({"aggs": {self.edge.name: esQuery}})

    def get_parts(self, aggs):
        domain = self.edge.domain
        buckets = aggs[literal_field(self.edge.name)].buckets
        for b in buckets:
            c = domain.getIndexByKey(b.key)
            yield (c, b)


class DimFieldListDecoder(AggsDecoder):
    def append_query(self, esQuery):
        fields = self.edge.domain.dimension.fields
        for v in fields:
            esQuery.terms = {"field": v}
            esQuery = wrap({"aggs": {v: esQuery}})
        esQuery.filter = simplify(self.edge.domain.esfilter)
        return esQuery

    def get_parts(self, aggs):
        pass


    def _get_sub(self, aggs, coord):
        domain = self.edge.domain
        buckets = aggs[literal_field(self.edge.name)].buckets
        for b in buckets:
            c = domain.getIndexByKey(b.key)
            yield (c, b)


class DimFieldDictDecoder(AggsDecoder):

    def append_query(self, esQuery):
        fields = self.edge.domain.dimension.fields
        for k, v in fields.items():
            esQuery.terms = {"field": v}
            esQuery = wrap({"aggs": {k: esQuery}})
        esQuery.filter = simplify(self.edge.domain.esfilter)
        return esQuery

    def get_parts(self, aggs):
        pass

