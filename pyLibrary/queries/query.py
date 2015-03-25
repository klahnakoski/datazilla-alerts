# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals
from __future__ import division
from pyLibrary.collections import AND, reverse
from pyLibrary.debugs.logs import Log
from pyLibrary.maths import Math
from pyLibrary.queries import wrap_from
from pyLibrary.queries.dimensions import Dimension
from pyLibrary.queries.domains import Domain, is_keyword
from pyLibrary.queries.filters import TRUE_FILTER, simplify_esfilter
from pyLibrary.dot.dicts import Dict
from pyLibrary.dot import nvl, split_field, join_field, Null, set_default
from pyLibrary.dot.lists import DictList
from pyLibrary.dot import wrap, unwrap, listwrap


DEFAULT_LIMIT = 10

qb =None
INDEX_CACHE = None
def _late_import():
    global qb
    global INDEX_CACHE

    from pyLibrary.queries import qb
    from pyLibrary.queries.es09.util import INDEX_CACHE
    qb=qb
    INDEX_CACHE=INDEX_CACHE


class Query(object):
    def __new__(cls, query, schema=None):
        if isinstance(query, Query):
            return query
        return object.__new__(cls)

    def __init__(self, query, schema=None):
        """
        NORMALIZE QUERY SO IT CAN STILL BE JSON
        """
        if isinstance(query, Query):
            return

        object.__init__(self)
        query = wrap(query)

        max_depth = 1

        self.format = query.format

        self.frum = wrap_from(query["from"], schema=schema)

        select = query.select
        if isinstance(select, list):
            self.select = wrap([unwrap(_normalize_select(s, schema=schema)) for s in select])
        elif select:
            self.select = _normalize_select(select, schema=schema)
        else:
            if query.edges or query.groupby:
                self.select = {"name": "count", "value": ".", "aggregate": "count"}
            else:
                self.select = {"name": "__all__", "value": "*", "aggregate": "none"}

        if query.groupby and query.edges:
            Log.error("You can not use both the `groupby` and `edges` clauses in the same query!")
        elif query.edges:
            self.edges = _normalize_edges(query.edges, schema=schema)
            self.groupby = None
        else:
            self.edges = None
            self.groupby = _normalize_groupby(query.groupby, schema=schema)


        self.where = _normalize_where(query.where, schema=schema)
        self.window = [_normalize_window(w) for w in listwrap(query.window)]
        self.sort = _normalize_sort(query.sort)
        self.limit = nvl(query.limit, DEFAULT_LIMIT)
        if not Math.is_integer(self.limit) or self.limit < 0:
            Log.error("Expecting limit >= 0")

        self.isLean = query.isLean


        # DEPTH ANALYSIS - LOOK FOR COLUMN REFERENCES THAT MAY BE DEEPER THAN
        # THE from SOURCE IS.
        # TODO: IGNORE REACHING INTO THE NON-NESTED TYPES
        if isinstance(self.frum, list):
            if not qb:
                _late_import()
            columns = qb.get_columns(self.frum)
        else:
            columns = self.frum.get_columns()
        vars = get_all_vars(self)
        for c in columns:
            if c.name in vars and c.depth:
                Log.error("This query, with variable {{var_name}} looks too deep", )

    @property
    def columns(self):
        return listwrap(self.select) + nvl(self.edges, self.groupby)

    def __getitem__(self, item):
        if item == "from":
            return self.frum
        return Dict.__getitem__(self, item)

    def copy(self):
        output = object.__new__(Query)
        source = object.__getattribute__(self, "__dict__")
        dest = object.__getattribute__(output, "__dict__")
        set_default(dest, source)
        return output


canonical_aggregates = {
    "min": "minimum",
    "max": "maximum",
    "add": "sum",
    "avg": "average",
    "mean": "average"
}

def _normalize_selects(selects, schema=None):
    if isinstance(selects, list):
        return wrap([_normalize_select(s, schema=schema) for s in selects])
    else:
        return _normalize_select(selects, schema=schema)

def _normalize_select(select, schema=None):
    if isinstance(select, basestring):
        if schema:
            s = schema[select]
            if s:
                return s.getSelect()
        return Dict(
            name=select.rstrip("."),  # TRAILING DOT INDICATES THE VALUE, BUT IS INVALID FOR THE NAME
            value=select,
            aggregate="none"
        )
    else:
        select = wrap(select)
        output = select.copy()
        output.name = nvl(select.name, select.value, select.aggregate)

        if not output.name:
            Log.error("expecting select to have a name: {{select}}", {"select": select})

        output.aggregate = nvl(canonical_aggregates.get(select.aggregate), select.aggregate, "none")
        return output


def _normalize_edges(edges, schema=None):
    return [_normalize_edge(e, schema=schema) for e in listwrap(edges)]


def _normalize_edge(edge, schema=None):
    if isinstance(edge, basestring):
        if schema:
            e = schema[edge]
            if e:
                if isinstance(e.fields, list) and len(e.fields) == 1:
                    return Dict(
                        name=e.name,
                        value=e.fields[0],
                        domain=e.getDomain()
                    )
                else:
                    return Dict(
                        name=e.name,
                        domain=e.getDomain()
                    )
        return Dict(
            name=edge,
            value=edge,
            domain=_normalize_domain(schema=schema)
        )
    else:
        edge = wrap(edge)
        if not edge.name and not isinstance(edge.value, basestring):
            Log.error("You must name compound edges: {{edge}}", {"edge": edge})

        if isinstance(edge.value, (dict, list)) and not edge.domain:
            # COMPLEX EDGE IS SHORT HAND
            domain = _normalize_domain(schema=schema)
            domain.dimension = Dict(fields=edge.value)

            return Dict(
                name=edge.name,
                allowNulls=False if edge.allowNulls is False else True,
                domain=domain
            )

        return Dict(
            name=nvl(edge.name, edge.value),
            value=edge.value,
            range=edge.range,
            allowNulls=False if edge.allowNulls is False else True,
            domain=_normalize_domain(edge.domain, schema=schema)
        )


def _normalize_groupby(groupby, schema=None):
    if groupby == None:
        return None
    return [_normalize_group(e, schema=schema) for e in listwrap(groupby)]


def _normalize_group(edge, schema=None):
    if isinstance(edge, basestring):
        return wrap({
            "name": edge,
            "value": edge,
            "domain": {"type": "default"}
        })
    else:
        edge = wrap(edge)
        if (edge.domain and edge.domain.type != "default") or edge.allowNulls != None:
            Log.error("groupby does not accept complicated domains")

        if not edge.name and not isinstance(edge.value, basestring):
            Log.error("You must name compound edges: {{edge}}", {"edge": edge})

        return wrap({
            "name": nvl(edge.name, edge.value),
            "value": edge.value,
            "domain": {"type": "default"}
        })


def _normalize_domain(domain=None, schema=None):
    if not domain:
        return Domain(type="default")
    elif isinstance(domain, Dimension):
        return domain.getDomain()
    elif schema and isinstance(domain, basestring) and schema[domain]:
        return schema[domain].getDomain()
    elif isinstance(domain, Domain):
        return domain

    if not domain.name:
        domain = domain.copy()
        domain.name = domain.type
    return Domain(**unwrap(domain))


def _normalize_window(window, schema=None):
    return Dict(
        name=nvl(window.name, window.value),
        value=window.value,
        edges=[_normalize_edge(e, schema) for e in listwrap(window.edges)],
        sort=_normalize_sort(window.sort),
        aggregate=window.aggregate,
        range=_normalize_range(window.range),
        where=_normalize_where(window.where, schema=schema)
    )


def _normalize_range(range):
    if range == None:
        return None

    return Dict(
        min=range.min,
        max=range.max
    )


def _normalize_where(where, schema=None):
    if where == None:
        return TRUE_FILTER
    if schema == None:
        return where
    where = simplify_esfilter(_where_terms(where, where, schema))
    return where


def _map_term_using_schema(master, path, term, schema_edges):
    """
    IF THE WHERE CLAUSE REFERS TO FIELDS IN THE SCHEMA, THEN EXPAND THEM
    """
    output = DictList()
    for k, v in term.items():
        dimension = schema_edges[k]
        if isinstance(dimension, Dimension):
            domain = dimension.getDomain()
            if dimension.fields:
                if isinstance(dimension.fields, dict):
                    # EXPECTING A TUPLE
                    for local_field, es_field in dimension.fields.items():
                        local_value = v[local_field]
                        if local_value == None:
                            output.append({"missing": {"field": es_field}})
                        else:
                            output.append({"term": {es_field: local_value}})
                    continue

                if len(dimension.fields) == 1 and is_keyword(dimension.fields[0]):
                    # SIMPLE SINGLE-VALUED FIELD
                    if domain.getPartByKey(v) is domain.NULL:
                        output.append({"missing": {"field": dimension.fields[0]}})
                    else:
                        output.append({"term": {dimension.fields[0]: v}})
                    continue

                if AND(is_keyword(f) for f in dimension.fields):
                    # EXPECTING A TUPLE
                    if not isinstance(v, tuple):
                        Log.error("expecing {{name}}={{value}} to be a tuple", {"name": k, "value": v})
                    for i, f in enumerate(dimension.fields):
                        vv = v[i]
                        if vv == None:
                            output.append({"missing": {"field": f}})
                        else:
                            output.append({"term": {f: vv}})
                    continue
            if len(dimension.fields) == 1 and is_keyword(dimension.fields[0]):
                if domain.getPartByKey(v) is domain.NULL:
                    output.append({"missing": {"field": dimension.fields[0]}})
                else:
                    output.append({"term": {dimension.fields[0]: v}})
                continue
            if domain.partitions:
                part = domain.getPartByKey(v)
                if part is domain.NULL or not part.esfilter:
                    Log.error("not expected to get NULL")
                output.append(part.esfilter)
                continue
            else:
                Log.error("not expected")
        elif isinstance(v, dict):
            sub = _map_term_using_schema(master, path + [k], v, schema_edges[k])
            output.append(sub)
            continue

        output.append({"term": {k: v}})
    return {"and": output}


def _move_nested_term(master, where, schema):
    """
    THE WHERE CLAUSE CAN CONTAIN NESTED PROPERTY REFERENCES, THESE MUST BE MOVED
    TO A NESTED FILTER
    """
    items = where.term.items()
    if len(items) != 1:
        Log.error("Expecting only one term")
    k, v = items[0]
    nested_path = _get_nested_path(k, schema)
    if nested_path:
        return {"nested": {
            "path": nested_path,
            "query": {"filtered": {
                "query": {"match_all": {}},
                "filter": {"and": [
                    {"term": {k: v}}
                ]}
            }}
        }}
    return where


def _get_nested_path(field, schema):
    if not INDEX_CACHE:
        _late_import()

    if is_keyword(field):
        field = join_field([schema.es.alias] + split_field(field))
        for i, f in reverse(enumerate(split_field(field))):
            path = join_field(split_field(field)[0:i + 1:])
            if path in INDEX_CACHE:
                return join_field(split_field(path)[1::])
    return None


def _where_terms(master, where, schema):
    """
    USE THE SCHEMA TO CONVERT DIMENSION NAMES TO ES FILTERS
    master - TOP LEVEL WHERE (FOR PLACING NESTED FILTERS)
    """
    if isinstance(where, dict):
        if where.term:
            # MAP TERM
            try:
                output = _map_term_using_schema(master, [], where.term, schema.edges)
                return output
            except Exception, e:
                Log.error("programmer problem?", e)
        elif where.terms:
            # MAP TERM
            output = DictList()
            for k, v in where.terms.items():
                if not isinstance(v, (list, set)):
                    Log.error("terms filter expects list of values")
                edge = schema.edges[k]
                if not edge:
                    output.append({"terms": {k: v}})
                else:
                    if isinstance(edge, basestring):
                        # DIRECT FIELD REFERENCE
                        return {"terms": {edge: v}}
                    try:
                        domain = edge.getDomain()
                    except Exception, e:
                        Log.error("programmer error", e)
                    fields = domain.dimension.fields
                    if isinstance(fields, dict):
                        or_agg = []
                        for vv in v:
                            and_agg = []
                            for local_field, es_field in fields.items():
                                vvv = vv[local_field]
                                if vvv != None:
                                    and_agg.append({"term": {es_field: vvv}})
                            or_agg.append({"and": and_agg})
                        output.append({"or": or_agg})
                    elif isinstance(fields, list) and len(fields) == 1 and is_keyword(fields[0]):
                        output.append({"terms": {fields[0]: v}})
                    elif domain.partitions:
                        output.append({"or": [domain.getPartByKey(vv).esfilter for vv in v]})
            return {"and": output}
        elif where["or"]:
            return {"or": [unwrap(_where_terms(master, vv, schema)) for vv in where["or"]]}
        elif where["and"]:
            return {"and": [unwrap(_where_terms(master, vv, schema)) for vv in where["and"]]}
        elif where["not"]:
            return {"not": unwrap(_where_terms(master, where["not"], schema))}
    return where


def _normalize_sort(sort=None):
    """
    CONVERT SORT PARAMETERS TO A NORMAL FORM SO EASIER TO USE
    """

    if not sort:
        return DictList.EMPTY

    output = DictList()
    for s in listwrap(sort):
        if isinstance(s, basestring) or Math.is_integer(s):
            output.append({"field": s, "sort": 1})
        else:
            output.append({"field": nvl(s.field, s.value), "sort": nvl(sort_direction[s.sort], 1)})
    return wrap(output)


sort_direction = {
    "asc": 1,
    "desc": -1,
    "none": 0,
    1: 1,
    0: 0,
    -1: -1,
    None: 1,
    Null: 1
}


def get_all_vars(query):
    output = []
    for s in listwrap(query.select):
        output.extend(select_get_all_vars(s))
    for s in listwrap(query.edges):
        output.extend(edges_get_all_vars(s))
    for s in listwrap(query.groupby):
        output.extend(edges_get_all_vars(s))
    output.extend(where_get_all_vars(query.where))
    return output


def select_get_all_vars(s):
    if isinstance(s.value, list):
        return s.value
    elif isinstance(s.value, basestring):
        return [s.value]
    elif s.value == None or s.value == ".":
        return []
    else:
        Log.error("not supported")

def edges_get_all_vars(e):
    output = []
    if isinstance(e.value, basestring):
        output.append(e.value)
    if e.domain.key:
        output.append(e.domain.key)
    if e.domain.where:
        output.extend(where_get_all_vars(e.domain.where))
    if e.domain.partitions:
        for p in e.domain.partitions:
            if p.where:
                output.extend(where_get_all_vars(p.where))
    return output

def where_get_all_vars(w):
    if w in [True, False, None]:
        return []

    output = []
    key = list(w.keys())[0]
    val = w[key]
    if key in ["and", "or"]:
        for ww in val:
            output.extend(where_get_all_vars(ww))
        return output

    if key == "not":
        return where_get_all_vars(val)

    if key in ["exists", "missing"]:
        if isinstance(val, unicode):
            return [val]
        else:
            return [val.field]

    if key in ["gte", "gt", "eq", "ne", "term", "terms", "lt", "lte"]:
        if not isinstance(val, dict):
            Log.error("Expecting `{{key}}` to have a dict value, not a {{type}}", {
                "key": key,
                "type": val.__class__.__name__
            })
        return list(val.keys())

    Log.error("do not know how to handle where {{where|json}}", {"where", w})


