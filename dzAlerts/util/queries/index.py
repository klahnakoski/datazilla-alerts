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
from types import GeneratorType
from ..env.logs import Log
from ..struct import unwrap, wrap, tuplewrap


class UniqueIndex(object):
    """
    DEFINE A SET OF ATTRIBUTES THAT UNIQUELY IDENTIFIES EACH OBJECT IN A list.
    THIS ALLOWS set-LIKE COMPARISIONS (UNION, INTERSECTION, DIFFERENCE, ETC) WHILE
    STILL MAINTAINING list-LIKE FEATURES
    """

    def __init__(self, keys):
        self._data = {}
        self._keys = unwrap(keys)
        self.count = 0

    def __getitem__(self, key):
        try:
            if isinstance(key, dict):
                key = tuplewrap(key[k] for k in self._keys)
            else:
                key = tuplewrap(key)

            d = self._data.get(key, None)

            if len(key) < len(self._keys):
                # RETURN ANOTHER Index
                output = UniqueIndex(self._keys[len(key):])
                output._data = d
                return output
            else:
                return wrap(d)
        except Exception, e:
            Log.error("something went wrong", e)

    def __setitem__(self, key, value):
        Log.error("Not implemented")


    def add(self, val):
        if isinstance(val, dict):
            key = tuplewrap(val[k] for k in self._keys)
        else:
            key = tuplewrap(val)

        d = self._data.get(key, None)
        if d != None:
            Log.error("key already filled")
        else:
            self._data[key] = unwrap(val)

        self.count += 1

    def __contains__(self, key):
        return self[key] != None

    def __iter__(self):
        return (wrap(v) for v in self._data.itervalues())

    def __sub__(self, other):
        output = UniqueIndex(self._keys)
        for v in self:
            if v not in other:
                output.add(v)
        return output

    def __and__(self, other):
        output = UniqueIndex(self._keys)
        for v in self:
            if v in other: output.add(v)
        return output

    def __or__(self, other):
        output = UniqueIndex(self._keys)
        for v in self: output.add(v)
        for v in other: output.add(v)
        return output

    def __len__(self):
        if self.count == 0:
            for d in self:
                self.count += 1
        return self.count

    def subtract(self, other):
        return self.__sub__(other)

    def intersect(self, other):
        return self.__and__(other)


class Index(object):
    """
    USING DATABASE TERMINOLOGY, THIS IS A NON-UNIQUE INDEX
    """

    def __init__(self, keys):
        self._data = {}
        self._keys = unwrap(keys)
        self.count = 0

    def __getitem__(self, key):
        try:
            if isinstance(key, dict):
                key = tuplewrap(key[k] for k in self._keys)
            elif isinstance(key, tuple):
                key = tuplewrap(key)
            else:
                Log.error("expecting a tuple")

            d = self._data.get(key, None)

            if len(key) < len(self._keys):
                # RETURN ANOTHER Index
                output = Index(self._keys[len(key):])
                output._data = d
                return output
            else:
                return wrap(list(d))
        except Exception, e:
            Log.error("something went wrong", e)

    def __setitem__(self, key, value):
        Log.error("Not implemented")


    def add(self, val):
        if isinstance(val, dict):
            key = tuplewrap(val[k] for k in self._keys)
        else:
            key = tuplewrap(val)

        d = self._data.get(key, None)
        if d == None:
            d = list()
            self._data[key] = d
        d.append(unwrap(val))
        self.count += 1


    # def __contains__(self, key):
    #     return self[key] != None

    def __iter__(self):
        def itr():
            for v in self._data.values():
                for vv in v:
                    yield vv
        return itr()

    def __sub__(self, other):
        output = UniqueIndex(self._keys)
        for v in self:
            if v not in other:
                output.add(v)
        return output

    def __and__(self, other):
        output = UniqueIndex(self._keys)
        for v in self:
            if v in other: output.add(v)
        return output

    def __or__(self, other):
        output = UniqueIndex(self._keys)
        for v in self: output.add(v)
        for v in other: output.add(v)
        return output

    def __len__(self):
        if self.count == 0:
            for d in self:
                self.count += 1
        return self.count

    def subtract(self, other):
        return self.__sub__(other)

    def intersect(self, other):
        return self.__and__(other)


