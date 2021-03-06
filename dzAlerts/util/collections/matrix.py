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
from ..collections import PRODUCT, reverse, MAX, MIN
from ..cnv import CNV
from ..env.logs import Log
from ..struct import Null, Struct, wrap


class Matrix(object):
    """
    SIMPLE n-DIMENSIONAL ARRAY OF OBJECTS
    """
    ZERO = None

    def __init__(self, *dims, **kwargs):
        kwargs = wrap(kwargs)
        list = kwargs.list
        if list:
            self.num = 1
            self.dims = (len(list), )
            self.cube = list
            return

        value = kwargs.value
        if value != None:
            self.num = 0
            self.dims = tuple()
            self.cube = value
            return

        self.num = len(dims)
        self.dims = tuple(dims)
        if self.num == 0:
            self.cube = Null
        else:
            self.cube = _null(*dims)

    @staticmethod
    def wrap(array):
        output = Matrix()
        output.num = 1
        output.dims = (len(array), )
        output.cube = array
        return output

    def __getitem__(self, index):
        if isinstance(index, list):
            m = self.cube
            for k in index:
                m = m[k]
            return m
        if isinstance(index, slice):
            pass

    def __setitem__(self, key, value):
        try:
            if len(key) != self.num:
                Log.error("Expecting coordinates to match the number of dimensions")
            last = self.num - 1
            m = self.cube
            for k in key[0:last:]:
                m = m[k]
            m[key[last]] = value
        except Exception, e:
            Log.error("can not set item", e)

    def __bool__(self):
        return self.cube != None

    def __nonzero__(self):
        return self.cube != None

    def __len__(self):
        if self.num == 0:
            return 0
        return PRODUCT(self.dims)

    @property
    def value(self):
        if self.num:
            Log.error("can not get value of with dimension")
        return self.cube

    def __lt__(self, other):
        return self.value < other

    def __gt__(self, other):
        return self.value > other

    def __eq__(self, other):
        if other == None:
            return False
        return self.value == other

    def __add__(self, other):
        return self.value + other

    def __radd__(self, other):
        return other + self.value

    def __sub__(self, other):
        return self.value - other

    def __rsub__(self, other):
        return other - self.value

    def __mul__(self, other):
        return self.value * other

    def __rmul__(self, other):
        return other * self.value

    def __div__(self, other):
        return self.value / other

    def __rdiv__(self, other):
        return other / self.value

    def __iter__(self):
        return (self[c] for c in self._all_combos())

    def __float__(self):
        return self.value

    def groupby(self, io_select):
        """
        SLICE THIS MATRIX INTO ONES WITH LESS DIMENSIONALITY
        """

        #offsets WILL SERVE TO MASK DIMS WE ARE NOT GROUPING BY, AND SERVE AS RELATIVE INDEX FOR EACH COORDINATE
        offsets = []
        new_dim = []
        acc = 1
        for i, d in reverse(enumerate(self.dims)):
            if not io_select[i]:
                new_dim.insert(0, d)
            offsets.insert(0, acc * io_select[i])
            acc *= d

        if not new_dim:
            # WHEN groupby ALL DIMENSIONS, ONLY THE VALUES REMAIN
            # RETURN AN ITERATOR OF PAIRS (c, v), WHERE
            # c - COORDINATES INTO THE CUBE
            # v - VALUE AT GIVEN COORDINATES
            return ((c, self[c]) for c in self._all_combos())
        else:
            output = [[None, Matrix(new_dim)] for i in range(acc)]
            _groupby(self.cube, 0, offsets, 0, output, tuple(), [])

        return output

    def aggregate(self, type):
        func = aggregates[type]
        if not type:
            Log.error("Aggregate of type {{type}} is not supported yet", {"type": type})

        return func(self.num, self.cube)


    def forall(self, method):
        """
        IT IS EXPECTED THE method ACCEPTS (value, coord, cube), WHERE
        value - VALUE FOUND AT ELEMENT
        coord - THE COORDINATES OF THE ELEMENT (PLEASE, READ ONLY)
        cube - THE WHOLE CUBE, FOR USE IN WINDOW FUNCTIONS
        """
        for c in self._all_combos():
            method(self[c], c, self.cube)


    def _all_combos(self):
        """
        RETURN AN ITERATOR OF ALL COORDINATES
        """
        num = self.num
        dim = self.dims

        combos = PRODUCT(dim)
        if not combos:
            return

        c = [0]*num  # THE CORRECT SIZE
        while True:
            yield c

            for i in range(num-1, -1, -1):
                c[i] += 1
                if c[i] < dim[i]:
                    break
                c[i] = 0
            else:
                break


    def __str__(self):
        return "Matrix " + CNV.object2JSON(self.dims) + ": " + str(self.cube)

    def __json__(self):
        return CNV.object2JSON(self.cube)


Matrix.ZERO = Matrix(value=None)

def _max(depth, cube):
    if depth == 0:
        return cube
    elif depth == 1:
        return MAX(cube)
    else:
        return MAX(_max(depth - 1, c) for c in cube)


def _min(depth, cube):
    if depth == 0:
        return cube
    elif depth == 1:
        return MIN(cube)
    else:
        return MIN(_min(depth - 1, c) for c in cube)


aggregates = Struct(
    max=_max,
    maximum=_max,
    min=_min,
    minimum=_min
)


def _iter(cube, depth):
    if depth == 1:
        return cube.__iter__()
    else:
        def iterator():
            for c in cube:
                for b in _iter(c, depth - 1):
                    yield b

        return iterator()


def _null(*dims):
    d0 = dims[0]
    if d0 == 0:
        Log.error("Zero dimensions not allowed")
    if len(dims) == 1:
        return [Null for i in range(dims[0])]
    else:
        return [_null(*dims[1::]) for i in range(dims[0])]


def _groupby(cube, depth, intervals, offset, output, group, new_coord):
    if depth == len(intervals):
        output[offset][0] = group
        output[offset][1][new_coord] = cube
        return

    interval = intervals[depth]

    if interval:
        for i, c in enumerate(cube):
            _groupby(c, depth + 1, intervals, offset + i * interval, output, group + ( i, ), new_coord)
    else:
        for i, c in enumerate(cube):
            _groupby(c, depth + 1, intervals, offset, output, group + (-1, ), new_coord + [i])


