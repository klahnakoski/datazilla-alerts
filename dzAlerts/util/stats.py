# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from math import sqrt
from .cnv import CNV
from .struct import nvl, Struct, Null
from .logs import Log


DEBUG = True
EPSILON = 0.000001


def stats2z_moment(stats):
    # MODIFIED FROM http://statsmodels.sourceforge.net/devel/_modules/statsmodels/stats/moment_helpers.html
    # ADDED count
    mc0, mc1, mc2, skew, kurt = stats.count, stats.mean, stats.variance, stats.skew, stats.kurtosis

    mz0 = mc0
    mz1 = mc1 * mc0
    mz2 = (mc2 + mc1 * mc1) * mc0
    mc3 = nvl(skew, 0) * (mc2 ** 1.5) # 3rd central moment
    mz3 = (mc3 + 3 * mc1 * mc2 + mc1 ** 3) * mc0  # 3rd non-central moment
    mc4 = (nvl(kurt, 0) + 3.0) * (mc2 ** 2.0) # 4th central moment
    mz4 = (mc4 + 4 * mc1 * mc3 + 6 * mc1 * mc1 * mc2 + mc1 ** 4) * mc0

    m = Z_moment(mz0, mz1, mz2, mz3, mz4)
    if DEBUG:
        globals()["DEBUG"] = False
        v = z_moment2stats(m, unbiased=False)
        try:
            assert closeEnough(v.count, stats.count)
            assert closeEnough(v.mean, stats.mean)
            assert closeEnough(v.variance, stats.variance)
            assert closeEnough(v.skew, stats.skew)
            assert closeEnough(v.kurtosis, stats.vkurtosis)
        except Exception, e:
            Log.error("programmer error")
        globals()["DEBUG"] = True
    return m


def closeEnough(a, b):
    if a == None and b == None:
        return True
    if a == None or b == None:
        return False

    if abs(a - b) <= EPSILON * (abs(a) + abs(b) + 1): return True
    return False


def z_moment2stats(z_moment, unbiased=True):
    Z = z_moment.S
    N = Z[0]
    if N == 0:
        return Stats()

    mean = Z[1] / N
    Z2 = Z[2] / N
    Z3 = Z[3] / N
    Z4 = Z[4] / N

    variance = (Z2 - mean * mean)
    mc3 = (Z3 - (3 * mean * variance + mean ** 3))  # 3rd central moment
    mc4 = (Z4 - (4 * mean * mc3 + 6 * mean * mean * variance + mean ** 4))

    if variance == 0.0:
        skew = None
        kurtosis = None
    elif variance < 0.0:
        Log.error("variance can not be negative ({{var}})", {"var":variance})
    else:
        skew = mc3 / (variance ** 1.5)
        kurtosis = (mc4 / (variance ** 2.0)) - 3.0

    stats = Stats(
        count=N,
        mean=mean,
        variance=variance,
        skew=skew,
        kurtosis=kurtosis,
        unbiased=unbiased
    )

    if DEBUG:
        globals()["DEBUG"] = False
        v = stats2z_moment(stats)
        try:
            for i in range(5):
                assert closeEnough(v.S[i], Z[i])
        except Exception, e:
            Log.error("Convertion failed.  Programmer error:\nfrom={{from|indent}},\nresult stats={{stats|indent}},\nexpected parem={{expected|indent}}", {
                "from": Z,
                "stats": stats,
                "expected": v.S
            })
        globals()["DEBUG"] = True

    return stats

class Stats(Struct):
    def __init__(self, **args):
        Struct.__init__(self)
        if "count" not in args:
            self.count = 0
            self.mean = 0
            self.variance = 0
            self.skew = None
            self.kurtosis = None
        elif "mean" not in args:
            self.count = args["count"]
            self.mean = 0
            self.variance = 0
            self.skew = None
            self.kurtosis = None
        elif "variance" not in args and "std" not in args:
            self.count = args["count"]
            self.mean = args["mean"]
            self.variance = 0
            self.skew = None
            self.kurtosis = None
        elif "skew" not in args:
            self.count = args["count"]
            self.mean = args["mean"]
            self.variance = args["variance"] if "variance" in args else args["std"] ** 2
            self.skew = None
            self.kurtosis = None
        elif "kurtosis" not in args:
            self.count = args["count"]
            self.mean = args["mean"]
            self.variance = args["variance"] if "variance" in args else args["std"] ** 2
            self.skew = args["skew"]
            self.kurtosis = None
        else:
            self.count = args["count"]
            self.mean = args["mean"]
            self.variance = args["variance"] if "variance" in args else args["std"] ** 2
            self.skew = args["skew"]
            self.kurtosis = args["kurtosis"]

        self.unbiased = \
            args["unbiased"] if "unbiased" in args else \
                not args["biased"] if "biased" in args else \
                    False


    @property
    def std(self):
        return sqrt(self.variance)


class Z_moment(object):
    """
    ZERO-CENTERED MOMENTS
    """

    def __init__(self, *args):
        self.S = tuple(args)

    def __add__(self, other):
        return Z_moment(*map(add, self.S, other.S))

    def __sub__(self, other):
        return Z_moment(*map(sub, self.S, other.S))

    @property
    def tuple(self):
    #RETURN AS ORDERED TUPLE
        return self.S

    @property
    def dict(self):
    #RETURN HASH OF SUMS
        return {"s" + unicode(i): m for i, m in enumerate(self.S)}


    @staticmethod
    def new_instance(values=None):
        if values == None: return Z_moment()
        values = [float(v) for v in values if v != None]

        return Z_moment(
            len(values),
            sum([n for n in values]),
            sum([pow(n, 2) for n in values]),
            sum([pow(n, 3) for n in values]),
            sum([pow(n, 4) for n in values])
        )


def add(a, b):
    return nvl(a, 0) + nvl(b, 0)


def sub(a, b):
    return nvl(a, 0) - nvl(b, 0)


def z_moment2dict(z):
    #RETURN HASH OF SUMS
    return {"s" + unicode(i): m for i, m in enumerate(z.S)}


setattr(CNV, "z_moment2dict", staticmethod(z_moment2dict))


def median(values):
    try:
        if not values:
            return Null

        l = len(values)
        _sorted = sorted(values)
        if l % 2 == 0:
            return (_sorted[l / 2 - 1] + _sorted[l / 2]) / 2
        else:
            return _sorted[l / 2]
    except Exception, e:
        Log.error("problem with median", e)
