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

from math import log

from pyLibrary import maths
from pyLibrary.collections import AND
from pyLibrary.debugs.logs import Log
from pyLibrary.maths import Math
from pyLibrary.structs.dicts import Struct


def median_test(samples1, samples2, resolution=None, interpolate=True):
    """
    interpolate=True WILL USE AN INTERPOLATED MEDIAN VALUE (FOR WHEN INTEGER VALUES ARE COMMON)
    resolution IS REQUIRED TO ASSUME MAXIMUM RESOLUTION OF THE SAMPLES, AND PROVIDE SOME BLURRING
    """
    if len(samples1) < 3 or len(samples2) < 3:
        return {"diff": 0, "score": 0}
    median = maths.stats.median(samples1 + samples2, simple=not interpolate, mean_weight=0.5)

    if resolution == None:
        if AND([Math.is_integer(v) for v in samples1 + samples2]):
            resolution = 1.0  # IF WE SEE INTEGERS, THEN BLUR
        else:
            resolution = median/1000000  # ASSUME SOME BLUR

    above1, below1 = count_partition(samples1, median, resolution=resolution)
    above2, below2 = count_partition(samples2, median, resolution=resolution)

    result = maths.stats.chisquare(
        [above1, below1, above2, below2],
        f_exp=[float(len(samples1)) / 2, float(len(samples1)) / 2, float(len(samples2)) / 2, float(len(samples2)) / 2]
    )
    mstat, prob = result
    try:
        if prob == 0.0:
            return Struct(mstat=mstat, score=8)
        else:
            return Struct(mstat=mstat, score=-log(prob, 10))
    except Exception, e:
        Log.error("problem with math", e)


def count_partition(samples, cut_value, resolution=1.0):
    """
    COMPARE SAMPLES TO cut_value AND COUNT IF GREATER OR LESSER
    """
    smaller = 0.0
    larger = 0.0
    min_cut = cut_value - resolution/2.0
    max_cut = cut_value + resolution/2.0
    for v in samples:
        if v > max_cut:
            larger += 1
        elif v < min_cut:
            smaller += 1
        else:
            larger += (v - min_cut) / resolution
            smaller += (max_cut - v) / resolution
    return smaller, larger

