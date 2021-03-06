# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import unicode_literals
import dzAlerts
from dzAlerts.util import maths
from dzAlerts.util.env.logs import Log


def median_test(samples1, samples2, interpolate=True):
    """
    interpolate=True WILL USE AN INTERPOLATED MEDIAN VALUE (FOR WHEN INTEGER VALUES ARE COMMON)
    """
    if len(samples1) < 3 or len(samples2) < 3:
        return {"diff": 0, "confidence": 0}
    median = dzAlerts.util.maths.stats.median(samples1 + samples2, simple=not interpolate, mean_weight=0.5)

    above1, below1 = count_partition(samples1, median)
    above2, below2 = count_partition(samples2, median)

    result = maths.stats.chisquare(
        [above1, below1, above2, below2],
        f_exp=[float(len(samples1)) / 2, float(len(samples1)) / 2, float(len(samples2)) / 2, float(len(samples2)) / 2]
    )

    return {"diff": result[0], "confidence": 1-result[1]}


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

