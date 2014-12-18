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

from dzAlerts.daemons.util.welchs_ttest import welchs_ttest
from pyLibrary.debugs.logs import Log
from pyLibrary.queries import Q
from pyLibrary.structs.dicts import Struct


def partition(series, score_threshold):
    """
    THIS IS TOTALLY FAKE UNTIL I FIND SOFTWARE THAT DOES THIS ALREADY
    (http://arxiv.org/pdf/1309.3295.pdf), OR I DO THE REAL MATH MYSELF.
    """
    output = []
    _partition(series, score_threshold, output)

    # REVIEW THE KNOTS TO ENSURE WE HAVE OPTIMAL PARTITIONS
    for s1, s2 in Q.pairwise(output):
        if welchs_ttest(s1, s2).score < score_threshold:
            Log.error("We seem to have determined a false knot")
    return output


def _partition(series, score_threshold, output):
    best = Struct(index=-1, score=0)
    for i, s in enumerate(series):
        candidate = Struct(index=i, score=welchs_ttest(series[:i], series[1:]).score)
        if candidate.score > score_threshold and candidate.score > best.score:
            best = candidate
    if best.index == -1:
        output.append(series)
    else:
        _partition(series[:best.index], score_threshold, output)
        _partition(series[best.index:], score_threshold, output)
