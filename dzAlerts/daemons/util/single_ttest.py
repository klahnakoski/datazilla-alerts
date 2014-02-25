# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import unicode_literals
from math import sqrt
from scipy import stats

from dzAlerts.util.env.logs import Log


def single_ttest(point, stats, min_variance=0):
    n1 = stats.count
    m1 = stats.mean
    v1 = max(stats.variance, 1.0/12.0)  # VARIANCE OF STANDARD UNIFORM DISTRIBUTION

    if n1 < 2:
        return {"confidence": 0, "diff": 0}

    try:
        tt = (point - m1) / sqrt(v1)
        t_distribution = stats.distributions.t(n1 - 1)
        confidence = t_distribution.cdf(tt)
        return {"confidence": confidence, "diff": tt}
    except Exception, e:
        Log.error("error with t-test", e)
