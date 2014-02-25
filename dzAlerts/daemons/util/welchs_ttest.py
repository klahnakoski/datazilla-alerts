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


def welchs_ttest(stats1, stats2):
    """
    SNAGGED FROM https://github.com/mozilla/datazilla-metrics/blob/master/dzmetrics/ttest.py#L56
    Execute TWO-sided Welch's t-test given pre-calculated means and stddevs.

    Accepts summary data (N, stddev, and mean) for two datasets and performs
    one-sided Welch's t-test, returning p-value.
    """
    n1 = stats1.count
    m1 = stats1.mean
    v1 = max(stats1.variance, 1.0/12.0)

    n2 = stats2.count
    m2 = stats2.mean
    v2 = max(stats2.variance, 1.0/12.0)

    if n1 < 2 or n2 < 2:
        return {"confidence": 0, "diff": 0}

    vpooled = v1 / n1 + v2 / n2
    # 1/12 == STD OF STANDARD UNIFORM DISTRIBUTION
    # We assume test replicates (xi) are actually rounded results from
    # actual measurements somewhere in the range of (xi - 0.5, xi + 0.5),
    # which has a variance of 1/12
    tt = abs(m1 - m2) / sqrt(vpooled)

    df_numerator = vpooled ** 2
    df_denominator = ((v1 / n1) ** 2) / (n1 - 1) + ((v2 / n2) ** 2) / (n2 - 1)
    df = df_numerator / df_denominator

    # abs(x - 0.5)*2 IS AN ATTEMPT TO GIVE HIGH NUMBERS TO EITHER TAIL OF THE cdf
    return {"confidence": abs(stats.t(df).cdf(tt) - 0.5) * 2, "diff": tt}



