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

from scipy.stats import beta

from pyLibrary.collections import MIN, MAX
from pyLibrary.maths import Math

REAL_REGRESSION_RATE = 0.01    # CHOOSE SOMETHING SMALL TO REFLECT RARITY
CONFIDENCE = 0.70


def failure_rate(previous_results, current_results, real_rate=REAL_REGRESSION_RATE, confidence=CONFIDENCE):
    if confidence is None:
        # USED FOR TESTING
        for page_name, results in previous_results.items():
            results["failure_probability"] = float(results["total_fail"]) / (results["total_pass"] + results["total_fail"])
    else:
        for page_name, results in previous_results.items():
            results["failure_probability"] = confident_fail_rate(results["total_fail"], results["total_pass"], confidence)

    result = real_rate
    for page_name, results in current_results.items():
        p = previous_results[page_name]["failure_probability"]
        for i in range(results["total_fail"]):
            result = Math.bayesian_add(result, 1 - p)
        for i in range(results["total_pass"]):
            result = Math.bayesian_add(result, p)

    return result


def confident_fail_rate(total_fail, total_pass, confidence):
    """
    WE CAN NOT BE OVERLY OPTIMISTIC (0.0) ABOUT PREVIOUS SAMPLES, NOR
    OVERLY PESSIMISTIC (1.0).  WE MODIFY THE RATIO OF fail/total TOWARD
    NEUTRAL (0.5) BY ACCOUNTING FOR SMALL total AND STILL BE WITHIN confidence

    stats.beta.ppf(x, a, b) ==  1 - stats.beta.ppf(1-x, b, a)
    betaincinv(a, b, y) == stats.beta.ppf(y, a, b)
    """

    # SMALL NAMES OF EQUAL LENGTH TO DEMONSTRATE THE SYMMETRY BELOW
    confi = confidence
    error = 1 - confi

    # ppf() IS THE PERCENT POINT FUNCTION (INVERSE OF cdf()
    max1 = MIN(beta.ppf(confi, total_fail + 1, total_pass), 1)
    min1 = MAX(beta.ppf(error, total_fail, total_pass + 1), 0)

    # PICK THE probability CLOSEST TO 0.5
    if min1 < 0.5 and 0.5 < max1:
        return 0.5
    elif max1 < 0.5:
        return max1
    elif 0.5 < min1:
        return min1
    else:
        assert False








