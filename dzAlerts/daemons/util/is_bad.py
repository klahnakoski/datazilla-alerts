# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import unicode_literals
from dzAlerts.util.maths import Math
from dzAlerts.util.struct import literal_field, set_default


def is_bad(settings, r):
    if settings.param.sustained_median.trigger < r.result.confidence:
        test_param = set_default(
            settings.param.suite[literal_field(r.Talos.Test.suite)],
            settings.param.test[literal_field(r.Talos.Test.name)],
            settings.param.default
        )

        if test_param.disable:
            return False

        if test_param.better == "higher":
            diff = -r.diff
        elif test_param.better == "lower":
            diff = r.diff
        else:
            diff = abs(r.diff)  # DEFAULT = ANY DIRECTION IS BAD

        if test_param.min_regression:
            if unicode(test_param.min_regression.strip()[-1]) == "%":
                min_diff = Math.abs(r.past_stats.mean * float(test_param.min_regression.strip()[:-1]) / 100.0)
            else:
                min_diff = Math.abs(float(test_param.min_regression))
        else:
            min_diff = Math.abs(r.past_stats.mean * 0.01)

        if diff > min_diff:
            return True

    return False
