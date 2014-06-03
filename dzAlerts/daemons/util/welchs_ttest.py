# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import unicode_literals

from dzAlerts.util.struct import Struct
from dzAlerts.util.vendor.strangman.stats import lttest_ind


def welchs_ttest(a, b):
    """
    a AND b ARE SAMPLES
    """
    t, prob = lttest_ind(a, b)
    return Struct(confidence=1-prob, tstat=t)

