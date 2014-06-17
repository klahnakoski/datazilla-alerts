# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import unicode_literals
from math import log

from dzAlerts.util.struct import Struct
from dzAlerts.util.vendor.strangman.stats import lttest_ind
from dzAlerts.util.struct import unwrap


def welchs_ttest(a, b):
    """
    a AND b ARE SAMPLES
    """
    if len(a) < 2 or len(b) < 2:
        return {"confidence": 0, "diff": 0}

    t, prob = lttest_ind(unwrap(a), unwrap(b))

    if prob == 0.0:
        return Struct(tstat=t, score=19)
    else:
        return Struct(tstat=t, score=-log(prob, 10))

