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
from pyLibrary.env.logs import Log
from pyLibrary.structs.wraps import unwrap
from pyLibrary.vendor.strangman.stats import ttest_ind


def welchs_ttest(a, b):
    """
    a AND b ARE SAMPLE SETS
    """
    try:
        if len(a) < 2 or len(b) < 2:
            return {"tstat": 0, "score": 0}

        t, prob = ttest_ind(unwrap(a), unwrap(b))

        if prob == 0.0:
            return {"tstat": t, "score": 19}
        else:
            return {"tstat": t, "score": -log(prob, 10)}
    except ZeroDivisionError, f:
        # WE CAN NOT KNOW WHAT WENT WRONG WITH THE LIBRARY
        return {"tstat": 0, "score": 0}
    except Exception, e:
        Log.error("programmer problem", e)
