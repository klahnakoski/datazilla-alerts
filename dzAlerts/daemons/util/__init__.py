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


#ARE THESE SEVERITY OR CONFIDENCE NUMBERS SIGNIFICANTLY DIFFERENT TO WARRANT AN
#UPDATE?
SIGNIFICANT = 0.2


def significant_difference(a, b):
    if a / b < (1 - SIGNIFICANT) or (1 + SIGNIFICANT) < a / b:
        return True
    if a in (0.0, 1.0) or b in (0.0, 1.0):
        return True
    b_diff = Math.bayesian_subtract(a, b)
    if 0.3 < b_diff < 0.7:
        return False
    return True
