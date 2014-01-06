# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import unicode_literals

import unittest
from dzAlerts.daemons.util.median_test import median_test


class TestMedianTest(unittest.TestCase):

    def test_tight_series(self):
        sample1 = [20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 21, 21, 21, 21, 21, 21, 21, 21, 21]
        sample2 = [20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21]

        simple_result = median_test(sample1, sample2, interpolate=False)  # EXAMPLE OF GOING WRONG
        smooth_result = median_test(sample1, sample2)
        assert smooth_result.confiodence < 0.90, "These are not different!"

    def test_smaller_series(self):
        sample1 = [20, 20, 20, 20, 20, 21, 21, 21]
        sample2 = [20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21]

        simple_result = median_test(sample1, sample2, interpolate=False)  # EXAMPLE OF GOING WRONG
        smooth_result = median_test(sample1, sample2)
        assert smooth_result.confiodence < 0.90, "These are not different!"
