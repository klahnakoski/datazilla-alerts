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
        # MORE 20s
        sample1 = [20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 21, 21, 21, 21, 21, 21, 21, 21, 21]
        sample2 = [20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21]

        smooth_result = median_test(sample1, sample2)
        assert smooth_result["confidence"] < 0.90, "These are not different!"

        # MORE 21s
        sample1 = [20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21]
        sample2 = [20, 20, 20, 20, 20, 20, 20, 20, 20, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21]

        smooth_result = median_test(sample1, sample2)
        assert smooth_result["confidence"] < 0.90, "These are not different!"

    def test_smaller_series(self):
        sample1 = [20, 20, 20, 20, 20, 21, 21, 21]
        sample2 = [20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21]

        simple_result = median_test(sample1, sample2, interpolate=False)  # EXAMPLE OF GOING WRONG
        smooth_result = median_test(sample1, sample2)
        assert smooth_result["confidence"] < 0.90, "These are not different!"

    def test_bimodal_series(self):
        # EVEN NUMBER FROM EACH SERIES, FIRST SERIES HAS SAMPLE FROM SECOND MODE
        sample1 = [             43.35910744, 43.65596955, 43.6805196, 43.78713329, 43.54635098, 43.9086471, 43.54120044, 43.27229271, 43.35015387, 40.03955818]
        sample2 = [             40.18726543, 40.71542234, 40.15441333, 39.95611288, 38.30201645, 35.48697324, 40.16275306, 39.96934014]
        smooth_result_a = median_test(sample1, sample2)
        assert smooth_result_a["confidence"] > 0.997

        # ODD NUMBER OF SERIES, NAIVE MEDIAN TEST WILL PICK MIDDLE VALUE (FROM
        # FIRST SERIES) AND ASSUME THAT MEDIAN IS 50/50 IN EITHER MODE
        sample1 = [43.41440184, 43.35910744, 43.65596955, 43.6805196, 43.78713329, 43.54635098, 43.9086471, 43.54120044, 43.27229271, 43.35015387]
        sample2 = [40.03955818, 40.18726543, 40.71542234, 40.15441333, 39.95611288, 38.30201645, 35.48697324, 40.16275306, 39.96934014]
        smooth_result_b = median_test(sample1, sample2)
        assert smooth_result_b["confidence"] > smooth_result_a["confidence"]
