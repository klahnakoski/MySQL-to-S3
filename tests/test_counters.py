# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from datetime import datetime

from mysql_to_s3.counter import DurationCounter, BatchCounter, Counter
from pyDots import Null
from pyLibrary.testing.fuzzytestcase import FuzzyTestCase


class TestCounters(FuzzyTestCase):

    def test_week_10(self):
        data = [
            # (value, expected) PAIRS
            (datetime(2017, 1, 1), [0, 0, 0]),
            (datetime(2017, 1, 1), [0, 0, 1]),
            (datetime(2017, 1, 1), [0, 0, 2]),
            (datetime(2017, 1, 2), [0, 0, 3]),
            (datetime(2017, 1, 2), [0, 0, 4]),
            (datetime(2017, 1, 2), [0, 0, 5]),
            (datetime(2017, 1, 2), [0, 0, 6]),
            (datetime(2017, 1, 2), [0, 0, 7]),
            (datetime(2017, 1, 2), [0, 0, 8]),
            (datetime(2017, 1, 2), [0, 0, 9]),
            (datetime(2017, 1, 3), [0, 1, 0]),
            (datetime(2017, 1, 4), [0, 1, 1]),
            (datetime(2017, 1, 5), [0, 1, 2]),
            (datetime(2017, 1, 6), [0, 1, 3]),
            (datetime(2017, 1, 7), [0, 1, 4]),
            (datetime(2017, 1, 8), [1, 0, 0]),
            (datetime(2017, 1, 9), [1, 0, 1]),
            (datetime(2017, 1, 9), [1, 0, 2]),
            (datetime(2017, 1, 10), [1, 0, 3]),
            (datetime(2017, 1, 11), [1, 0, 4]),
            (datetime(2017, 1, 11), [1, 0, 5]),
            (datetime(2017, 1, 12), [1, 0, 6]),
            (datetime(2017, 1, 12), [1, 0, 7]),
            (datetime(2017, 1, 12), [1, 0, 8])
        ]

        c = DurationCounter(
            start=datetime(2017, 1, 1),
            duration="week",
            child=BatchCounter(
                start=0,
                size=10,
                child=Counter(0)
            )
        )

        result = [c.next((d, Null)) for d, _ in data]
        expecting = [e for _, e in data]

        self.assertEqual(result, expecting, "Expecting counter")

    def test_cut_week_10(self):
        """
        TEST THAT, NO MATTER WHERE THE COUNTING IS RESTARTED, WE GET THE SAME RESULT
        """
        data = [
            # (value, expected) PAIRS
            (datetime(2017, 1, 1), [0, 0, 0]),
            (datetime(2017, 1, 1), [0, 0, 1]),
            (datetime(2017, 1, 1), [0, 0, 2]),
            (datetime(2017, 1, 2), [0, 0, 3]),
            (datetime(2017, 1, 2), [0, 0, 4]),
            (datetime(2017, 1, 2), [0, 0, 5]),
            (datetime(2017, 1, 2), [0, 0, 6]),
            (datetime(2017, 1, 2), [0, 0, 7]),
            (datetime(2017, 1, 2), [0, 0, 8]),
            (datetime(2017, 1, 2), [0, 0, 9]),
            (datetime(2017, 1, 3), [0, 1, 0]),
            (datetime(2017, 1, 4), [0, 1, 1]),
            (datetime(2017, 1, 5), [0, 1, 2]),
            (datetime(2017, 1, 6), [0, 1, 3]),
            (datetime(2017, 1, 7), [0, 1, 4]),
            (datetime(2017, 1, 8), [1, 0, 0]),
            (datetime(2017, 1, 9), [1, 0, 1]),
            (datetime(2017, 1, 9), [1, 0, 2]),
            (datetime(2017, 1, 10), [1, 0, 3]),
            (datetime(2017, 1, 11), [1, 0, 4]),
            (datetime(2017, 1, 11), [1, 0, 5]),
            (datetime(2017, 1, 12), [1, 0, 6]),
            (datetime(2017, 1, 12), [1, 0, 7]),
            (datetime(2017, 1, 12), [1, 0, 8])
        ]
        expecting = [e for _, e in data]

        for cut in range(1, len(data)+1, 1):
            c = DurationCounter(
                start=datetime(2017, 1, 1),
                duration="week",
                child=BatchCounter(
                    start=0,
                    size=10,
                    child=Counter(0)
                )
            )

            resultA = [c.next((d, Null)) for d, _ in data[:cut]]
            cut_point = resultA[-1]

            c = DurationCounter(
                start=datetime(2017, 1, 1),
                duration="week",
                child=BatchCounter(
                    start=0,
                    size=10,
                    child=Counter(0)
                )
            )
            c.reset(cut_point)

            resultB = [c.next((d, Null)) for d, _ in data[cut-1:]][1:]  # REPLAY LAST VALUE, AND IGNORE IT

            result = resultA + resultB

            self.assertEqual(result, expecting, "Expecting counter")


    def test_day_3(self):
        data = [
            # (value, expected) PAIRS
            (datetime(2017, 1, 1), [0, 0, 0]),
            (datetime(2017, 1, 1), [0, 0, 1]),
            (datetime(2017, 1, 1), [0, 0, 2]),
            (datetime(2017, 1, 2), [1, 0, 0]),
            (datetime(2017, 1, 2), [1, 0, 1]),
            (datetime(2017, 1, 2), [1, 0, 2]),
            (datetime(2017, 1, 2), [1, 1, 0]),
            (datetime(2017, 1, 2), [1, 1, 1]),
            (datetime(2017, 1, 2), [1, 1, 2]),
            (datetime(2017, 1, 2), [1, 2, 0]),
            (datetime(2017, 1, 3), [2, 0, 0]),
            (datetime(2017, 1, 4), [3, 0, 0]),
            (datetime(2017, 1, 5), [4, 0, 0]),
            (datetime(2017, 1, 6), [5, 0, 0]),
            (datetime(2017, 1, 7), [6, 0, 0]),
            (datetime(2017, 1, 8), [7, 0, 0]),
            (datetime(2017, 1, 9), [8, 0, 0]),
            (datetime(2017, 1, 9), [8, 0, 1]),
            (datetime(2017, 1, 10), [9, 0, 0]),
            (datetime(2017, 1, 11), [10, 0, 0]),
            (datetime(2017, 1, 11), [10, 0, 1]),
            (datetime(2017, 1, 12), [11, 0, 0]),
            (datetime(2017, 1, 12), [11, 0, 1]),
            (datetime(2017, 1, 12), [11, 0, 2])
        ]

        c = DurationCounter(
            start=datetime(2017, 1, 1),
            duration="day",
            child=BatchCounter(
                start=0,
                size=3,
                child=Counter(0)
            )
        )

        result = [c.next((d, Null)) for d, _ in data]
        expecting = [e for _, e in data]

        self.assertEqual(result, expecting, "Expecting counter")



