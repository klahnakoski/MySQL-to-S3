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

from mo_logs import Log
from mo_times import Date, Duration
from mo_math import round

class Counter(object):
    def __init__(self, start):
        self.count = start

    def next(self, value=None):
        output = self.count
        self.count += 1
        return [output]

    def reset(self, start=None):
        if start:
            self.count = start[0]
        else:
            self.count = 0

class BatchCounter(object):
    def __init__(self, start, size, child):
        self.next_output = start
        self.size = size
        self.child = child

    def next(self, value):
        output = self.next_output
        c = self.child.next(value[1:])
        if c[0] >= self.size-1:
            self.next_output += 1
            self.child.reset()
        return [output]+c

    def reset(self, start=None):
        if start:
            self.next_output = start[0]
            self.child.reset(start[1:])
        else:
            self.next_output = 0
            self.child.reset()


class DurationCounter(object):

    def __init__(self, start, duration, child):
        self.duration = Duration(duration)
        self.start = self.last_value = Date(start).floor(self.duration)
        self.batch = 0
        self.child = child

    def next(self, value):
        v = Date(value[0])
        if self.last_value.floor(self.duration) > v:
            Log.error("Expecting strictly increasing")
        self.last_value = v

        key = round((v.floor(self.duration)-self.start)/self.duration, decimal=0)
        if key != self.batch:
            self.child.reset()
            self.batch = key

        c = self.child.next(value[1:])
        return [self.batch] + c

    def reset(self, start=None):
        if start:
            self.batch = start[0]
            self.child.reset(start[1:])
        else:
            self.start = Date.MIN
            self.child.reset()

