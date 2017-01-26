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

from MoLogs import strings
from pyDots import wrap
from pyLibrary.aws import s3


def _key2etl(key):
    """
    CONVERT S3 KEY TO ETL HEADER

    S3 NAMING CONVENTION: a.b.c WHERE EACH IS A STEP IN THE ETL PROCESS
    HOW TO DEAL WITH a->b AS AGGREGATION?  b:a.c?   b->c is agg: a.c:b
    """
    key = s3.strip_extension(key)

    tokens = []
    s = 0
    i = strings.find(key, [":", "."])
    while i < len(key):
        tokens.append(key[s:i])
        tokens.append(key[i])
        s = i + 1
        i = strings.find(key, [":", "."], s)
    tokens.append(key[s:i])

    _reverse_aggs(tokens)
    # tokens.reverse()
    source = {
        "id": format_id(tokens[0])
    }
    for i in range(2, len(tokens), 2):
        source = {
            "id": format_id(tokens[i]),
            "source": source,
            "type": "join" if tokens[i - 1] == "." else "agg"
        }
    return wrap(source)


def _reverse_aggs(seq):
    # SHOW AGGREGATION IN REVERSE ORDER (ASSUME ONLY ONE)
    for i in range(1, len(seq), 2):
        if seq[i] == ":":
            seq[i - 1], seq[i + 1] = seq[i + 1], seq[i - 1]


def format_id(value):
    """
    :param value:
    :return: int() IF POSSIBLE
    """
    try:
        return int(value)
    except Exception:
        return unicode(value)



def lt(l, r):
    """
    :param l: left key
    :param r: right key
    :return: True if l<r
    """
    if r is None or l is None:
        return True
    for ll, rr in zip(l, r):
        if ll < rr:
            return True
        elif ll > rr:
            return False
    return False

