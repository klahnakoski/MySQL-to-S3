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

from MoLogs import Log, startup, constants
from mysql_to_s3.extract import Extract
from pyLibrary.sql.mysql import MySQL
from pyLibrary.testing.fuzzytestcase import FuzzyTestCase
from pyLibrary.times.timer import Timer

settings = startup.read_settings(filename="tests/resources/config/test.json")
constants.set(settings.constants)


class TestExtract(FuzzyTestCase):
    @classmethod
    def setUpClass(cls):
        Log.start(settings.debug)

    def setUp(self):
        with Timer("setup database"):
            MySQL.execute_file("tests/resources/database.sql", settings=settings.database)

    def test_general(self):
        db = MySQL(settings.database)

        config = {
            "extract": {
                "last": "tests/output/test_run.json",
                "field": "id",
                "type": "number",
                "start": 0,
                "batch": 10,
                "ids": "select id from fact_table"
            },
            "destination": "tests/output/test_output.json",
            "snowflake": {
                "fact_table": "fact_table",
                "show_foreign_keys": False,
                "null_values": [
                    "-",
                    "unknown",
                    ""
                ],
                "add_relations": [],
                "include": [],
                "exclude": [],
                "reference_only": [
                    "inner2.value"
                ],
                "database": settings.database
            },
            "debug": {
                "trace": True
            }
        }
        e = Extract(config)
        while e.extract():
            pass

        # VERIFY FILE CONTENTS
