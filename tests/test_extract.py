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
from pyDots import set_default
from pyLibrary.env.files import File
from pyLibrary.sql.mysql import MySQL
from pyLibrary.testing.fuzzytestcase import FuzzyTestCase
from pyLibrary.times.timer import Timer

settings = startup.read_settings(filename="tests/resources/config/test.json")
constants.set(settings.constants)


class TestExtract(FuzzyTestCase):

    @classmethod
    def setUpClass(cls):
        Log.start(settings.debug)
        with Timer("setup database"):
            try:
                with MySQL(settings.database) as db:
                    db.query("drop database testing")
            except Exception, e:
                if "Can't drop database " in e:
                    pass
                else:
                    Log.warning("problem removing db", cause=e)
            MySQL.execute_file("tests/resources/database.sql", settings=settings.database)

    def setUp(self):
        pass

    def test_simple(self):
        config = set_default({"extract": {"ids": "select 22 id"}}, config_template)
        Extract(config).extract()

        result = File(filename).read_json()
        expected = [expected_results[22]]
        self.assertEqual(result, expected, "expecting identical")

    def test_complex(self):
        config = set_default({"extract": {"ids": "select 10 id"}}, config_template)
        Extract(config).extract()

        result = File(filename).read_json()
        expected = [expected_results[10]]
        self.assertEqual(result, expected, "expecting identical")


filename = "tests/output/test_output.json"

config_template = {
    "extract": {
        "last": "tests/output/test_run.json",
        "field": "id",
        "type": "number",
        "start": 0,
        "batch": 10
    },
    "destination": filename,
    "snowflake": {
        "fact_table": "fact_table",
        "show_foreign_keys": True,
        "null_values": [
            "-",
            "unknown",
            ""
        ],
        "add_relations": [],
        "include": [],
        "exclude": [],
        "reference_only": [
            "inner1",
            "inner2"
        ],
        "database": settings.database
    },
    "debug": {
        "trace": True
    }
}


expected_results = {
    10: {"fact_table": {
                "about": {"id": 1, "time": {"id": -1, "value": 0}, "value": "a"},
                "id": 10,
                "name": "A",
                "nested1": [{
                    "about": {"id": -1, "value": 0},
                    "description": "aaa",
                    "id": 100,
                    "nested2": [
                        {
                            "about": {"id": 1, "time": {"id": -1, "value": 0}, "value": "a"},
                            "id": 1000,
                            "minutia": 3.1415926539,
                            "ref": 100
                        },
                        {
                            "about": {"id": 2, "time": {"id": -2}, "value": "b"},
                            "id": 1001,
                            "minutia": 4,
                            "ref": 100
                        },
                        {
                            "about": {"id": 3, "value": "c"},
                            "id": 1002,
                            "minutia": 5.1,
                            "ref": 100
                        }
                    ],
                    "ref": 10
                }]
            }}



}
