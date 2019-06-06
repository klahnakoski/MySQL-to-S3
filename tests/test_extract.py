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

from mo_dots import set_default, wrap, Null
from mo_files import File
from mo_logs import Log, startup, constants
from mo_testing.fuzzytestcase import FuzzyTestCase
from mo_times.timer import Timer
from mysql_to_s3 import snowflake_schema
from mysql_to_s3.extract import Extract
from pyLibrary.sql.mysql import MySQL, execute_file

settings = startup.read_settings(filename="tests/resources/config/test.json")
constants.set(settings.constants)


class TestExtract(FuzzyTestCase):
    @classmethod
    def setUpClass(cls):
        Log.start(settings.debug)
        snowflake_schema.DEBUG = (True,)

        with Timer("setup database"):
            try:
                with MySQL(schema=None, kwargs=settings.database) as db:
                    db.query("drop database testing")
            except Exception as e:
                if "Can't drop database " in e:
                    pass
                else:
                    Log.warning("problem removing db", cause=e)
            execute_file(
                "tests/resources/database.sql", schema=None, kwargs=settings.database
            )

    def setUp(self):
        pass

    def test_simple(self):
        config = config_template
        db = MySQL(**config.snowflake.database)
        with db:
            Extract(kwargs=config).extract(
                db=db, start_point=Null, first_value=Null, data=[22], please_stop=Null
            )

        result = File(filename).read_json()
        result[0].etl = None
        expected = expected_results["simple"]
        self.assertEqual(result, expected, "expecting identical")
        self.assertEqual(expected, result, "expecting identical")

    def test_complex(self):
        config = config_template
        db = MySQL(**config.snowflake.database)
        with db:
            Extract(kwargs=config).extract(
                db=db, start_point=Null, first_value=Null, data=[10], please_stop=Null
            )

        result = File(filename).read_json()
        result[0].etl = None
        expected = expected_results["complex"]
        self.assertEqual(result, expected, "expecting identical")
        self.assertEqual(expected, result, "expecting identical")

    def test_inline(self):
        config = set_default(
            {"snowflake": {"reference_only": ["inner1.value", "inner2.value"]}},
            config_template,
        )
        db = MySQL(**config.snowflake.database)
        with db:
            Extract(kwargs=config).extract(
                db=db, start_point=Null, first_value=Null, data=[10], please_stop=Null
            )

        result = File(filename).read_json()
        result[0].etl = None
        expected = expected_results["inline"]
        self.assertEqual(result, expected, "expecting identical")
        self.assertEqual(expected, result, "expecting identical")

    def test_lean(self):
        config = set_default(
            {"snowflake": {"show_foreign_keys": False}}, config_template
        )
        db = MySQL(**config.snowflake.database)
        with db:
            Extract(kwargs=config).extract(
                db=db, start_point=Null, first_value=Null, data=[10], please_stop=Null
            )

        result = File(filename).read_json()
        result[0].etl = None
        expected = expected_results["lean"]
        self.assertEqual(result, expected, "expecting identical")
        self.assertEqual(expected, result, "expecting identical")

    def test_lean_inline(self):
        config = set_default(
            {
                "snowflake": {
                    "show_foreign_keys": False,
                    "reference_only": ["inner1.value", "inner2.value"],
                }
            },
            config_template,
        )
        db = MySQL(**config.snowflake.database)
        with db:
            Extract(kwargs=config).extract(
                db=db, start_point=Null, first_value=Null, data=[10], please_stop=Null
            )

        result = File(filename).read_json()
        result[0].etl = None
        expected = expected_results["lean_inline"]
        self.assertEqual(result, expected, "expecting identical")
        self.assertEqual(expected, result, "expecting identical")

    def test_lean_inline_all(self):
        config = set_default(
            {
                "extract": {"ids": "select * from fact_table"},
                "snowflake": {
                    "show_foreign_keys": False,
                    "reference_only": ["inner1.value", "inner2.value"],
                },
            },
            config_template,
        )
        db = MySQL(**config.snowflake.database)
        data = [10, 11, 12, 13, 15, 16, 17, 18, 19, 20, 21, 22]
        with db:
            Extract(kwargs=config).extract(
                db=db, start_point=Null, first_value=Null, data=data, please_stop=Null
            )

        result = File(filename).read_json()
        for r in result:
            r.etl = None
        expected = expected_results["lean_inline_all"]
        self.assertEqual(result, expected, "expecting identical")
        self.assertEqual(expected, result, "expecting identical")

    def test_rename(self):
        config = set_default(
            {"snowflake": {"show_foreign_keys": False, "exclude_path": "id.bad_name"}},
            config_template,
        )

        db = MySQL(**config.snowflake.database)
        data = [10]
        with db:
            Extract(kwargs=config).extract(
                db=db, start_point=Null, first_value=Null, data=data, please_stop=Null
            )

        result = File(filename).read_json()
        for r in result:
            r.etl = None
        expected = expected_results["rename"]
        self.assertEqual(result, expected, "expecting identical")
        self.assertEqual(expected, result, "expecting identical")


filename = "tests/output/test_output.json"

config_template = wrap(
    {
        "extract": {
            "last": "tests/output/test_run.json",
            "field": "id",
            "type": "number",
            "start": 0,
            "batch": 100,
        },
        "destination": filename,
        "snowflake": {
            "fact_table": "fact_table",
            "show_foreign_keys": True,
            "null_values": ["-", "unknown", ""],
            "add_relations": [],
            "name_relations": {
                "fact_table.id <- nested1.ref": "interesting",
                "fact_table.bad_name -> inner1.id": "more",
            },
            "include": [],
            "exclude": [],
            "exclude_path": "id.more",
            "reference_only": ["inner1", "inner2"],
            "database": settings.database,
        },
        "debug": {"trace": True},
    }
)

expected_results = {
    "simple": [{"fact_table": {"id": 22, "name": "L"}}],
    "lean_inline": [
        {
            "fact_table": {
                "about": "a",
                "id": 10,
                "name": "A",
                "interesting": {
                    "about": 0,
                    "description": "aaa",
                    "nested2": [
                        {"about": "a", "minutia": 3.1415926539},
                        {"about": "b", "minutia": 4},
                        {"about": "c", "minutia": 5.1},
                    ],
                },
            }
        }
    ],
    "lean": [
        {
            "fact_table": {
                "about": {"value": "a", "time": {"value": 0}},
                "id": 10,
                "name": "A",
                "interesting": {
                    "about": {"value": 0},
                    "description": "aaa",
                    "nested2": [
                        {
                            "about": {"value": "a", "time": {"value": 0}},
                            "minutia": 3.1415926539,
                        },
                        {"about": {"value": "b"}, "minutia": 4},
                        {"about": {"value": "c"}, "minutia": 5.1},
                    ],
                },
            }
        }
    ],
    "complex": [
        {
            "fact_table": {
                "about": {"id": 1, "time": {"id": -1, "value": 0}, "value": "a"},
                "id": 10,
                "name": "A",
                "interesting": {
                    "about": {"id": -1, "value": 0},
                    "description": "aaa",
                    "id": 100,
                    "nested2": [
                        {
                            "about": {
                                "id": 1,
                                "time": {"id": -1, "value": 0},
                                "value": "a",
                            },
                            "id": 1000,
                            "minutia": 3.1415926539,
                            "ref": 100,
                        },
                        {
                            "about": {"id": 2, "time": {"id": -2}, "value": "b"},
                            "id": 1001,
                            "minutia": 4,
                            "ref": 100,
                        },
                        {
                            "about": {"id": 3, "value": "c"},
                            "id": 1002,
                            "minutia": 5.1,
                            "ref": 100,
                        },
                    ],
                    "ref": 10,
                },
            }
        }
    ],
    "inline": [
        {
            "fact_table": {
                "about": {"id": 1, "value": "a"},
                "id": 10,
                "name": "A",
                "interesting": {
                    "about": {"id": -1, "value": 0},
                    "ref": 10,
                    "description": "aaa",
                    "nested2": [
                        {
                            "about": {"id": 1, "value": "a"},
                            "ref": 100,
                            "id": 1000,
                            "minutia": 3.1415926539,
                        },
                        {
                            "about": {"id": 2, "value": "b"},
                            "ref": 100,
                            "id": 1001,
                            "minutia": 4,
                        },
                        {
                            "about": {"id": 3, "value": "c"},
                            "ref": 100,
                            "id": 1002,
                            "minutia": 5.1,
                        },
                    ],
                    "id": 100,
                },
            }
        }
    ],
    "lean_inline_all": [
        {
            "fact_table": {
                "interesting": {
                    "about": 0,
                    "description": "aaa",
                    "nested2": [
                        {"about": "a", "minutia": 3.1415926539},
                        {"about": "b", "minutia": 4},
                        {"about": "c", "minutia": 5.1},
                    ],
                },
                "about": "a",
                "id": 10,
                "name": "A",
            }
        },
        {
            "fact_table": {
                "interesting": {
                    "description": "bbb",
                    "nested2": {"about": "a", "minutia": 6.2},
                },
                "about": "b",
                "id": 11,
                "name": "B",
            }
        },
        {
            "fact_table": {
                "interesting": {
                    "description": "ccc",
                    "nested2": {"about": "c", "minutia": 7.3},
                },
                "about": "c",
                "id": 12,
                "name": "C",
            }
        },
        {
            "fact_table": {
                "interesting": {"about": 0, "description": "ddd"},
                "id": 13,
                "name": "D",
            }
        },
        {
            "fact_table": {
                "interesting": [
                    {"about": 0, "description": "eee"},
                    {"about": 0, "description": "fff"},
                ],
                "about": "a",
                "id": 15,
                "name": "E",
            }
        },
        {
            "fact_table": {
                "interesting": [{"description": "ggg"}, {"description": "hhh"}],
                "about": "b",
                "id": 16,
                "name": "F",
            }
        },
        {
            "fact_table": {
                "interesting": [{"description": "iii"}, {"description": "jjj"}],
                "about": "c",
                "id": 17,
                "name": "G",
            }
        },
        {
            "fact_table": {
                "interesting": [{"description": "kkk"}, {"description": "lll"}],
                "id": 18,
                "name": "H",
            }
        },
        {"fact_table": {"about": "a", "id": 19, "name": "I"}},
        {"fact_table": {"about": "b", "id": 20, "name": "J"}},
        {"fact_table": {"about": "c", "id": 21, "name": "K"}},
        {"fact_table": {"id": 22, "name": "L"}},
    ],
    "rename": [
        {
            "fact_table": {
                "about": {"value": "a", "time": {"value": 0}},
                "more": {"value": "b"},
                "id": 10,
                "name": "A",
                "interesting": {
                    "about": {"value": 0},
                    "description": "aaa",
                    "nested2": [
                        {
                            "about": {"value": "a", "time": {"value": 0}},
                            "minutia": 3.1415926539,
                        },
                        {"about": {"value": "b"}, "minutia": 4},
                        {"about": {"value": "c"}, "minutia": 5.1},
                    ],
                },
            }
        }
    ],
}
