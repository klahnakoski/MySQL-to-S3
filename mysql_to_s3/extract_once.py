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

from contextlib import closing

from jx_elasticsearch import elasticsearch
from jx_mysql.mysql import MySQL, quote_column, quote_list, ConcatSQL
from jx_mysql.mysql_snowflake_extractor import MySqlSnowflakeExtractor
from jx_python import jx
from mo_dots import Data, coalesce, set_default
from mo_files import File
from mo_future import text, is_text
from mo_json import value2json
from mo_kwargs import override
from mo_logs import Log, startup, constants, machine_metadata
from mo_logs.strings import expand_template
from mo_sql import SQL_SELECT, SQL_FROM, SQL_WHERE, SQL_IN
from mo_threads import Signal, Thread, Queue
from mo_times import Date
from mo_times.timer import Timer
from mysql_to_s3.utils import check_database
from pyLibrary.env.git import get_revision

DEBUG = True


class ExtractOnce(object):
    @override
    def __init__(self, kwargs=None):
        self.settings = kwargs
        self.schema = MySqlSnowflakeExtractor(self.settings.snowflake)
        self._extract = extract = kwargs.extract

        # SOME PREP
        get_revision()

        check_database(kwargs.snowflake.database, DEBUG)

        extract.threads = coalesce(extract.threads, 1)
        self.done_pulling = Signal()
        self.queue = Queue(
            "all batches", max=2 * coalesce(extract.threads, 1), silent=True
        )

        if not is_text(self.settings.destination):
            Log.error("caon only handle filename for destination")

        Thread.run("get records", self.get_all_ids)

    def get_all_ids(self, please_stop):
        with MySQL(**self.settings.snowflake.database) as db:
            sql = expand_template(self.settings.extract.ids, self.settings.extract)
            Log.note("SQL: {{sql}}", sql=sql)

            with Timer("Grab ids for processing"):
                with closing(db.db.cursor()) as cursor:
                    cursor.execute(text(sql))
                    for batch, ids in jx.chunk(
                        (row[0] for row in cursor), self.settings.extract.batch
                    ):
                        self.queue.add({"batch": batch, "ids": ids})
                    else:
                        Log.alert("NO RECORDS")

    def extract(self, db, batch, ids, please_stop):

        fact_table = self.settings.snowflake.fact_table

        id = quote_column(self._extract.field)
        ids_sql = ConcatSQL(
            [
                SQL_SELECT,
                id,
                SQL_FROM,
                quote_column(self.settings.snowflake.fact_table),
                SQL_WHERE,
                id,
                SQL_IN,
                quote_list(ids),
            ]
        )
        sql = self.schema.get_sql(ids_sql)

        # Log.note("{{sql}}", sql=sql)

        # WRITE PRETTY JSON TO FILE

        parent_etl = Data(
            batch=batch, revision=get_revision(), machine=machine_metadata
        )

        with Timer("Sending SQL"):
            cursor = db.query(sql, stream=True, row_tuples=True)
            data = []
            self.schema.construct_docs(cursor, data.append, please_stop)
            for i, d in enumerate(data):
                destination = File(self.settings.destination).add_suffix(
                    d[self.settings.extract.field]
                )
                destination.write(
                    value2json(
                        set_default(
                            {
                                "etl": {
                                    "id": i,
                                    "source": parent_etl,
                                    "timestamp": Date.now(),
                                }
                            },
                            elasticsearch.scrub(d),
                        )
                    )
                )


def main():
    try:
        settings = startup.read_settings()
        with startup.SingleInstance(settings.args.filename):
            constants.set(settings.constants)
            Log.start(settings.debug)

            extractor = ExtractOnce(settings)

            def extract(please_stop):
                with MySQL(**settings.snowflake.database) as db:
                    with db.transaction():
                        for kwargs in extractor.queue:
                            if please_stop:
                                break
                            try:
                                extractor.extract(
                                    db=db, please_stop=please_stop, **kwargs
                                )
                            except Exception as e:
                                Log.warning("Could not extract", cause=e)
                                extractor.queue.add(kwargs)

            for i in range(settings.extract.threads):
                Thread.run("extract #" + text(i), extract)

            please_stop = Signal()
            Thread.current().wait_for_shutdown_signal(
                please_stop=please_stop, allow_exit=True, wait_forever=False
            )
    except Exception as e:
        Log.warning("Problem with data extraction", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
