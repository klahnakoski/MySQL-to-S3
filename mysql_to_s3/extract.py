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
from jx_mysql import MySQL, quote_column, quote_value
from jx_mysql.mysql import sql_call
from jx_mysql.mysql_snowflake_extractor import MySqlSnowflakeExtractor
from mo_dots import Null, listwrap, coalesce
from mo_dots.lists import last
from mo_files import File, TempFile
from mo_future import text, is_text
from mo_kwargs import override
from mo_logs import Log, startup, constants, machine_metadata
from mo_threads import Signal, Thread, Queue, THREAD_STOP
from mo_times import Date, Duration
from mo_times.timer import Timer
from mysql_to_s3.counter import Counter, DurationCounter, BatchCounter
from mysql_to_s3.utils import check_database
from pyLibrary import convert, aws
from pyLibrary.aws import s3
from pyLibrary.env.git import get_revision
from mo_sql import (
    SQL,
    sql_list,
    SQL_LIMIT,
    SQL_ORDERBY,
    SQL_WHERE,
    SQL_FROM,
    SQL_SELECT,
    SQL_OR,
    sql_iso,
    SQL_TRUE,
    SQL_AND,
    SQL_AS,
    ConcatSQL,
    SQL_GT,
    SQL_GE,
    SQL_EQ,
    SQL_LT,
    SQL_IN,
)

DEBUG = True


class Extract(object):
    @override
    def __init__(self, kwargs=None):
        self.settings = kwargs
        self.schema = MySqlSnowflakeExtractor(self.settings.snowflake)
        self._extract = extract = kwargs.extract

        # SOME PREP
        get_revision()

        check_database(kwargs.snowflake.database, DEBUG)

        extract.type = listwrap(extract.type)
        extract.start = listwrap(extract.start)
        extract.batch = listwrap(extract.batch)
        extract.field = listwrap(extract.field)
        if any(
            len(extract.type) != len(other)
            for other in [extract.start, extract.batch, extract.field]
        ):
            Log.error(
                "Expecting same number of dimensions for `type`, `start`, `batch`, and `field` in the `extract` inner object"
            )
        for i, t in enumerate(extract.type):
            if t == "time":
                extract.start[i] = Date(extract.start[i])
                extract.batch[i] = Duration(extract.batch[i])
            elif t == "number":
                pass
            else:
                Log.error('Expecting `extract.type` to be "number" or "time"')

        extract.threads = coalesce(extract.threads, 1)
        self.done_pulling = Signal()
        self.queue = Queue(
            "all batches", max=2 * coalesce(extract.threads, 1), silent=True
        )

        if not is_text(self.settings.destination):
            self.bucket = s3.Bucket(self.settings.destination)

        if self.settings.notify:
            self.notify = aws.Queue(self.settings.notify)
        else:
            self.notify = Null

        Thread.run("get records", self.pull_all_remaining)

    def pull_all_remaining(self, please_stop):
        try:
            try:
                content = File(self.settings.extract.last).read_json()
                if len(content) == 1:
                    Log.note(
                        "Got a manually generated file {{filename}}",
                        filename=self.settings.extract.last,
                    )
                    first_value = list(
                        (listwrap(content[0]) + DUMMY_LIST)[: len(self._extract.type) :]
                    )
                    start_point = tuple(first_value)
                    #     Date(s) + Duration(b) * c if t == 'time' else s + b * c
                    #     for t, b, s, c in zip(self._extract.type, self._extract.batch, self._extract.start, first_value)
                    # )
                else:
                    Log.note(
                        "Got a machine generated file {{filename}}",
                        filename=self.settings.extract.last,
                    )
                    start_point, first_value = content
                    start_point = tuple(start_point)
                Log.note("First value is {{start|json}}", start=first_value)
            except Exception as e:
                Log.error(
                    "Expecting a file {{filename}} with the last good S3 bucket etl id in array form eg: [[954, 0]]",
                    filename=self.settings.extract.last,
                    cause=e,
                )
                start_point = tuple(self._extract.start)
                first_value = Null

            counter = Counter(start=0)
            for t, s, b, f, i in reversed(
                zip(
                    self._extract.type,
                    self._extract.start,
                    self._extract.batch,
                    listwrap(first_value) + DUMMY_LIST,
                    range(len(self._extract.start)),
                )
            ):
                if t == "time":
                    counter = DurationCounter(start=s, duration=b, child=counter)
                    first_value[i] = Date(s) + Duration(b) * f
                else:
                    counter = BatchCounter(start=s, size=b, child=counter)

            batch_size = self._extract.batch.last() * 2 * self.settings.extract.threads
            with MySQL(**self.settings.snowflake.database) as db:
                while not please_stop:
                    sql = self._build_list_sql(
                        db, first_value, batch_size + self._extract.batch.last()
                    )
                    pending = []
                    counter.reset(start_point)
                    with Timer("Grab a block of ids for processing"):
                        with closing(db.db.cursor()) as cursor:
                            acc = []
                            Log.note("SQL: {{sql}}", sql=sql)
                            cursor.execute(text(sql))
                            count = 0
                            for row in cursor:
                                detail_key = counter.next(row)
                                key = tuple(detail_key[:-1])
                                count += 1
                                if key != start_point:
                                    if first_value:
                                        if not acc:
                                            Log.error(
                                                "not expected, {{filename}} is probably set too far in the past",
                                                filename=self.settings.extract.last,
                                            )
                                        pending.append(
                                            {
                                                "start_point": start_point,
                                                "first_value": first_value,
                                                "data": acc,
                                            }
                                        )
                                    acc = []
                                    start_point = key
                                    first_value = row
                                acc.append(
                                    row[-1]
                                )  # ASSUME LAST COLUMN IS THE FACT TABLE id
                    Log.note("adding {{num}} for processing", num=len(pending))
                    self.queue.extend(pending)

                    if count < batch_size:
                        self.queue.add(THREAD_STOP)
                        break
        except Exception as e:
            Log.warning("Problem pulling data", cause=e)
        finally:
            self.done_pulling.go()
            Log.note("pulling new data is done")

    def _build_list_sql(self, db, first, batch_size):
        # TODO: ENSURE THE LAST COLUMN IS THE id
        if first:
            # BUILD LOGIC FOR TUPLE COMPARISION:  self._extract.field >= first
            dim = len(self._extract.field)
            where = SQL_OR.join(
                sql_iso(
                    SQL_AND.join(
                        ConcatSQL(
                            (
                                quote_column(f),
                                ineq(i, e, dim),
                                quote_value(Date(v) if t == "time" else v),
                            )
                        )
                        for e, (f, v, t) in enumerate(
                            zip(
                                self._extract.field[0 : i + 1 :],
                                first,
                                self._extract.type[0 : i + 1 :],
                            )
                        )
                    )
                )
                for i in range(dim)
            )
        else:
            where = SQL_TRUE

        # ADD QUERY LIMIT - FOR CHUNKS THIS IS A SIMPLE LIMIT
        # DURATIONS REQUIRE A WHERE CLAUSE
        if last(self._extract.type) != "time":
            limit = SQL_LIMIT + quote_value(batch_size)
        else:
            where += ConcatSQL(
                (
                    SQL_AND,
                    quote_column(last(self._extract.field)),
                    SQL_LT,
                    quote_value((Date(last(first)) + Duration(batch_size))),
                )
            )
            limit = ""

        selects = []
        for t, f in zip(self._extract.type, self._extract.field):
            if t == "time":
                selects.append(
                    sql_call(
                        "CAST",
                        (ConcatSQL((quote_column(f), SQL_AS, SQL("DATETIME(6)"))),),
                    )
                )
            else:
                selects.append(quote_column(f))
        sql = ConcatSQL(
            (
                SQL_SELECT,
                sql_list(selects),
                SQL_FROM,
                self.settings.snowflake.fact_table,
                SQL_WHERE,
                where,
                SQL_ORDERBY,
                sql_list(quote_column(f) for f in self._extract.field),
                limit,
            )
        )

        return sql

    def extract(self, db, start_point, first_value, data, please_stop):
        Log.note(
            "Starting scan of {{table}} at {{id}} and sending to batch {{start_point}}",
            table=self.settings.snowflake.fact_table,
            id=first_value,
            start_point=start_point,
        )

        id = quote_column(self._extract.field.last())
        ids = ConcatSQL(
            (
                SQL_SELECT,
                id,
                SQL_FROM,
                self.settings.snowflake.fact_table,
                SQL_WHERE,
                id,
                SQL_IN,
                sql_iso(sql_list(map(quote_value, data))),
            )
        )
        sql = self.schema.get_sql(ids)

        with Timer("Sending SQL"):
            cursor = db.query(sql, stream=True, row_tuples=True)

        extract = self.settings.extract
        fact_table = self.settings.snowflake.fact_table

        with TempFile() as temp_file:
            parent_etl = Null
            for s in start_point:
                parent_etl = {"id": s, "source": parent_etl}
            parent_etl["revision"] = get_revision()
            parent_etl["machine"] = machine_metadata

            counter = Counter(0)

            def append(value):
                """
                :param value: THE DOCUMENT TO ADD
                :return: PleaseStop
                """
                temp_file.append(
                    convert.value2json(
                        {
                            fact_table: elasticsearch.scrub(value),
                            "etl": {
                                "id": counter.count,
                                "source": parent_etl,
                                "timestamp": Date.now(),
                            },
                        }
                    )
                )
                counter.next()

            with Timer("assemble data"):
                self.schema.construct_docs(cursor, append, please_stop)

            s3_file_name = ".".join(map(text, start_point))
            with Timer(
                "write to destination {{filename}}", param={"filename": s3_file_name}
            ):
                if not isinstance(self.settings.destination, text):
                    # WRITE JSON LINES TO S3
                    destination = self.bucket.get_key(s3_file_name, must_exist=False)
                    destination.write_lines(temp_file)
                else:
                    # WRITE PRETTY JSON TO FILE
                    destination = File(self.settings.destination).add_suffix(
                        s3_file_name
                    )
                    destination.write(
                        convert.value2json(
                            [convert.json2value(o) for o in temp_file], pretty=True
                        )
                    )
                    return False

        # NOTIFY SQS
        now = Date.now()
        self.notify.add(
            {
                "bucket": self.settings.destination.bucket,
                "key": s3_file_name,
                "timestamp": now.unix,
                "date/time": now.format(),
            }
        )

        # SUCCESS!!
        File(extract.last).write(convert.value2json([start_point, first_value]))


def main():
    try:
        settings = startup.read_settings()
        with startup.SingleInstance(settings.args.filename):
            constants.set(settings.constants)
            Log.start(settings.debug)

            extractor = Extract(settings)

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


DUMMY_LIST = [Null] * 5


def ineq(i, e, dim):
    if e < i:
        return SQL_EQ
    elif i == dim - 1:
        return SQL_GE
    else:
        return SQL_GT


if __name__ == "__main__":
    main()
