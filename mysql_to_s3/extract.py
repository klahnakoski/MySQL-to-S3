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
from tempfile import NamedTemporaryFile

from MoLogs import Log, startup, constants, machine_metadata
from mysql_to_s3.counter import Counter, DurationCounter, BatchCounter
from mysql_to_s3.snowflake_schema import SnowflakeSchema
from pyDots import Data, wrap, Null, listwrap, unwrap, relative_field
from pyLibrary import convert
from pyLibrary.aws import s3
from pyLibrary.env import elasticsearch
from pyLibrary.env.files import File
from pyLibrary.env.git import get_git_revision
from pyLibrary.meta import use_settings
from pyLibrary.queries import jx
from pyLibrary.sql import SQL
from pyLibrary.sql.mysql import MySQL
from pyLibrary.thread.signal import Signal
from pyLibrary.thread.threads import Thread, Queue
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import Duration
from pyLibrary.times.timer import Timer

DEBUG = True


class Extract(object):

    @use_settings
    def __init__(self, settings=None):
        self.settings = settings
        self.schema = SnowflakeSchema(self.settings.snowflake)
        self._extract = extract = settings.extract

        # SOME PREP
        get_git_revision()

        # VERIFY WE DO NOT HAVE TOO MANY OTHER PROCESSES WORKING ON STUFF
        with closing(MySQL(**settings.snowflake.database)) as db:
            processes = None
            try:
                processes = jx.filter(db.query("show processlist"), {"and": [{"neq": {"Command": "Sleep"}}, {"neq": {"Info": "show processlist"}}]})
            except Exception, e:
                Log.warning("no database", cause=e)
            # if processes:
            #     Log.error("Processes are running\n{{list|json}}", list=processes)

        extract.type = listwrap(extract.type)
        extract.start = listwrap(extract.start)
        extract.batch = listwrap(extract.batch)
        extract.field = listwrap(extract.field)
        if any(len(extract.type) != len(other) for other in [extract.start, extract.batch, extract.field]):
            Log.error("Expecting same number of dimensions for `type`, `start`, `batch`, and `field` in the `extract` inner object")
        for i, t in enumerate(extract.type):
            if t == "time":
                extract.start[i] = Date(extract.start[i])
                extract.batch[i] = Duration(extract.batch[i])
            elif t == "number":
                pass
            else:
                Log.error('Expecting `extract.type` to be "number" or "time"')

        self.queue = Queue("all batches", max=10, silent=True)
        Thread.run("get records", self.pull_all_remaining)

    def pull_all_remaining(self, please_stop):
        counter = Counter(start=0)
        for t, s, b in reversed(zip(self._extract.type, self._extract.start, self._extract.batch)):
            if t == "time":
                counter = DurationCounter(start=s, duration=b, child=counter)
            else:
                counter = BatchCounter(start=s, size=b, child=counter)
        try:
            start_point, first_value = File(self.settings.extract.last).read_json()
            start_point = tuple(start_point)
        except Exception, _:
            start_point = tuple(self._extract.start)
            first_value = Null
        counter.reset(start_point)

        batch_size = self._extract.batch.last() * 10
        with closing(MySQL(**self.settings.snowflake.database)) as db:
            while not please_stop:
                sql = self._build_list_sql(db, first_value, batch_size + 1)
                with Timer("Grab a block of ids for processing"):
                    with closing(MySQL(**self.settings.snowflake.database)) as db:
                        with closing(db.db.cursor()) as cursor:
                            acc = []
                            cursor.execute(sql)
                            count = 0
                            pending = []

                            for row in cursor:
                                count += 1
                                key = tuple(counter.next(row)[:-1])
                                if key != start_point:
                                    if first_value:
                                        if not acc:
                                            Log.error("not expected")
                                        pending.append({"start_point": start_point, "first_value": first_value, "data": acc})
                                    acc = []
                                    start_point = key
                                    first_value = row
                                acc.append(row[-1])  # ASSUME LAST COLUMN IS THE FACT TABLE id
                self.queue.extend(pending)

                if count < batch_size:
                    self.queue.add(Thread.STOP)
                    break

    def _build_list_sql(self, db, first, batch_size):
        # TODO: ENSURE THE LAST COLUMN IS THE id
        where = "1=1"
        if first:
            where = " OR ".join(
                "(" + " AND ".join(
                    db.quote_column(f) + (SQL("=") if e < i else SQL(">=")) + db.quote_value(v)
                    for e, (f, v) in enumerate(zip(self._extract.field[0:i + 1:], first))
                ) + ")"
                for i in range(len(self._extract.field))
            )

        sql = (
            "SELECT " + ", ".join(db.quote_column(f) for f in self._extract.field) +
            "\nFROM " + self.settings.snowflake.fact_table +
            "\nWHERE " + where +
            "\nORDER BY " + ", ".join(db.quote_column(f) for f in self._extract.field) +
            "\nLIMIT " + db.quote_value(batch_size)
        )
        return sql

    def extract(self, db, start_point, first_value, data, please_stop):
        Log.note(
            "Starting scan of {{table}} at {{id}} and sending to batch {{start_point}}",
            table=self.settings.snowflake.fact_table,
            id=first_value,
            start_point=start_point
        )

        id = db.quote_column(self._extract.field.last())
        ids = (
            "SELECT " + id +
            " FROM " + self.settings.snowflake.fact_table +
            " WHERE " + id + " in (" + ",".join(map(db.quote_value, data)) + ")"
        )
        sql = self.schema.get_sql(ids)
        with Timer("Sending SQL"):
            cursor = db.db.cursor()
            try:
                cursor.execute(sql)
            except Exception, e:
                Log.error("Problem with {{sql}}", sql=sql, cause=e)

        extract = self.settings.extract
        fact_table = self.settings.snowflake.fact_table

        output = File(NamedTemporaryFile(delete=False).name)
        parent_etl = None
        for s in reversed(start_point):
            parent_etl = {
                "id": s,
                "source": parent_etl
            }

        def append(value, i):
            """
            :param value: THE DOCUMENT TO ADD
            :return: PleaseStop
            """
            output.append(convert.value2json({
                fact_table: elasticsearch.scrub(value),
                "etl": {
                    "id": i,
                    "source": parent_etl,
                    "timestamp": Date.now(),
                    "revision": get_git_revision(),
                    "machine": machine_metadata
                }
            }))
        with Timer("assemble data"):
            with closing(cursor):
                self.construct_docs(cursor, append, please_stop)

        # WRITE TO S3
        s3_file_name = ".".join(map(unicode, start_point))
        with Timer("write to destination"):
            try:
                if not isinstance(self.settings.destination, unicode):
                    destination = s3.Bucket(self.settings.destination).get_key(s3_file_name, must_exist=False)
                    destination.write_lines(output)
                else:
                    destination = File(self.settings.destination)
                    destination.write(convert.value2json([convert.json2value(o) for o in output], pretty=True))
                    return False
            finally:
                output.delete()

        # SUCCESS!!
        File(extract.last).write(convert.value2json([start_point, first_value]))

    def construct_docs(self, cursor, append, please_stop):
        """
        :param cursor: ITERATOR OF RECORDS
        :param append: METHOD TO CALL WITH CONSTRUCTED DOCUMENT
        :return: (count, first, next, next_key)
        number of documents added
        the first document in the batch
        the first document of the next batch
        """
        null_values = set(self.settings.snowflake.null_values) | {None}

        count = 0
        columns = tuple(wrap(c) for c in self.schema.columns)
        with Timer("Downloading from MySQL"):
            curr_record = Null
            for row in cursor:
                if please_stop:
                    Log.error("Got `please_stop` signal")

                nested_path = []
                next_record = None

                for c, value in zip(columns, row):
                    if value in null_values:
                        continue
                    if len(nested_path) < len(c.nested_path):
                        nested_path = unwrap(c.nested_path)
                        next_record = Data()
                    next_record[c.put] = value

                if len(nested_path) > 1:
                    path = nested_path[-2]
                    children = curr_record[path]
                    if children == None:
                        children = curr_record[path] = wrap([])
                    if len(nested_path) > 2:
                        parent_path = path
                        for path in list(reversed(nested_path[0:-2:])):
                            parent = children.last()
                            relative_path = relative_field(path, parent_path)
                            children = parent[relative_path]
                            if children == None:
                                children = parent[relative_path] = wrap([])
                            parent_path=path

                    children.append(next_record)
                    continue

                if curr_record == next_record:
                    Log.error("not expected")

                if curr_record:
                    append(curr_record["id"], count)
                    count += 1
                curr_record = next_record

            # DEAL WITH LAST RECORD
            if curr_record:
                append(curr_record["id"], count)
                count += 1

        Log.note("{{num}} records", num=count)


def main():
    try:
        settings = startup.read_settings()
        constants.set(settings.constants)
        Log.start(settings.debug)

        extractor = Extract(settings)

        def extract(please_stop):
            with closing(MySQL(**settings.snowflake.database)) as db:
                for kwargs in extractor.queue:
                    if please_stop:
                        break
                    try:
                        extractor.extract(db=db, please_stop=please_stop, **kwargs)
                    except Exception, e:
                        Log.warning("Could not extract", cause=e)

        for i in range(settings.extract.threads):
            Thread.run("extract #"+unicode(i), extract)

        please_stop = Signal()
        Thread.wait_for_shutdown_signal(please_stop=please_stop, allow_exit=True)
    except Exception, e:
        Log.error("Problem with data extraction", e)
    finally:
        Log.stop()


if __name__=="__main__":
    main()
