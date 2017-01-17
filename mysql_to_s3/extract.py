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
from MoLogs.strings import expand_template
from mysql_to_s3 import lt
from mysql_to_s3.snowflake_schema import SnowflakeSchema
from pyDots import coalesce, Data, wrap, Null, listwrap, unwrap, relative_field
from pyLibrary import convert, jsons
from pyLibrary.aws import s3
from pyLibrary.env.files import File
from pyLibrary.env.git import get_git_revision
from pyLibrary.maths import Math
from pyLibrary.meta import use_settings
from pyLibrary.queries import jx
from pyLibrary.sql.mysql import MySQL
from pyLibrary.thread.signal import Signal
from pyLibrary.thread.threads import Thread
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
        processes = None
        try:
            self.db = MySQL(**settings.snowflake.database)
            processes = jx.filter(self.db.query("show processlist"), {"and": [{"neq": {"Command": "Sleep"}}, {"neq": {"Info": "show processlist"}}]})
        except Exception, e:
            Log.warning("no database", cause=e)
        # if processes:
        #     Log.error("Processes are running\n{{list|json}}", list=processes)

        # self.last IS THE FIRST RECORD IN THE CURRENT BATCH (AND HAS BEEN WRITTEN TO S3)
        try:
            self.first = File(extract.last).read_json()
        except Exception:
            self.first = wrap({f: v for f, v in zip(listwrap(extract.field), listwrap(extract.start))})

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
                self.first[extract.field[i]] = Date(self.first[extract.field[i]])
            elif t == "number":
                pass
            else:
                Log.error('Expecting `extract.type` to be "number" or "time"')

        # DETERMINE WHICH etl.id WE WROTE TO LAST
        self.first_key = self._get_key(self.first)
        if isinstance(self.settings.destination, basestring):
            self.bucket = None
        else:
            self.bucket = s3.Bucket(self.settings.destination)
        self.last_batch = 0
        prefix = ".".join(self._get_s3_name(self.first_key).split(".")[:-1])
        if not prefix:
            prefix = None
        if self.bucket:
            existing = self.bucket.keys(prefix=prefix)
            self.last_batch = coalesce(wrap(sorted(int(e.split(".")[-1]) for e in existing)).last(), 0)
        else:
            self.last_batch = 0

    def close(self):
        try:
            self.db.close()
        except Exception:
            pass

    def _get_key(self, r):
        output = tuple(
            Date(r[k]) if t == "time" else r[k]
            for t, k in zip(self._extract.type, self._extract.field)
        )
        return output

    def _belongs_to_next_batch(self, key):
        """
        :param key: key
        :return: True if belongs to next batch, given self.last is in current batch
        """

        # self.last_key = self._get_key(self.last)
        for k, l, t, b in zip(key, self.first_key, self._extract.type, self._extract.batch):
            if t=="time":
                if k.floor(b) > l.floor(b):
                    return True
            else:
                if Math.floor(k, b) > Math.floor(l, b):
                    return True
        return False

    def _get_etl(self, key):
        etl = None
        for k, t, s, b in zip(key, self._extract.type, self._extract.start, self._extract.batch)[:-1]:
            id = Math.round((k - s) / b, decimal=0)
            etl = {"id": id, "source": etl}

        id = self.last_batch
        etl = {"id": id, "source": etl}
        return etl

    def _get_s3_name(self, key):
        ids = []
        for k, t, s, b in zip(key, self._extract.type, self._extract.start, self._extract.batch)[:-1]:
            id = Math.round((k - s) / b, decimal=0)
            ids.append(unicode(id))
        ids.append(unicode(int(self.last_batch)))
        return ".".join(ids)

    def extract(self):
        Log.note(
            "Starting scan of {{table}} at {{id}} and sending to batch {{num}}",
            table=self.settings.snowflake.fact_table,
            id=self.first_key,
            num=self.last_batch
        )

        sql = self.schema.get_sql(expand_template(self.settings.extract.ids, {"last": self.first}))
        with Timer("Sending SQL"):
            cursor = self.db.db.cursor()
            cursor.execute(sql)

        extract = self.settings.extract
        fact_table = self.settings.snowflake.fact_table

        output = File(NamedTemporaryFile(delete=False).name)

        def append(value, i):
            """
            :param value: THE DOCUMENT TO ADD
            :return: PleaseStop
            """
            etl = self._get_etl(self._get_key(value))

            output.append(convert.value2json({
                fact_table: jsons.scrub(value),
                "etl": {
                    "id": i,
                    "source": etl,
                    "timestamp": Date.now(),
                    "revision": get_git_revision(),
                    "machine": machine_metadata
                }
            }))

        file_name = self._get_s3_name(self.first_key)

        with closing(cursor):
            count, first, next = self.construct_docs(cursor, append)

        if not first:
            Log.error("no last record encountered")

        # WRITE TO S3
        with Timer("write to destination"):
            try:
                if self.bucket:
                    destination = self.bucket.get_key(file_name, must_exist=False)
                    destination.write_lines(output)
                else:
                    destination = File(self.settings.destination)
                    destination.write(convert.value2json([convert.json2value(o) for o in output], pretty=True))
                    return False
            finally:
                output.delete()

        # SUCCESS!!
        if not next:
            return False

        next_key = self._get_key(next)
        if count >= self._extract.batch.last() or self._belongs_to_next_batch(next_key):
            self.first = next
            self.first_key = next_key
            self.last_batch += 1
            File(extract.last).write(convert.value2json(next))
            return True
        return False

    def construct_docs(self, cursor, append):
        """
        :param cursor: ITERATOR OF RECORDS
        :param append: METHOD TO CALL WITH CONSTRUCTED DOCUMENT
        :return: (count, first, next, next_key)
        number of documents added
        the first document in the batch
        the first document of the next batch
        """
        batch_size = self._extract.batch.last()
        null_values = set(self.settings.snowflake.null_values) | {None}
        next = next_key = None
        first = self.first
        first_key = self._get_key(first)

        count = 0
        columns = tuple(wrap(c) for c in self.schema.columns)
        with Timer("Downloading from MySQL"):
            curr_record = Null
            for row in cursor:
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

                # append RECORD TO output
                core_record = next_record["id"]
                core_id = self._get_key(core_record)

                if lt(core_id, first_key):
                    first = core_record
                    first_key = core_id
                if curr_record:
                    if count >= batch_size or self._belongs_to_next_batch(core_id):
                        if next_key is None or lt(core_id, next_key):
                            next = core_record
                            next_key = core_id
                    else:
                        append(curr_record["id"], count)
                        count += 1
                curr_record = next_record

            # DEAL WITH LAST RECORD
            if curr_record:
                core_record = curr_record["id"]
                core_id = self._get_key(core_record)
                if count >= batch_size or self._belongs_to_next_batch(core_id):
                    if next_key is None or lt(core_id, next_key):
                        next = core_record
                        next_key = core_id
                else:
                    append(curr_record["id"], count)
                    count += 1
        Log.note("{{num}} records", num=count)
        return count, first, next


def main():
    try:
        settings = startup.read_settings()
        constants.set(settings.constants)
        Log.start(settings.debug)

        def extract(please_stop):
            with closing(Extract(settings)) as e:
                while e.extract() and not please_stop:
                    pass

        e = Thread.run("extracting", extract)
        please_stop = Signal()
        e.stopped.on_go(please_stop.go)
        Thread.wait_for_shutdown_signal(please_stop=please_stop, allow_exit=True)
        e.join()
    except Exception, e:
        Log.error("Problem with data extraction", e)
    finally:
        Log.stop()


if __name__=="__main__":
    main()
