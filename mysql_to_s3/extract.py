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
from copy import deepcopy, copy
from tempfile import NamedTemporaryFile

from MoLogs import Log, strings, startup, constants, machine_metadata
from MoLogs.strings import expand_template
from mysql_to_s3 import lt, _key2etl
from pyDots import coalesce, Data, wrap, Null, FlatList, unwrap, join_field, split_field, relative_field, concat_field, literal_field, set_default, startswith_field, listwrap
from pyLibrary import convert
from pyLibrary.aws import s3
from pyLibrary.env.files import File
from pyLibrary.env.git import get_git_revision
from pyLibrary.maths import Math
from pyLibrary.maths.randoms import Random
from pyLibrary.meta import use_settings
from pyLibrary.queries import jx
from pyLibrary.queries.unique_index import UniqueIndex
from pyLibrary.sql.mysql import MySQL
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import Duration
from pyLibrary.times.timer import Timer

DEBUG = True

class Extract(object):

    @use_settings
    def __init__(self, settings=None):
        self.settings = settings
        self.settings.exclude = set(self.settings.exclude)
        self.settings.show_foreign_keys = coalesce(self.settings.show_foreign_keys, True)
        self._extract = extract = settings.extract

        # SOME PREP
        get_git_revision()

        # VERIFY WE DO NOT HAVE TOO MANY OTHER PROCESSES WORKING ON STUFF
        processes = None
        try:
            self.db = MySQL(**settings.database)
            processes = jx.filter(self.db.query("show processlist"), {"and": [{"neq": {"Command": "Sleep"}}, {"neq": {"Info": "show processlist"}}]})
        except Exception, e:
            Log.warning("no database", cause=e)
        # if processes:
        #     Log.error("Processes are running\n{{list|json}}", list=processes)

        # self.last IS THE FIRST RECORD IN THE CURRENT BATCH (AND HAS BEEN WRITTEN TO S3)
        try:
            self.last = File(extract.last).read_json()
        except Exception:
            self.last = wrap({f: v for f, v in zip(listwrap(extract.field), listwrap(extract.start))})

        extract.type = listwrap(extract.type)
        extract.start = listwrap(extract.start)
        extract.batch = listwrap(extract.batch)
        extract.field = listwrap(extract.field)
        if any(len(extract.type) != len(other) for other in [extract.start, extract.batch, extract.field]):
            Log.error("Expecting same number of dimensionst for `type`, `start`, `batch`, and `field` in the `extract` inner object")
        for i, t in enumerate(extract.type):
            if t == "time":
                extract.start[i] = Date(extract.start[i])
                extract.batch[i] = Duration(extract.batch[i])
                self.last[extract.field[i]] = Date(self.last[extract.field[i]])
            elif t == "number":
                pass
            else:
                Log.error('Expecting `extract.type` to be "number" or "time"')

        # DETERMINE WHICH etl.id WE WROTE TO LAST
        last_key = self._get_key(self.last)
        self.bucket = s3.Bucket(self.settings.destination)
        self.last_batch = 0
        existing = self.bucket.keys(prefix=".".join(self._get_s3_name(last_key).split(".")[:-1]))
        self.last_batch = coalesce(wrap(sorted(int(e.split(".")[-1]) for e in existing)).last(), 0)

    def _get_key(self, r):
        output = tuple(
            Date(r[k]) if t == "time" else r[k]
            for t, k in zip(self._extract.type, self._extract.field)
        )
        return output

    def _belongs_to_next_batch(self, key):
        """
        :param key: key
        :return: True if belongs to next batch, given self.last is in curret batch
        """

        last_key = self._get_key(self.last)
        for k, l, t, b in zip(key, last_key, self._extract.type, self._extract.batch):
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
        ids.append(unicode(self.last_batch))
        return ".".join(ids)

    def make_sql(self):
        # GET ALL RELATIONS
        raw_relations = self.db.query("""
            SELECT
                table_schema,
                table_name,
                referenced_table_schema,
                referenced_table_name,
                referenced_column_name,
                constraint_name,
                column_name,
                ordinal_position
            FROM
                information_schema.key_column_usage
            WHERE
                referenced_column_name IS NOT NULL
        """, param=self.settings.database)

        for r in self.settings.add_relations:
            try:
                a, b = map(strings.trim, r.split("->"))
                a = a.split(".")
                b = b.split(".")
                raw_relations.append(Data(
                    table_schema=a[0],
                    table_name=a[1],
                    referenced_table_schema=b[0],
                    referenced_table_name=b[1],
                    referenced_column_name=b[2],
                    constraint_name=Random.hex(20),
                    column_name=a[2],
                    ordinal_position=1
                ))
            except Exception, e:
                Log.error("Could not parse {{line|quote}}", line=r, cause=e)

        relations = jx.select(raw_relations, [
            {"name": "constraint.name", "value": "constraint_name"},
            {"name": "table.schema", "value": "table_schema"},
            {"name": "table.name", "value": "table_name"},
            {"name": "column.name", "value": "column_name"},
            {"name": "referenced.table.schema", "value": "referenced_table_schema"},
            {"name": "referenced.table.name", "value": "referenced_table_name"},
            {"name": "referenced.column.name", "value": "referenced_column_name"},
            {"name": "ordinal_position", "value": "ordinal_position"}
        ])

        # GET ALL TABLES
        raw_tables = self.db.query("""
            SELECT
                t.table_schema,
                t.table_name,
                c.constraint_name,
                c.constraint_type,
                k.column_name,
                k.ordinal_position
            FROM
                information_schema.tables t
            LEFT JOIN
                information_schema.table_constraints c on c.table_name=t.table_name AND c.table_schema=t.table_schema and (constraint_type='UNIQUE' or constraint_type='PRIMARY KEY')
            LEFT JOIN
                information_schema.key_column_usage k on k.constraint_name=c.constraint_name AND k.table_name=t.table_name and k.table_schema=t.table_schema
            ORDER BY
                t.table_schema,
                t.table_name,
                c.constraint_name,
                k.ordinal_position,
                k.column_name
        """, param=self.settings.database)

        # ORGANIZE, AND PICK ONE UNIQUE CONSTRAINT FOR LINKING
        tables = UniqueIndex(keys=["name", "schema"])
        for t, c in jx.groupby(raw_tables, ["table_name", "table_schema"]):
            c = wrap(list(c))
            best_index = Null
            is_referenced = False
            is_primary = False
            for g, w in jx.groupby(c, "constraint_name"):
                if not g.constraint_name:
                    continue
                w = list(w)
                ref = False
                for r in relations:
                    if r.table.name == t.table_name and r.table.schema == t.table_schema and r.constraint.name == g.constraint_name:
                        ref = True
                is_prime = w[0].constraint_type == "PRIMARY"

                reasons_this_one_is_better = [
                    best_index == None,  # WE DO NOT HAVE A CANDIDATE YET
                    is_prime and not is_primary,  # PRIMARY KEYS ARE GOOD TO HAVE
                    is_primary == is_prime and ref and not is_referenced,  # REFERENCED UNIQUE TUPLES ARE GOOD TOO
                    is_primary == is_prime and ref == is_referenced and len(w) < len(best_index)  # THE SHORTER THE TUPLE, THE BETTER
                ]
                if any(reasons_this_one_is_better):
                    is_primary = is_prime
                    is_referenced = ref
                    best_index = w

            tables.add({
                "name": t.table_name,
                "schema": t.table_schema,
                "id": [b.column_name for b in best_index]
            })

        fact_table = tables[self.settings.fact_table, self.settings.database.schema]
        ids_table = {
            "alias": "t0",
            "name": "__ids__",
            "schema": fact_table.schema,
            "id": fact_table.id
        }
        relations.extend(
            wrap({
                "constraint": {"name":"__link_ids_to_fact_table__"},
                "table": ids_table,
                "column": {"name": c},
                "referenced": {
                    "table": fact_table,
                    "column": {
                        "name": c
                    }
                },
                "ordinal_position": i
            })
            for i, c in enumerate(fact_table.id)
        )
        tables.add(ids_table)

        # GET ALL COLUMNS
        raw_columns = self.db.query("""
            SELECT
                column_name,
                table_schema,
                table_name,
                ordinal_position,
                data_type
            FROM
                information_schema.columns
        """, param=self.settings.database)

        reference_only_tables = [r.split(".")[0] for r in self.settings.reference_only]
        related_column_table_schema_triples = {
            t
            for r in relations
            for t in [
                (r.column.name, r.table.name, r.table.schema),
                (r.referenced.column.name, r.referenced.table.name, r.referenced.table.schema)
            ]
        }

        columns = UniqueIndex(["column.name", "table.name", "table.schema"])
        for c in raw_columns:
            if c.table_name in reference_only_tables:
                if c.table_name + "." + c.column_name in self.settings.reference_only:
                    include = True
                    reference = True
                elif c.column_name in tables[(c.table_name, c.table_schema)].id:
                    include = self.settings.show_foreign_keys
                    reference = False
                else:
                    include = False
                    reference = False
            elif c.column_name in tables[(c.table_name, c.table_schema)].id:
                include = self.settings.show_foreign_keys
                reference = False
            elif (c.column_name, c.table_name, c.table_schema) in related_column_table_schema_triples:
                include = self.settings.show_foreign_keys
                reference = False
            else:
                include = True
                reference = False

            rel = {
                "column": {
                    "name": c.column_name,
                    "type": c.data_type
                },
                "table": {
                    "name": c.table_name,
                    "schema": c.table_schema
                },
                "ordinal_position": c.ordinal_position,
                "is_id": c.column_name in tables[(c.table_name, c.table_schema)].id,
                "include": include,
                "reference": reference   # TRUE IF THIS COLUMN REPRESENTS THE ROW
            }
            columns.add(rel)

        # ITERATE OVER ALL PATHS
        todo = FlatList()
        output_columns = FlatList()
        nested_path_to_join = {}
        all_nested_paths = [["."]]
        # done_relations = []

        def follow_paths(position, path, nested_path, done_relations):
            if position.name in self.settings.exclude:
                return
            Log.note("Trace {{path}}", path=path)
            if position.name!="__ids__":
                self.db.query("SELECT * FROM "+self.db.quote_column(position.name, position.schema)+" LIMIT 1")

            if position.name in reference_only_tables:
                return

            curr_join_list = copy(nested_path_to_join[nested_path[0]])

            # INNER OBJECTS
            referenced_tables = list(jx.groupby(jx.filter(relations, {"eq": {"table.name": position.name, "table.schema": position.schema}}), "constraint.name"))
            for g, constraint_columns in referenced_tables:
                g = unwrap(g)
                constraint_columns = deepcopy(constraint_columns)
                if g["constraint.name"] in done_relations:
                    continue
                if any(cc for cc in constraint_columns if cc.referenced.table.name in self.settings.exclude):
                    continue

                done_relations.add(g["constraint.name"])

                many_to_one_joins = nested_path_to_join[nested_path[0]]
                index = len(many_to_one_joins)

                alias = "t" + unicode(index)
                for c in constraint_columns:
                    c.referenced.table.alias = alias
                    c.table = position
                many_to_one_joins.append({
                    "join_columns": constraint_columns,
                    "path": path,
                    "nested_path": nested_path
                })

                # referenced_table_path = join_field(split_field(path) + ["/".join(constraint_columns.referenced.table.name)])
                # HANDLE THE COMMON *id SUFFIX
                name = []
                for a, b in zip(constraint_columns.column.name, constraint_columns.referenced.table.name):
                    if a.startswith(b):
                        name.append(b)
                    elif a.endswith("_id"):
                        name.append(a[:-3])
                    else:
                        name.append(a)
                referenced_column_path = join_field(split_field(path) + ["/".join(name)])
                col_pointer_name = relative_field(referenced_column_path, nested_path[0])

                for col in columns:
                    if col.table.name == constraint_columns[0].referenced.table.name and col.table.schema == constraint_columns[0].referenced.table.schema:
                        col_full_name = concat_field(col_pointer_name, literal_field(col.column.name))

                        if col.is_id and col.table.name == fact_table.name and col.table.schema == fact_table.schema:
                            # ALWAYS SHOW THE ID OF THE FACT
                            c_index = len(output_columns)
                            output_columns.append({
                                "table_alias": alias,
                                "column_alias": "c"+unicode(c_index),
                                "column": col,
                                "path": referenced_column_path,
                                "nested_path": nested_path,
                                "put": col_full_name
                            })
                        elif col.column.name == constraint_columns[0].column.name:
                            c_index = len(output_columns)
                            output_columns.append({
                                "table_alias": alias,
                                "column_alias": "c"+unicode(c_index),
                                "column": col,
                                "path": referenced_column_path,
                                "nested_path": nested_path,
                                "put": col_full_name if self.settings.show_foreign_keys else None
                            })
                        elif col.is_id:
                            c_index = len(output_columns)
                            output_columns.append({
                                "table_alias": alias,
                                "column_alias": "c"+unicode(c_index),
                                "column": col,
                                "path": referenced_column_path,
                                "nested_path": nested_path,
                                "put": col_full_name if self.settings.show_foreign_keys else None
                            })
                        elif col.reference:
                            c_index = len(output_columns)
                            output_columns.append({
                                "table_alias": alias,
                                "column_alias": "c"+unicode(c_index),
                                "column": col,
                                "path": referenced_column_path,
                                "nested_path": nested_path,
                                "put": col_pointer_name if not self.settings.show_foreign_keys else col_full_name  # REFERENCE FIELDS CAN REPLACE THE WHOLE OBJECT BEING REFERENCED
                            })
                        elif col.include:
                            c_index = len(output_columns)
                            output_columns.append({
                                "table_alias": alias,
                                "column_alias": "c"+unicode(c_index),
                                "column": col,
                                "path": referenced_column_path,
                                "nested_path": nested_path,
                                "put": col_full_name
                            })

                if position.name in reference_only_tables:
                    continue

                todo.append(Data(
                    position=constraint_columns[0].referenced.table.copy(),
                    path=referenced_column_path,
                    nested_path=nested_path,
                    done_relations=copy(done_relations)
                ))

            # NESTED OBJECTS
            for g, constraint_columns in jx.groupby(jx.filter(relations, {"eq": {"referenced.table.name": position.name, "referenced.table.schema": position.schema}}), "constraint.name"):
                g = unwrap(g)
                constraint_columns = deepcopy(constraint_columns)
                if g["constraint.name"] in done_relations:
                    continue
                done_relations.add(g["constraint.name"])

                many_table = set(constraint_columns.table.name)
                if not (many_table - self.settings.exclude):
                    continue

                referenced_column_path = join_field(split_field(path) + ["/".join(many_table)])
                new_nested_path = [referenced_column_path] + nested_path
                all_nested_paths.append(new_nested_path)

                # if new_path not in self.settings.include:
                #     Log.note("Exclude nested path {{path}}", path=new_path)
                #     continue
                one_to_many_joins = nested_path_to_join[referenced_column_path] = copy(curr_join_list)
                index = len(one_to_many_joins)
                alias = "t"+unicode(index)
                for c in constraint_columns:
                    c.table.alias = alias
                    c.referenced.table = position
                one_to_many_joins.append(set_default({}, g, {
                    "children": True,
                    "join_columns": constraint_columns,
                    "path": path,
                    "nested_path": nested_path
                }))

                for col in columns:
                    if col.table.name == constraint_columns[0].table.name and col.table.schema == constraint_columns[0].table.schema:
                        col_full_name = join_field(split_field(referenced_column_path)[len(split_field(new_nested_path[0])):]+[literal_field(col.column.name)])

                        if col.column.name == constraint_columns[0].column.name:
                            c_index = len(output_columns)
                            output_columns.append({
                                "table_alias": alias,
                                "column_alias": "c"+unicode(c_index),
                                "column": col,
                                "path": referenced_column_path,
                                "nested_path": new_nested_path,
                                "put": col_full_name if self.settings.show_foreign_keys else None
                            })
                        elif col.is_id:
                            c_index = len(output_columns)
                            output_columns.append({
                                "table_alias": alias,
                                "column_alias": "c"+unicode(c_index),
                                "column": col,
                                "path": referenced_column_path,
                                "nested_path": new_nested_path,
                                "put": col_full_name if self.settings.show_foreign_keys else None
                            })
                        else:
                            c_index = len(output_columns)
                            output_columns.append({
                                "table_alias": alias,
                                "column_alias": "c"+unicode(c_index),
                                "column": col,
                                "path": referenced_column_path,
                                "nested_path": new_nested_path,
                                "put": col_full_name
                            })

                todo.append(Data(
                    position=constraint_columns[0].table,
                    path=referenced_column_path,
                    nested_path=new_nested_path,
                    done_relations=copy(done_relations)
                ))

        path = "."
        nested_path = [path]
        nested_path_to_join["."] = [{
            "path": path,
            "join_columns": [{"referenced": {"table": ids_table}}],
            "nested_path": nested_path
        }]

        todo.append(Data(
            position=ids_table,
            path=path,
            nested_path=nested_path,
            done_relations=set()
        ))

        while todo:
            item = todo.pop(0)
            follow_paths(**item)

        sql = self.compose_sql(all_nested_paths, nested_path_to_join, output_columns)

        return Data(
            sql=sql,
            columns=output_columns,
            where=self.settings.where
        )

    def compose_sql(self, all_nested_paths, nested_path_to_join, output_columns):
        """
        :param all_nested_paths: list of tuples; each tuple is a path (from deep to shallow of all nested documents)
        :param nested_path_to_join: dict from nested path to information required to build sql join
        :param output_columns: ordered list of every output column
        :return:
        """

        # GENERATE SQL
        # HOW TO AVOID JOINING LOOKUP TABLES WHEN PROVIDING NESTED RECORDS?
        # INCLUDE ID FOR EACH PARENT IN THE NESTED PATH
        # INCLUDE ALL COLUMNS FOR INNER OBJECTS TOO
        sql = []
        for nested_path in all_nested_paths:
            # MAKE THE REQUIRED JOINS
            sql_joins = []

            for i, curr_join in enumerate(nested_path_to_join[nested_path[0]]):
                curr_join = wrap(curr_join)
                rel = curr_join.join_columns[0]
                if i == 0:
                    sql_joins.append("\nFROM (" + expand_template(self.settings.extract.ids, {"last": self.last}) + ") AS " + rel.referenced.table.alias)
                elif curr_join.children:
                    full_name = self.db.quote_column(rel.table.name, rel.table.schema)
                    sql_joins.append(
                        "\nJOIN " + full_name + " AS " + rel.table.alias +
                        "\nON " + " AND ".join(
                            rel.table.alias + "." + self.db.quote_column(const_col.column.name) + "=" + rel.referenced.table.alias + "." + self.db.quote_column(const_col.referenced.column.name)
                            for const_col in curr_join.join_columns
                        )
                    )
                else:
                    full_name = self.db.quote_column(rel.referenced.table.name, rel.referenced.table.schema)
                    sql_joins.append(
                        "\nLEFT JOIN " + full_name + " AS " + rel.referenced.table.alias +
                        "\nON " + " AND ".join(
                            rel.referenced.table.alias + "." + self.db.quote_column(const_col.referenced.column.name) + "=" + rel.table.alias + "." + self.db.quote_column(const_col.column.name)
                            for const_col in curr_join.join_columns
                        )
                    )

            # ONLY SELECT WHAT WE NEED, NULL THE REST
            selects = []
            not_null_column_seen = False
            for ci, c in enumerate(output_columns):
                if c.column_alias[1:] != unicode(ci):
                    Log.error("expecting consistency")
                if c.nested_path[0] == nested_path[0]:
                    s = c.table_alias + "." + c.column.column.name + " as " + c.column_alias
                    if s == None:
                        Log.error("bug")
                    selects.append(s)
                    not_null_column_seen = True
                elif startswith_field(nested_path[0], c.path):
                    # PARENT ID REFERENCES
                    if c.column.is_id:
                        s = c.table_alias + "." + c.column.column.name + " as " + c.column_alias
                        selects.append(s)
                        not_null_column_seen = True
                    else:
                        selects.append("NULL as " + c.column_alias)
                else:
                    selects.append("NULL as " + c.column_alias)

            if not_null_column_seen:
                sql.append("SELECT\n\t" + ",\n\t".join(selects) + "".join(sql_joins))
        return sql

    def extract(self, meta):
        Log.note(
            "Starting scan of {{table}} at {{id}} and sending to batch {{num}}",
            table=self.settings.fact_table,
            id=[self.last[f] for f in self._extract.field],
            num=self.last_batch
        )

        columns = tuple(wrap(c) for c in unwrap(meta.columns))

        # ORDERING
        sort = []
        ordering = []
        for ci, c in enumerate(columns):
            if c.column.is_id:
                sort.append(c.column_alias + " IS NULL")
                sort.append(c.column_alias)
                ordering.append(ci)

        sql = "\nUNION ALL\n".join(meta.sql)
        sql = "SELECT * FROM (" + sql + ") as a\nORDER BY " + ",".join(sort)

        if DEBUG:
            Log.note("{{sql}}", sql=sql)
        with Timer("Sending SQL"):
            cursor = self.db.db.cursor()
            cursor.execute(sql)

        extract = self.settings.extract
        curr = Null
        fact_table = self.settings.fact_table
        null_values = set(self.settings.null_values) | {None}

        count = 0
        output = File(NamedTemporaryFile(delete=False).name)

        def append(value, i):
            """
            :param value: THE DOCUMENT TO ADD
            :return: PleaseStop
            """
            etl = self._get_etl(self._get_key(value))

            output.append(convert.value2json({
                fact_table: value,
                "etl": {
                    "id": i,
                    "source": etl,
                    "timestamp": Date.now(),
                    "revision": get_git_revision(),
                    "machine": machine_metadata
                }
            }))

        last = self.last
        last_key = self._get_key(last)
        file_name = self._get_s3_name(last_key)

        with Timer("Begin copying from MySQL"):
            with closing(cursor):
                for row in cursor:
                    nested_path = []
                    curr_record = None

                    for c, value in zip(columns, row):
                        if value in null_values:
                            continue
                        if len(nested_path) < len(c.nested_path):
                            nested_path = c.nested_path
                            curr_record = Data()
                        if c.put != None:
                            try:
                                curr_record[c.put] = value
                            except Exception, e:
                                Log.warning("should not happen", cause=e)

                    if len(nested_path) != 1:
                        n = nested_path[-1]
                        children = curr[n]
                        if children == None:
                            children = curr[n] = []
                        for n in list(reversed(nested_path[1:-1:])):
                            parent = children[-1]
                            children = parent[n]
                            if children == None:
                                children = parent[n] = []

                        children.append(curr_record)
                        continue

                    if curr == curr_record:
                        Log.error("not expected")

                    # append RECORD TO output
                    core_record = curr_record["id"]
                    core_id = self._get_key(core_record)

                    if lt(core_id, last_key):
                        last = core_record
                        last_key = self._get_key(last)
                    if curr:
                        if count >= self._extract.batch[-1] or self._belongs_to_next_batch(core_id):
                            last = core_record
                            last_key = core_id
                        else:
                            append(curr["id"], count)
                            count += 1
                    curr = curr_record

            # DEAL WITH LAST RECORD
            if curr:
                if count >= self._extract.batch.last() or self._belongs_to_next_batch(core_id):
                    last = core_record
                    last_key = core_id
                else:
                    append(curr["id"], count)
                    count += 1

        Log.note("{{num}} records written", num=count)

        if not last:
            Log.error("no last record encountered")

        # WRITE TO S3
        destination = self.bucket.get_key(file_name, must_exist=False)
        destination.write(output)
        output.delete()

        # SUCCESS!!
        if self._belongs_to_next_batch(last_key):
            self.last_batch += 1
            File(extract.last).write(convert.value2json(last))
            return True
        return False


def main():
    try:
        settings = startup.read_settings()
        constants.set(settings.constants)
        Log.start(settings.debug)

        e = Extract(settings)
        meta = e.make_sql()
        # Log.note("{{sql}}", sql=meta.sql)
        File("output/meta.json").write(convert.value2json(meta, pretty=True, sort_keys=True))
        meta = convert.json2value(File("output/meta.json").read())
        while e.extract(meta):
            pass
    except Exception, e:
        Log.error("Problem with data extraction", e)
    finally:
        Log.stop()


if __name__=="__main__":
    main()
