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

from copy import deepcopy, copy

from pyLibrary.sql import SQL

from jx_python import jx
from mo_collections import UniqueIndex
from mo_dots import coalesce, Data, wrap, Null, FlatList, unwrap, join_field, split_field, relative_field, concat_field, literal_field, set_default, startswith_field
from mo_future import text_type
from mo_kwargs import override
from mo_logs import Log, strings
from mo_logs.exceptions import Explanation
from mo_math.randoms import Random
from mo_times.timer import Timer

from pyLibrary.sql.mysql import MySQL

DEBUG = False


class SnowflakeSchema(object):

    @override
    def __init__(self, kwargs=None):
        self.settings = kwargs
        self.settings.exclude = set(self.settings.exclude)
        self.settings.show_foreign_keys = coalesce(self.settings.show_foreign_keys, True)

        self.all_nested_paths = None
        self.nested_path_to_join = None
        self.columns = None

        with Explanation("scan database", debug=DEBUG):
            self.db = MySQL(**kwargs.database)
            with self.db:
                with self.db.transaction():
                    self._scan_database()
        if DEBUG:
            Log.note("SQL={{sql}}", sql=self.get_sql(get_ids=SQL("SELECT 1")))

    def get_sql(self, get_ids):
        sql = self._compose_sql(get_ids)

        # ORDERING
        sort = []
        ordering = []
        for ci, c in enumerate(self.columns):
            if c.sort:
                sort.append(c.column_alias + " IS NOT NULL")
                sort.append(c.column_alias)
                ordering.append(ci)

        union_all_sql = "\nUNION ALL\n".join(sql)
        union_all_sql = "SELECT * FROM (" + union_all_sql + ") as a\nORDER BY " + ",".join(sort)

        if DEBUG:
            Log.note("{{sql}}", sql=union_all_sql)
        return union_all_sql

    def _scan_database(self):
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

        if not raw_relations:
            Log.error("No relations in the database")

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
            except Exception as e:
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

        reference_only_tables = [r.split(".")[0] for r in self.settings.reference_only if len(r.split(".")) == 2]
        reference_all_tables = [r.split(".")[0] for r in self.settings.reference_only if len(r.split(".")) == 1]
        foreign_column_table_schema_triples = {(r.column.name, r.table.name, r.table.schema) for r in relations}
        referenced_column_table_schema_triples = {(r.referenced.column.name, r.referenced.table.name, r.referenced.table.schema) for r in relations}
        related_column_table_schema_triples = foreign_column_table_schema_triples | referenced_column_table_schema_triples

        columns = UniqueIndex(["column.name", "table.name", "table.schema"])
        for c in raw_columns:
            if c.table_name in reference_only_tables:
                if c.table_name + "." + c.column_name in self.settings.reference_only:
                    include = True
                    reference = True
                    foreign = False
                elif c.column_name in tables[(c.table_name, c.table_schema)].id:
                    include = self.settings.show_foreign_keys
                    reference = False
                    foreign = False
                else:
                    include = False
                    reference = False
                    foreign = False
            elif c.table_name in reference_all_tables:
                # TABLES USED FOR REFERENCE, NO NESTED DOCUMENTS EXPECTED
                if c.column_name in tables[(c.table_name, c.table_schema)].id:
                    include = self.settings.show_foreign_keys
                    reference = True
                    foreign = False
                elif (c.column_name, c.table_name, c.table_schema) in foreign_column_table_schema_triples:
                    include = False
                    reference = False
                    foreign = True
                else:
                    include = True
                    reference = False
                    foreign = False
            elif c.column_name in tables[(c.table_name, c.table_schema)].id:
                include = self.settings.show_foreign_keys
                reference = False
                foreign = False
            elif (c.column_name, c.table_name, c.table_schema) in foreign_column_table_schema_triples:
                include = False
                reference = False
                foreign = True
            elif (c.column_name, c.table_name, c.table_schema) in referenced_column_table_schema_triples:
                include = self.settings.show_foreign_keys
                reference = False
                foreign = False
            else:
                include = True
                reference = False
                foreign = False

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
                "include": include,      # TRUE IF THIS COLUMN IS OUTPUTTED
                "reference": reference,  # TRUE IF THIS COLUMN REPRESENTS THE ROW
                "foreign": foreign       # TRUE IF THIS COLUMN POINTS TO ANOTHER ROW
            }
            columns.add(rel)

        # ITERATE OVER ALL PATHS
        todo = FlatList()
        output_columns = FlatList()
        nested_path_to_join = {}
        all_nested_paths = [["."]]

        def follow_paths(position, path, nested_path, done_relations, no_nested_docs):
            if position.name in self.settings.exclude:
                return
            if DEBUG:
                Log.note("Trace {{path}}", path=path)
            if position.name != "__ids__":
                with Timer("Test we can access "+position.name, debug=DEBUG):
                    # USED TO CONFIRM WE CAN ACCESS THE TABLE (WILL THROW ERROR WHEN IF IT FAILS)
                    self.db.query("SELECT * FROM "+self.db.quote_column(position.name, position.schema)+" LIMIT 1")

            if position.name in reference_all_tables:
                no_nested_docs = True
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

                alias = "t" + text_type(index)
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
# insert into nested1 VALUES (100, 10, 'aaa', -1);
# id.about.time.nested1 .ref=10
# id.about.time.nested1 .ref.name
                for col in columns:
                    if col.table.name == constraint_columns[0].referenced.table.name and col.table.schema == constraint_columns[0].referenced.table.schema:
                        col_full_name = concat_field(col_pointer_name, literal_field(col.column.name))

                        if col.is_id and col.table.name == fact_table.name and col.table.schema == fact_table.schema:
                            # ALWAYS SHOW THE ID OF THE FACT
                            c_index = len(output_columns)
                            output_columns.append({
                                "table_alias": alias,
                                "column_alias": "c"+text_type(c_index),
                                "column": col,
                                "sort": True,
                                "path": referenced_column_path,
                                "nested_path": nested_path,
                                "put": col_full_name
                            })
                        elif col.column.name == constraint_columns[0].column.name:
                            c_index = len(output_columns)
                            output_columns.append({
                                "table_alias": alias,
                                "column_alias": "c"+text_type(c_index),
                                "column": col,
                                "sort": False,
                                "path": referenced_column_path,
                                "nested_path": nested_path,
                                "put": col_full_name if self.settings.show_foreign_keys else None
                            })
                        elif col.is_id:
                            c_index = len(output_columns)
                            output_columns.append({
                                "table_alias": alias,
                                "column_alias": "c"+text_type(c_index),
                                "column": col,
                                "sort": False,
                                "path": referenced_column_path,
                                "nested_path": nested_path,
                                "put": col_full_name if self.settings.show_foreign_keys else None
                            })
                        elif col.reference:
                            c_index = len(output_columns)
                            output_columns.append({
                                "table_alias": alias,
                                "column_alias": "c"+text_type(c_index),
                                "column": col,
                                "sort": False,
                                "path": referenced_column_path,
                                "nested_path": nested_path,
                                "put": col_pointer_name if not self.settings.show_foreign_keys else col_full_name  # REFERENCE FIELDS CAN REPLACE THE WHOLE OBJECT BEING REFERENCED
                            })
                        elif col.include:
                            c_index = len(output_columns)
                            output_columns.append({
                                "table_alias": alias,
                                "column_alias": "c"+text_type(c_index),
                                "column": col,
                                "sort": False,
                                "path": referenced_column_path,
                                "nested_path": nested_path,
                                "put": col_full_name
                            })

                if position.name in reference_only_tables:
                    continue

                todo.append(Data(
                    position=copy(constraint_columns[0].referenced.table),
                    path=referenced_column_path,
                    nested_path=nested_path,
                    done_relations=copy(done_relations),
                    no_nested_docs=no_nested_docs
                ))

            # NESTED OBJECTS
            if not no_nested_docs:
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
                    alias = "t"+text_type(index)
                    for c in constraint_columns:
                        c.table.alias = alias
                        c.referenced.table = position
                    one_to_many_joins.append(set_default({}, g, {
                        "children": True,
                        "join_columns": constraint_columns,
                        "path": path,
                        "nested_path": nested_path
                    }))
# insert into nested1 VALUES (100, 10, 'aaa', -1); # id.about.time.nested1 .ref=10# id.about.time.nested1 .ref.name
                    for col in columns:
                        if col.table.name == constraint_columns[0].table.name and col.table.schema == constraint_columns[0].table.schema:
                            col_full_name = join_field(split_field(referenced_column_path)[len(split_field(new_nested_path[0])):]+[literal_field(col.column.name)])

                            if col.column.name == constraint_columns[0].column.name:
                                c_index = len(output_columns)
                                output_columns.append({
                                    "table_alias": alias,
                                    "column_alias": "c"+text_type(c_index),
                                    "column": col,
                                    "sort": col.is_id,
                                    "path": referenced_column_path,
                                    "nested_path": new_nested_path,
                                    "put": col_full_name if self.settings.show_foreign_keys else None
                                })
                            elif col.is_id:
                                c_index = len(output_columns)
                                output_columns.append({
                                    "table_alias": alias,
                                    "column_alias": "c"+text_type(c_index),
                                    "column": col,
                                    "sort": col.is_id,
                                    "path": referenced_column_path,
                                    "nested_path": new_nested_path,
                                    "put": col_full_name if self.settings.show_foreign_keys else None
                                })
                            else:
                                c_index = len(output_columns)
                                output_columns.append({
                                    "table_alias": alias,
                                    "column_alias": "c"+text_type(c_index),
                                    "column": col,
                                    "sort": col.is_id,
                                    "path": referenced_column_path,
                                    "nested_path": new_nested_path,
                                    "put": col_full_name if col.include else None
                                })

                    todo.append(Data(
                        position=constraint_columns[0].table,
                        path=referenced_column_path,
                        nested_path=new_nested_path,
                        done_relations=copy(done_relations),
                        no_nested_docs=no_nested_docs
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
            done_relations=set(),
            no_nested_docs=False
        ))

        while todo:
            item = todo.pop(0)
            follow_paths(**item)

        self.all_nested_paths = all_nested_paths
        self.nested_path_to_join = nested_path_to_join
        self.columns = output_columns

    def _compose_sql(self, get_ids):
        """
        :param get_ids: SQL to get the ids, and used to select the documents returned
        :return:
        """

        sql = []
        for nested_path in self.all_nested_paths:
            # MAKE THE REQUIRED JOINS
            sql_joins = []

            for i, curr_join in enumerate(self.nested_path_to_join[nested_path[0]]):
                curr_join = wrap(curr_join)
                rel = curr_join.join_columns[0]
                if i == 0:
                    sql_joins.append("\nFROM (" + get_ids + ") AS " + rel.referenced.table.alias)
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
            for ci, c in enumerate(self.columns):
                if c.column_alias[1:] != text_type(ci):
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

