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

from pyLibrary import strings, convert
from pyLibrary.debugs import constants, startup
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import Dict, wrap, join_field, set_default, startswith_field, Null, split_field, DictList, coalesce, literal_field, unwrap, relative_field, concat_field
from pyLibrary.env.files import File
from pyLibrary.maths.randoms import Random
from pyLibrary.meta import use_settings
from pyLibrary.queries import jx
from pyLibrary.queries.unique_index import UniqueIndex
from pyLibrary.sql.mysql import MySQL


class Extract(object):

    @use_settings
    def __init__(self, settings=None):
        self.settings=settings
        self.settings.exclude = set(self.settings.exclude)
        self.settings.show_ids = coalesce(self.settings.show_ids, True)
        processes = None
        try:
            self.db = MySQL(**settings.database)
            processes = jx.filter(self.db.query("show processlist"), {"and": [{"neq": {"Command": "Sleep"}}, {"neq": {"Info": "show processlist"}}]})
        except Exception, e:
            Log.warning("no database", cause=e)
        if processes:
            Log.error("Processes are running\n{{list|json}}", list=processes)

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
                raw_relations.append(Dict(
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
                Log.error("Could not parse {{line|quote}}", line=r)

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

        prime_table = tables[self.settings.fact_table, self.settings.database.schema]
        ids_table = {
            "alias": "t0",
            "name": "__ids__",
            "schema": prime_table.schema,
            "id": prime_table.id
        }
        relations.extend(
            wrap({
                "constraint": {"name":"__link_ids_to_fact_table__"},
                "table": ids_table,
                "column": {"name": c},
                "referenced": {
                    "table": prime_table,
                    "column": {
                        "name": c
                    }
                },
                "ordinal_position": i
            })
            for i, c in enumerate(prime_table.id)
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
                    include = self.settings.show_ids
                    reference = False
                else:
                    include = False
                    reference = False
            elif c.column_name in tables[(c.table_name, c.table_schema)].id:
                include = self.settings.show_ids
                reference = False
            elif (c.column_name, c.table_name, c.table_schema) in related_column_table_schema_triples:
                include = self.settings.show_ids
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
        todo = DictList()
        output_columns = DictList()
        nested_path_to_join = {}
        all_nested_paths = [["."]]
        done_relations = []

        def follow_paths(position, path, nested_path):
            if position.name in self.settings.exclude:
                return
            Log.note("Trace {{path}}", path=path)
            if position.name!="__ids__":
                self.db.query("SELECT * FROM "+self.db.quote_column(position.name, position.schema)+" LIMIT 1")

            if position.name in reference_only_tables:
                return

            curr_join_list = copy(nested_path_to_join[nested_path[0]])

            # REFERENCE TABLES
            referenced_tables = list(jx.groupby(jx.filter(relations, {"eq": {"table.name": position.name, "table.schema": position.schema}}), "constraint.name"))
            for g, constraint_columns in referenced_tables:
                g = unwrap(g)
                constraint_columns = deepcopy(constraint_columns)
                if g["constraint.name"] in done_relations:
                    continue
                if any(c for c in constraint_columns if c.referenced.table.name in self.settings.exclude):
                    continue

                done_relations.append(g["constraint.name"])

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

                referenced_table_path = join_field(split_field(path) + ["/".join(constraint_columns.referenced.table.name)])

                for col in columns:
                    if col.table.name == constraint_columns[0].referenced.table.name and col.table.schema == constraint_columns[0].referenced.table.schema:
                        # HANDLE THE COMMON *id SUFFIX
                        name = []
                        for a, b in zip(constraint_columns.column.name, constraint_columns.referenced.table.name):
                            if a.startswith(b):
                                name.append(b)
                            elif a.endswith("_id"):
                                name.append(a[:-3])
                            else:
                                name.append(a)

                        col_pointer_name = relative_field(referenced_table_path, nested_path[0])
                        col_full_name = concat_field(col_pointer_name, literal_field(col.column.name))

                        if col.column.name == constraint_columns[0].column.name:
                            c_index = len(output_columns)
                            output_columns.append({
                                "table_alias": alias,
                                "column_alias": "c"+unicode(c_index),
                                "column": col,
                                "path": referenced_table_path,
                                "nested_path": nested_path,
                                "put": col_full_name if self.settings.show_ids else None
                            })
                        elif col.is_id:
                            c_index = len(output_columns)
                            output_columns.append({
                                "table_alias": alias,
                                "column_alias": "c"+unicode(c_index),
                                "column": col,
                                "path": referenced_table_path,
                                "nested_path": nested_path,
                                "put": col_full_name if self.settings.show_ids else None
                            })
                        elif col.reference:
                            c_index = len(output_columns)
                            output_columns.append({
                                "table_alias": alias,
                                "column_alias": "c"+unicode(c_index),
                                "column": col,
                                "path": referenced_table_path,
                                "nested_path": nested_path,
                                "put": col_pointer_name if not self.settings.show_ids else col_full_name  # REFERENCE FIELDS CAN REPLACE THE WHOLE OBJECT BEING REFERENCED
                            })
                        elif col.include:
                            c_index = len(output_columns)
                            output_columns.append({
                                "table_alias": alias,
                                "column_alias": "c"+unicode(c_index),
                                "column": col,
                                "path": referenced_table_path,
                                "nested_path": nested_path,
                                "put": col_full_name
                            })

                if position.name in reference_only_tables:
                    continue

                todo.append(Dict(
                    position=constraint_columns[0].referenced.table.copy(),
                    path=referenced_table_path,
                    nested_path=nested_path
                ))

            # CHILDREN
            for g, constraint_columns in jx.groupby(jx.filter(relations, {"eq": {"referenced.table.name": position.name, "referenced.table.schema": position.schema}}), "constraint.name"):
                g = unwrap(g)
                constraint_columns = deepcopy(constraint_columns)
                if g["constraint.name"] in done_relations:
                    continue
                done_relations.append(g["constraint.name"])

                many_table = set(constraint_columns.table.name)
                if not (many_table - self.settings.exclude):
                    continue

                referenced_table_path = join_field(split_field(path) + ["/".join(many_table)])
                new_nested_path = [referenced_table_path] + nested_path
                all_nested_paths.append(new_nested_path)

                # if new_path not in self.settings.include:
                #     Log.note("Exclude nested path {{path}}", path=new_path)
                #     continue
                one_to_many_joins = nested_path_to_join[referenced_table_path] = copy(curr_join_list)
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
                        col_full_name = join_field(split_field(referenced_table_path)[len(split_field(new_nested_path[0])):]+[literal_field(col.column.name)])

                        if col.column.name == constraint_columns[0].column.name:
                            c_index = len(output_columns)
                            output_columns.append({
                                "table_alias": alias,
                                "column_alias": "c"+unicode(c_index),
                                "column": col,
                                "path": referenced_table_path,
                                "nested_path": new_nested_path,
                                "put": col_full_name if self.settings.show_ids else None
                            })
                        elif col.is_id:
                            c_index = len(output_columns)
                            output_columns.append({
                                "table_alias": alias,
                                "column_alias": "c"+unicode(c_index),
                                "column": col,
                                "path": referenced_table_path,
                                "nested_path": new_nested_path,
                                "put": col_full_name if self.settings.show_ids else None
                            })
                        else:
                            c_index = len(output_columns)
                            output_columns.append({
                                "table_alias": alias,
                                "column_alias": "c"+unicode(c_index),
                                "column": col,
                                "path": referenced_table_path,
                                "nested_path": new_nested_path,
                                "put": col_full_name
                            })

                todo.append(Dict(
                    position=constraint_columns[0].table,
                    path=referenced_table_path,
                    nested_path=new_nested_path
                ))

        path = "."
        nested_path = [path]
        nested_path_to_join["."] = [{
            "path": path,
            "join_columns": [{"referenced": {"table": ids_table}}],
            "nested_path": nested_path
        }]

        todo.append(Dict(
            position=ids_table,
            path=path,
            nested_path=nested_path
        ))

        while todo:
            item = todo.pop(0)
            follow_paths(**item)

        sql = self.compose_sql(all_nested_paths, nested_path_to_join, output_columns)

        return Dict(
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
                    sql_joins.append("\nFROM (" + self.settings.ids + ") AS " + rel.referenced.table.alias)
                elif curr_join.children:
                    full_name = self.db.quote_column(rel.table.schema) + "." + self.db.quote_column(rel.table.name)
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
        columns = meta.columns

        # ORDERING
        sort = []
        ordering = []
        for ci, c in enumerate(columns):
            if c.column.is_id:
                sort.append(c.column_alias + " IS NULL")
                sort.append(c.column_alias)
                ordering.append(ci)

        sql = "\nUNION ALL\n".join(meta.sql)
        sql = "SELECT * FROM ("+sql+") as a\nORDER BY "+",".join(sort)

        Log.note("{{sql}}", sql=sql)

        cursor = self.db.db.cursor()
        cursor.execute(sql)
        data = list(cursor)
        cursor.close()

        File("cursor.json").write(convert.value2json(data, pretty=True))

        # data = convert.json2value(File("cursor.json").read())

        output = []
        null_values=set(self.settings.null_values) | {None}

        for row in data:
            nested_path = []
            curr_record = None
            for ci, c in enumerate(columns):
                value = row[ci]
                if value in null_values:
                    continue
                if len(nested_path) < len(c.nested_path):
                    nested_path = c.nested_path
                    curr_record = Dict()
                if c.put != None:
                    try:
                        curr_record[c.put] = value
                    except Exception, e:
                        Log.warning("should not happen", cause=e)

            children = output
            for n in jx.reverse(nested_path)[1::]:
                parent = children[-1]
                children = parent[n]
                if children == None:
                    children = parent[n] = []

            children.append(curr_record)

        File("result.json").write(convert.value2json(output, pretty=True))
        # Log.note("{{data}}", data=output)

        # PUSH TO S3
        pass


def main():
    try:
        settings = startup.read_settings()
        constants.set(settings.constants)
        Log.start(settings.debug)

        e = Extract(settings)
        meta = e.make_sql()
        # Log.note("{{sql}}", sql=meta.sql)
        File("meta.json").write(convert.value2json(meta, pretty=True, sort_keys=True))
        meta = convert.json2value(File("meta.json").read())
        e.extract(meta)

        # Log.note("{{sql}}", sql=sql)

    except Exception, e:
        Log.error("Problem with data extraction", e)
    finally:
        Log.stop()


if __name__=="__main__":
    main()