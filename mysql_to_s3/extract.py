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

from pyLibrary.debugs import constants, startup
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import Dict, wrap, join_field, set_default, startswith_field, Null, split_field, DictList
from pyLibrary.meta import use_settings
from pyLibrary.queries import jx
from pyLibrary.queries.unique_index import UniqueIndex
from pyLibrary.sql.mysql import MySQL


class Extract(object):

    @use_settings
    def __init__(self, settings=None):
        self.settings=settings
        self.db = MySQL(**settings.database)



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
                table_schema={{schema}} AND
                referenced_column_name IS NOT NULL
        """, param=self.settings.database)

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
            WHERE
                t.table_schema = {{schema}}
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

        # GET ALL COLUMNS
        raw_columns = self.db.query("""
            SELECT
                column_name,
                table_schema,
                table_name,
                ordinal_position
            FROM
                information_schema.columns
            WHERE
                table_schema={{schema}}
        """, param=self.settings.database)

        columns = UniqueIndex(["column.name", "table.name", "table.schema"])
        for c in raw_columns:
            col = {
                "column":{"name": c.column_name},
                "table": {
                    "name": c.table_name,
                    "schema": c.table_schema
                },
                "ordinal_position": c.ordinal_position,
                "is_id": c.column_name in tables[(c.table_name, c.table_schema)].id
            }
            columns.add(col)

        # ITERATE OVER ALL PATHS
        todo = DictList()
        output_columns = DictList()
        joins = DictList()
        all_nested_paths = [["."]]
        done_relations = []

        def follow_paths(position, path, nested_path):
            Log.note("Trace {{path}}", path=path)

            if position.name in self.settings.exclude:
                return

            # REFERENCE TABLES
            for g, c in jx.groupby(jx.filter(relations, {"eq": {"table.name": position.name, "table.schema": position.schema}}), "constraint.name"):
                c = wrap(list(c))
                if g.constraint.name in done_relations:
                    continue
                done_relations.append(g.constraint.name)

                index = len(joins)
                joins.append({
                    "table_alias": "t"+unicode(index),
                    "join_columns": c,
                    "path": path,
                    "nested_path": nested_path
                })

                for col in columns:
                    if col.table == c[0].referenced.table:
                        c_index = len(output_columns)
                        output_columns.append({
                            "table_alias": "t"+unicode(index),
                            "column_alias": "c"+unicode(c_index),
                            "column": col,
                            "path": path,
                            "nested_path": nested_path
                        })

                todo.append(Dict(
                    position=c[0].referenced.table,
                    path=join_field(split_field(path) + ["/".join(c.column.name)]),
                    nested_path=nested_path
                ))

            if position.name in self.settings.reference_tables:
                return

            # CHILDREN
            for g, c in jx.groupby(jx.filter(relations, {"eq": {"referenced.table.name": position.name, "referenced.table.schema": position.schema}}), "constraint.name"):
                c = wrap(list(c))
                if g.constraint.name in done_relations:
                    continue
                done_relations.append(g.constraint.name)

                new_path = join_field(split_field(path) + ["/".join(c.table.name)])
                new_nested_path = [new_path] + nested_path
                all_nested_paths.append(new_nested_path)

                # if new_path not in self.settings.include:
                #     Log.note("Exclude nested path {{path}}", path=new_path)
                #     continue

                index = len(joins)
                joins.append(set_default({}, g, {
                    "children": True,
                    "table_alias": "t"+unicode(index),
                    "join_columns": c,
                    "path": path,
                    "nested_path": nested_path
                }))

                for col in columns:
                    if col.table == c[0].table:
                        c_index = len(output_columns)
                        output_columns.append({
                            "table_alias": "t"+unicode(index),
                            "column_alias": "c"+unicode(c_index),
                            "column": col,
                            "path": new_path,
                            "nested_path": new_nested_path
                        })

                todo.append(Dict(
                    position=c[0].table,
                    path=new_path,
                    nested_path=new_nested_path
                ))

        position = Dict(name=self.settings.fact_table, schema=self.settings.database.schema)
        path = "."
        nested_path = [path]
        joins.append({
            "table_alias": "t0",
            "path": path,
            "nested_path": nested_path
        })

        # SELECT COLUMNS
        for col in columns:
            if col.table == position:
                c_index = len(output_columns)
                output_columns.append({
                    "table_alias": "t0",
                    "column_alias": "c"+unicode(c_index),
                    "column": col,
                    "path": path,
                    "nested_path": nested_path
                })

        todo.append(Dict(
            position=position,
            path=path,
            nested_path=nested_path
        ))

        while todo:
            item = todo.pop(0)
            follow_paths(**item)

        # GENERATE SQL
        # HOW TO AVOID JOINING LOOKUP TABLES WHEN PROVIDING NESTED RECORDS?
        # INCLUDE ID FOR EACH PARENT IN THE NESTED PATH
        # INCLUDE ALL COLUMNS FOR INNER OBJECTS TOO
        sql = []
        for nested_path in all_nested_paths:
            # MAKE THE REQUIRED JOINS
            sql_joins = []
            prev = None
            for i, j in enumerate(joins):
                if not startswith_field(j.path, nested_path):
                    continue
                try:
                    if i==0:
                        "\nFROM " + self.db.quote_column(j.table.name) + " AS " + j.table_alias
                    else:
                        sql_joins.append(
                            "\nLEFT JOIN " + self.db.quote_column(j.table.name) + " AS " + j.table_alias +
                            "\nON "+" AND ".join(
                                j.table_alias + "." + self.db.quote_column(c.referenced.column.name) + "=" + prev.table_alias + "." + self.db.quote_column(c.column.name)
                                for c in j.join_columns
                            )
                        )
                finally:
                    prev=j

            # ONLY SELECT WHAT WE NEED, NULL THE REST
            selects = []
            for c in output_columns:
                if not startswith_field(c.path):
                    selects.append("NULL")
                    continue
                if c.nested_path[0] != nested_path:
                    for col in tables[c.table].id:
                        details = columns[col, c.column.table.name, c.column.table.schema]
                        selects.append(
                            details.table_alias + "." + details.column.name + " AS " + details.column.alias
                        )
            sql.append("SELECT " + ",".join(selects) + "".join(sql_joins))

        return "\nUNION ALL\n".join(sql)


    def extract(self):
        # DETERMINE WHICH RECORDS TO PULL

        # PULL DATA IN BLOCKS

        # PUSH TO S3
        pass



def main():
    try:
        settings = startup.read_settings()
        constants.set(settings.constants)
        Log.start(settings.debug)

        sql = Extract(settings).make_sql()

    except Exception, e:
        Log.error("Problem with data extraction", e)
    finally:
        Log.stop()


if __name__=="__main__":
    main()
