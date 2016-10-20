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

from pyLibrary import strings
from pyLibrary.debugs import constants, startup
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import Dict, wrap, join_field, set_default, startswith_field, Null, split_field, DictList
from pyLibrary.maths.randoms import Random
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

        # GET ALL COLUMNS
        raw_columns = self.db.query("""
            SELECT
                column_name,
                table_schema,
                table_name,
                ordinal_position
            FROM
                information_schema.columns
        """, param=self.settings.database)

        # TODO: MARKUP COLUMNS AS UID, REFERENCED, OR EXCLUDED
        reference_tables = [r.split(".")[0] for r in self.settings.reference_only]

        columns = UniqueIndex(["column.name", "table.name", "table.schema"])
        for c in raw_columns:
            if c.table_name in reference_tables:
                if c.table_name + "." + c.column_name in self.settings.reference_only:
                    reference = True
                    exclude = False
                elif c.column_name in tables[(c.table_name, c.table_schema)].id:
                    exclude = self.settings.show_ids == False
                    reference = False
                else:
                    exclude = True
                    reference = False
            elif c.column_name in tables[(c.table_name, c.table_schema)].id:
                exclude = self.settings.show_ids == False
                reference = False
            else:
                exclude = False
                reference = False

            rel = {
                "column": {"name": c.column_name},
                "table": {
                    "name": c.table_name,
                    "schema": c.table_schema
                },
                "ordinal_position": c.ordinal_position,
                "is_id": c.column_name in tables[(c.table_name, c.table_schema)].id,
                "exclude": exclude,
                "reference": reference
            }
            columns.add(rel)

        # ITERATE OVER ALL PATHS
        todo = DictList()
        output_columns = DictList()
        nested_path_to_join = {}
        all_nested_paths = [["."]]
        done_relations = []

        def follow_paths(position, path, nested_path):
            Log.note("Trace {{path}}", path=path)

            if position.name in self.settings.exclude:
                return

            curr_join_list = copy(nested_path_to_join[nested_path[0]])

            # REFERENCE TABLES
            for g, constraint_columns in jx.groupby(jx.filter(relations, {"eq": {"table.name": position.name, "table.schema": position.schema}}), "constraint.name"):
                constraint_columns = wrap(deepcopy(list(constraint_columns)))
                if g.constraint.name in done_relations:
                    continue
                done_relations.append(g.constraint.name)

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

                new_path = join_field(split_field(path) + ["/".join(constraint_columns.referenced.table.name)])

                for col in columns:
                    if col.table.name == constraint_columns[0].referenced.table.name and col.table.schema == constraint_columns[0].referenced.table.schema:
                        # col_path = join_field(split_field(path)+[col.column.name])
                        if col.reference or col.is_id:
                            c_index = len(output_columns)
                            output_columns.append({
                                "table_alias": alias,
                                "column_alias": "c"+unicode(c_index),
                                "column": col,
                                "path": new_path,
                                "nested_path": nested_path
                            })
                        elif not col.exclude:
                            c_index = len(output_columns)
                            output_columns.append({
                                "table_alias": alias,
                                "column_alias": "c"+unicode(c_index),
                                "column": col,
                                "path": new_path,
                                "nested_path": nested_path
                            })

                # HANDLE THE COMMON *id SUFFIX
                name = []
                for a, b in zip(constraint_columns.column.name, constraint_columns.referenced.table.name):
                    if a.startswith(b):
                        name.append(b)
                    elif a.endswith("_id"):
                        name.append(a[:-3])
                    else:
                        name.append(a)

                if position.name in reference_tables:
                    continue

                todo.append(Dict(
                    position=constraint_columns[0].referenced.table.copy(),
                    path=new_path,
                    nested_path=nested_path
                ))

            if position.name in reference_tables:
                return

            # CHILDREN
            for g, constraint_columns in jx.groupby(jx.filter(relations, {"eq": {"referenced.table.name": position.name, "referenced.table.schema": position.schema}}), "constraint.name"):
                constraint_columns = wrap(deepcopy(list(constraint_columns)))
                if g.constraint.name in done_relations:
                    continue
                done_relations.append(g.constraint.name)

                new_path = join_field(split_field(path) + ["/".join(constraint_columns.table.name)])
                new_nested_path = [new_path] + nested_path
                all_nested_paths.append(new_nested_path)

                # if new_path not in self.settings.include:
                #     Log.note("Exclude nested path {{path}}", path=new_path)
                #     continue
                one_to_many_joins = nested_path_to_join[new_path] = copy(curr_join_list)
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
                        c_index = len(output_columns)
                        output_columns.append({
                            "table_alias": alias,
                            "column_alias": "c"+unicode(c_index),
                            "column": col,
                            "path": new_path,
                            "nested_path": new_nested_path
                        })

                todo.append(Dict(
                    position=constraint_columns[0].table,
                    path=new_path,
                    nested_path=new_nested_path
                ))





        position = Dict(
            name=self.settings.fact_table,
            schema=self.settings.database.schema,
            alias="t0"
        )
        path = "."
        nested_path = [path]
        nested_path_to_join["."]=[{
            "path": path,
            "join_columns": [{"referenced": {"table": position}}],
            "nested_path": nested_path
        }]

        # SELECT COLUMNS
        for rel in columns:
            if rel.table.name == position.name and rel.table.schema == position.schema:
                c_index = len(output_columns)
                output_columns.append({
                    "table_alias": "t0",
                    "column_alias": "c"+unicode(c_index),
                    "column": rel,
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

            for i, curr_join in enumerate(nested_path_to_join[nested_path[0]]):
                curr_join = wrap(curr_join)
                rel = curr_join.join_columns[0]
                if i == 0:
                    full_name = self.db.quote_column(rel.referenced.table.schema)+"."+self.db.quote_column(rel.referenced.table.name)
                    sql_joins.append("\nFROM " + full_name + " AS " + rel.referenced.table.alias)
                elif curr_join.children:
                    full_name = self.db.quote_column(rel.table.schema)+"."+self.db.quote_column(rel.table.name)
                    sql_joins.append(
                        "\nLEFT JOIN " + full_name + " AS " + rel.table.alias +
                        "\nON " + " AND ".join(
                            rel.table.alias + "." + self.db.quote_column(const_col.column.name) + "=" + rel.referenced.table.alias + "." + self.db.quote_column(const_col.column.name)
                            for const_col in curr_join.join_columns
                        )
                    )
                else:
                    full_name = self.db.quote_column(rel.referenced.table.schema)+"."+self.db.quote_column(rel.referenced.table.name)
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
            for c in output_columns:
                if c.nested_path[0] == nested_path[0]:
                    s = c.table_alias+"."+c.column.column.name+" as "+c.column_alias
                    if s==None:
                        Log.error("bug")
                    selects.append(s)
                    not_null_column_seen = True
                elif startswith_field(nested_path[0], c.path):
                    # PARENT ID REFERENCES
                    if c.is_id:
                        selects.append(c.alias)
                        not_null_column_seen = True
                    else:
                        selects.append("NULL")
                else:
                    selects.append("NULL")

            if not_null_column_seen:
                sql.append("SELECT " + ",\n".join(selects) + "".join(sql_joins))

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
        Log.note("{{sql}}", sql=sql)

    except Exception, e:
        Log.error("Problem with data extraction", e)
    finally:
        Log.stop()


if __name__=="__main__":
    main()
