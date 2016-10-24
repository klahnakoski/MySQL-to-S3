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
from pyLibrary.dot import Dict, wrap, join_field, set_default, startswith_field, Null, split_field, DictList, coalesce, literal_field
from pyLibrary.env.files import File
from pyLibrary.maths.randoms import Random
from pyLibrary.meta import use_settings
from pyLibrary.queries import jx
from pyLibrary.queries.expressions import jx_expression
from pyLibrary.queries.meta import Column
from pyLibrary.queries.unique_index import UniqueIndex
from pyLibrary.sql.mysql import MySQL, mysql_type_to_json_type


class Extract(object):

    @use_settings
    def __init__(self, settings=None):
        self.settings=settings
        self.settings.exclude = set(self.settings.exclude)
        self.settings.show_ids = coalesce(self.settings.show_ids, True)
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
                ordinal_position,
                data_type
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
                    include = True
                elif c.column_name in tables[(c.table_name, c.table_schema)].id:
                    include = self.settings.show_ids
                    reference = False
                else:
                    include = False
                    reference = False
            elif c.column_name in tables[(c.table_name, c.table_schema)].id:
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
                        # HANDLE THE COMMON *id SUFFIX
                        name = []
                        for a, b in zip(constraint_columns.column.name, constraint_columns.referenced.table.name):
                            if a.startswith(b):
                                name.append(b)
                            elif a.endswith("_id"):
                                name.append(a[:-3])
                            else:
                                name.append(a)

                        col_full_name = join_field(split_field(path)[len(split_field(nested_path[0])):]+[literal_field(col.column.name)])

                        if col.column.name == constraint_columns[0].column.name:
                            c_index = len(output_columns)
                            output_columns.append({
                                "table_alias": alias,
                                "column_alias": "c"+unicode(c_index),
                                "column": col,
                                "path": new_path,
                                "nested_path": nested_path,
                                "put": col_full_name if self.settings.show_ids else None
                            })
                        elif col.is_id:
                            c_index = len(output_columns)
                            output_columns.append({
                                "table_alias": alias,
                                "column_alias": "c"+unicode(c_index),
                                "column": col,
                                "path": new_path,
                                "nested_path": nested_path,
                                "put": col_full_name if self.settings.show_ids else None
                            })
                        elif col.reference:
                            c_index = len(output_columns)
                            output_columns.append({
                                "table_alias": alias,
                                "column_alias": "c"+unicode(c_index),
                                "column": col,
                                "path": new_path,
                                "nested_path": nested_path,
                                "put": col_full_name
                            })
                        elif col.include:
                            c_index = len(output_columns)
                            output_columns.append({
                                "table_alias": alias,
                                "column_alias": "c"+unicode(c_index),
                                "column": col,
                                "path": new_path,
                                "nested_path": nested_path,
                                "put": col_full_name
                            })

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

                many_table = set(constraint_columns.table.name)
                if not (many_table - self.settings.exclude):
                    continue

                new_path = join_field(split_field(path) + ["/".join(many_table)])
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
                        col_full_name = join_field(split_field(new_path)[len(split_field(new_nested_path[0])):]+[literal_field(col.column.name)])

                        if col.column.name == constraint_columns[0].column.name:
                            c_index = len(output_columns)
                            output_columns.append({
                                "table_alias": alias,
                                "column_alias": "c"+unicode(c_index),
                                "column": col,
                                "path": new_path,
                                "nested_path": new_nested_path,
                                "put": col_full_name if self.settings.show_ids else None
                            })
                        elif col.is_id:
                            c_index = len(output_columns)
                            output_columns.append({
                                "table_alias": alias,
                                "column_alias": "c"+unicode(c_index),
                                "column": col,
                                "path": new_path,
                                "nested_path": new_nested_path,
                                "put": col_full_name if self.settings.show_ids else None
                            })
                        else:
                            c_index = len(output_columns)
                            output_columns.append({
                                "table_alias": alias,
                                "column_alias": "c"+unicode(c_index),
                                "column": col,
                                "path": new_path,
                                "nested_path": new_nested_path,
                                "put": col_full_name
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
        schema = {}

        # SELECT COLUMNS
        for rel in columns:
            if rel.table.name == position.name and rel.table.schema == position.schema:
                c_index = len(output_columns)
                output_columns.append({
                    "table_alias": "t0",
                    "column_alias": "c"+unicode(c_index),
                    "column": rel,
                    "path": path,
                    "nested_path": nested_path,
                    "put": literal_field(rel.column.name)
                })
                schema[literal_field(rel.column.name)] = {Column(
                    name="c"+unicode(c_index),
                    table=position.alias,
                    es_column=self.db.quote_column(rel.column.name),
                    es_index=position.alias,
                    type=mysql_type_to_json_type[rel.column.type],
                    nested_path=nested_path,
                    relative=False
                )}

        ex = jx_expression(coalesce(self.settings.where, True))
        self.settings.where = ex.to_sql(schema)[0].sql.b

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
                        "\nJOIN " + full_name + " AS " + rel.table.alias +
                        "\nON " + " AND ".join(
                            rel.table.alias + "." + self.db.quote_column(const_col.column.name) + "=" + rel.referenced.table.alias + "." + self.db.quote_column(const_col.referenced.column.name)
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
            for ci, c in enumerate(output_columns):
                if c.column_alias[1:]!=unicode(ci):
                    Log.error("expecting consistency")
                if c.nested_path[0] == nested_path[0]:
                    s = c.table_alias+"."+c.column.column.name+" as "+c.column_alias
                    if s==None:
                        Log.error("bug")
                    selects.append(s)
                    not_null_column_seen = True
                elif startswith_field(nested_path[0], c.path):
                    # PARENT ID REFERENCES
                    if c.column.is_id:
                        s = c.table_alias+"."+c.column.column.name+" as "+c.column_alias
                        selects.append(s)
                        not_null_column_seen = True
                    else:
                        selects.append("NULL as "+c.column_alias)
                else:
                    selects.append("NULL as "+c.column_alias)

            if not_null_column_seen:
                sql.append("SELECT\n\t" + ",\n\t".join(selects) + "".join(sql_joins))

        return Dict(
            sql=sql,
            columns=output_columns,
            where=self.settings.where
        )

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

        sql = "\nUNION ALL\n".join([s+"\nWHERE "+meta.where for s in meta.sql])
        sql = "SELECT * FROM ("+sql+") as a\nORDER BY "+",".join(sort)

        cursor = self.db.db.cursor()
        cursor.execute(sql)

        output = DictList()

        for row in cursor:
            nested_path = []
            curr_record = Dict()
            for ci, c in enumerate(columns):
                value = row[ci]
                if value != None:
                    if len(nested_path) < len(c.nested_path):
                        nested_path = c.nested_path
                        curr_record = Dict()
                    if c.put != None:
                        curr_record[c.put] = value

            children = output
            for n in jx.reverse(nested_path)[1::]:
                parent = children.last()
                children = parent[n]
                if children == None:
                    children = parent[n] = []

            children.append(curr_record)

        Log.note("{{data}}", data=output)

        cursor.close()
        # PUSH TO S3
        pass



def main():
    try:
        settings = startup.read_settings()
        constants.set(settings.constants)
        Log.start(settings.debug)

        e = Extract(settings)
        meta = e.make_sql()
        Log.note("{{sql}}", sql=meta.sql)
        File("temp.json").write(convert.value2json(meta, pretty=True, sort_keys=True))
        meta = convert.json2value(File("temp.json").read())
        e.extract(meta)

        # Log.note("{{sql}}", sql=sql)

    except Exception, e:
        Log.error("Problem with data extraction", e)
    finally:
        Log.stop()


if __name__=="__main__":
    main()
