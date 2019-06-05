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

from jx_python import jx
from mo_collections import UniqueIndex
from mo_dots import (
    coalesce,
    Data,
    wrap,
    Null,
    FlatList,
    unwrap,
    join_field,
    split_field,
    relative_field,
    concat_field,
    literal_field,
    set_default,
    startswith_field,
    listwrap,
)
from mo_future import text_type
from mo_kwargs import override
from mo_logs import Log, strings
from mo_logs.exceptions import Explanation
from mo_math.randoms import Random
from pyLibrary.sql import (
    SQL_SELECT,
    sql_list,
    sql_alias,
    SQL_NULL,
    sql_iso,
    SQL_FROM,
    SQL_LEFT_JOIN,
    sql_and,
    SQL_ON,
    SQL_JOIN,
    SQL_UNION_ALL,
    SQL_ORDERBY,
    SQL_STAR,
    SQL_IS_NOT_NULL,
    SQL,
)
from pyLibrary.sql.mysql import MySQL, quote_column

DEBUG = False


class SnowflakeSchema(object):
    @override
    def __init__(self, kwargs=None):
        self.settings = kwargs
        self.settings.exclude = set(listwrap(self.settings.exclude))
        self.settings.exclude_path = list(
            map(split_field, listwrap(self.settings.exclude_path))
        )
        self.settings.show_foreign_keys = coalesce(
            self.settings.show_foreign_keys, True
        )

        self.all_nested_paths = None
        self.nested_path_to_join = None
        self.columns = None

        with Explanation("scan database", debug=DEBUG):
            self.db = MySQL(**kwargs.database)
            with self.db:
                with self.db.transaction():
                    self._scan_database()

    def get_sql(self, get_ids):
        sql = self._compose_sql(get_ids)

        # ORDERING
        sort = []
        ordering = []
        for ci, c in enumerate(self.columns):
            if c.sort:
                sort.append(quote_column(c.column_alias) + SQL_IS_NOT_NULL)
                sort.append(quote_column(c.column_alias))
                ordering.append(ci)

        union_all_sql = SQL_UNION_ALL.join(sql)
        union_all_sql = (
            SQL_SELECT
            + SQL_STAR
            + SQL_FROM
            + sql_alias(sql_iso(union_all_sql), quote_column("a"))
            + SQL_ORDERBY
            + sql_list(sort)
        )
        return union_all_sql

    def path_not_allowed(self, path):
        return path != "." and any(
            path_in_path(e, p)
            for p in [split_field(path)]
            for e in self.settings.exclude_path
        )

    def _scan_database(self):
        # GET ALL RELATIONS
        raw_relations = self.db.query(
            """
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
        """,
            param=self.settings.database,
        )

        if not raw_relations:
            Log.error("No relations in the database")

        for r in self.settings.add_relations:
            try:
                a, b = map(strings.trim, r.split("->"))
                a = a.split(".")
                b = b.split(".")
                raw_relations.append(
                    Data(
                        table_schema=a[0],
                        table_name=a[1],
                        referenced_table_schema=b[0],
                        referenced_table_name=b[1],
                        referenced_column_name=b[2],
                        constraint_name=Random.hex(20),
                        column_name=a[2],
                        ordinal_position=1,
                    )
                )
            except Exception as e:
                Log.error("Could not parse {{line|quote}}", line=r, cause=e)

        relations = jx.select(
            raw_relations,
            [
                {"name": "constraint.name", "value": "constraint_name"},
                {"name": "table.schema", "value": "table_schema"},
                {"name": "table.name", "value": "table_name"},
                {"name": "column.name", "value": "column_name"},
                {"name": "referenced.table.schema", "value": "referenced_table_schema"},
                {"name": "referenced.table.name", "value": "referenced_table_name"},
                {"name": "referenced.column.name", "value": "referenced_column_name"},
                {"name": "ordinal_position", "value": "ordinal_position"},
            ],
        )

        # GET ALL TABLES
        raw_tables = self.db.query(
            """
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
        """,
            param=self.settings.database,
        )

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
                    if (
                        r.table.name == t.table_name
                        and r.table.schema == t.table_schema
                        and r.constraint.name == g.constraint_name
                    ):
                        ref = True
                is_prime = w[0].constraint_type == "PRIMARY"

                reasons_this_one_is_better = [
                    best_index == None,  # WE DO NOT HAVE A CANDIDATE YET
                    is_prime and not is_primary,  # PRIMARY KEYS ARE GOOD TO HAVE
                    is_primary == is_prime
                    and ref
                    and not is_referenced,  # REFERENCED UNIQUE TUPLES ARE GOOD TOO
                    is_primary == is_prime
                    and ref == is_referenced
                    and len(w) < len(best_index),  # THE SHORTER THE TUPLE, THE BETTER
                ]
                if any(reasons_this_one_is_better):
                    is_primary = is_prime
                    is_referenced = ref
                    best_index = w

            tables.add(
                {
                    "name": t.table_name,
                    "schema": t.table_schema,
                    "id": [b.column_name for b in best_index],
                }
            )

        fact_table = tables[self.settings.fact_table, self.settings.database.schema]
        ids_table = {
            "alias": "t0",
            "name": "__ids__",
            "schema": fact_table.schema,
            "id": fact_table.id,
        }
        relations.extend(
            wrap(
                {
                    "constraint": {"name": "__link_ids_to_fact_table__"},
                    "table": ids_table,
                    "column": {"name": c},
                    "referenced": {"table": fact_table, "column": {"name": c}},
                    "ordinal_position": i,
                }
            )
            for i, c in enumerate(fact_table.id)
        )
        tables.add(ids_table)

        # GET ALL COLUMNS
        raw_columns = self.db.query(
            """
            SELECT
                column_name,
                table_schema,
                table_name,
                ordinal_position,
                data_type
            FROM
                information_schema.columns
        """,
            param=self.settings.database,
        )

        reference_only_tables = [
            r.split(".")[0]
            for r in self.settings.reference_only
            if len(r.split(".")) == 2
        ]
        reference_all_tables = [
            r.split(".")[0]
            for r in self.settings.reference_only
            if len(r.split(".")) == 1
        ]
        foreign_column_table_schema_triples = {
            (r.column.name, r.table.name, r.table.schema) for r in relations
        }
        referenced_column_table_schema_triples = {
            (
                r.referenced.column.name,
                r.referenced.table.name,
                r.referenced.table.schema,
            )
            for r in relations
        }
        related_column_table_schema_triples = (
            foreign_column_table_schema_triples | referenced_column_table_schema_triples
        )

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
                elif (
                    c.column_name,
                    c.table_name,
                    c.table_schema,
                ) in foreign_column_table_schema_triples:
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
            elif (
                c.column_name,
                c.table_name,
                c.table_schema,
            ) in foreign_column_table_schema_triples:
                include = False
                reference = False
                foreign = True
            elif (
                c.column_name,
                c.table_name,
                c.table_schema,
            ) in referenced_column_table_schema_triples:
                include = self.settings.show_foreign_keys
                reference = False
                foreign = False
            else:
                include = True
                reference = False
                foreign = False

            rel = {
                "column": {"name": c.column_name, "type": c.data_type},
                "table": {"name": c.table_name, "schema": c.table_schema},
                "ordinal_position": c.ordinal_position,
                "is_id": c.column_name in tables[(c.table_name, c.table_schema)].id,
                "include": include,  # TRUE IF THIS COLUMN IS OUTPUTTED
                "reference": reference,  # TRUE IF THIS COLUMN REPRESENTS THE ROW
                "foreign": foreign,  # TRUE IF THIS COLUMN POINTS TO ANOTHER ROW
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

            if self.path_not_allowed(path):
                return
            if DEBUG:
                Log.note("Trace {{path}}", path=path)
            if position.name != "__ids__":
                # USED TO CONFIRM WE CAN ACCESS THE TABLE (WILL THROW ERROR WHEN IF IT FAILS)
                self.db.query(
                    "SELECT * FROM "
                    + quote_column(position.name, position.schema)
                    + " LIMIT 1"
                )

            if position.name in reference_all_tables:
                no_nested_docs = True
            if position.name in reference_only_tables:
                return

            curr_join_list = copy(nested_path_to_join[nested_path[0]])

            # INNER OBJECTS
            referenced_tables = list(
                jx.groupby(
                    jx.filter(
                        relations,
                        {
                            "eq": {
                                "table.name": position.name,
                                "table.schema": position.schema,
                            }
                        },
                    ),
                    "constraint.name",
                )
            )
            for g, constraint_columns in referenced_tables:
                g = unwrap(g)
                constraint_columns = deepcopy(constraint_columns)
                if g["constraint.name"] in done_relations:
                    continue
                if any(
                    cc
                    for cc in constraint_columns
                    if cc.referenced.table.name in self.settings.exclude
                ):
                    continue

                done_relations.add(g["constraint.name"])

                many_to_one_joins = nested_path_to_join[nested_path[0]]
                index = len(many_to_one_joins)

                alias = "t" + text_type(index)
                for c in constraint_columns:
                    c.referenced.table.alias = alias
                    c.table = position
                many_to_one_joins.append(
                    {
                        "join_columns": constraint_columns,
                        "path": path,
                        "nested_path": nested_path,
                    }
                )

                # referenced_table_path = join_field(split_field(path) + ["/".join(constraint_columns.referenced.table.name)])
                # HANDLE THE COMMON *id SUFFIX
                name = []
                for a, b in zip(
                    constraint_columns.column.name,
                    constraint_columns.referenced.table.name,
                ):
                    if a.startswith(b):
                        name.append(b)
                    elif a.endswith("_id"):
                        name.append(a[:-3])
                    else:
                        name.append(a)
                referenced_column_path = join_field(
                    split_field(path) + ["/".join(name)]
                )
                col_pointer_name = relative_field(
                    referenced_column_path, nested_path[0]
                )
                # insert into nested1 VALUES (100, 10, 'aaa', -1);
                # id.about.time.nested1 .ref=10
                # id.about.time.nested1 .ref.name
                for col in columns:
                    if (
                        col.table.name == constraint_columns[0].referenced.table.name
                        and col.table.schema
                        == constraint_columns[0].referenced.table.schema
                    ):
                        col_full_name = concat_field(
                            col_pointer_name, literal_field(col.column.name)
                        )

                        if (
                            col.is_id
                            and col.table.name == fact_table.name
                            and col.table.schema == fact_table.schema
                        ):
                            # ALWAYS SHOW THE ID OF THE FACT
                            c_index = len(output_columns)
                            output_columns.append(
                                {
                                    "table_alias": alias,
                                    "column_alias": "c" + text_type(c_index),
                                    "column": col,
                                    "sort": True,
                                    "path": referenced_column_path,
                                    "nested_path": nested_path,
                                    "put": col_full_name,
                                }
                            )
                        elif col.column.name == constraint_columns[0].column.name:
                            c_index = len(output_columns)
                            output_columns.append(
                                {
                                    "table_alias": alias,
                                    "column_alias": "c" + text_type(c_index),
                                    "column": col,
                                    "sort": False,
                                    "path": referenced_column_path,
                                    "nested_path": nested_path,
                                    "put": col_full_name
                                    if self.settings.show_foreign_keys
                                    else None,
                                }
                            )
                        elif col.is_id:
                            c_index = len(output_columns)
                            output_columns.append(
                                {
                                    "table_alias": alias,
                                    "column_alias": "c" + text_type(c_index),
                                    "column": col,
                                    "sort": False,
                                    "path": referenced_column_path,
                                    "nested_path": nested_path,
                                    "put": col_full_name
                                    if self.settings.show_foreign_keys
                                    else None,
                                }
                            )
                        elif col.reference:
                            c_index = len(output_columns)
                            output_columns.append(
                                {
                                    "table_alias": alias,
                                    "column_alias": "c" + text_type(c_index),
                                    "column": col,
                                    "sort": False,
                                    "path": referenced_column_path,
                                    "nested_path": nested_path,
                                    "put": col_pointer_name
                                    if not self.settings.show_foreign_keys
                                    else col_full_name,  # REFERENCE FIELDS CAN REPLACE THE WHOLE OBJECT BEING REFERENCED
                                }
                            )
                        elif col.include:
                            c_index = len(output_columns)
                            output_columns.append(
                                {
                                    "table_alias": alias,
                                    "column_alias": "c" + text_type(c_index),
                                    "column": col,
                                    "sort": False,
                                    "path": referenced_column_path,
                                    "nested_path": nested_path,
                                    "put": col_full_name,
                                }
                            )

                if position.name in reference_only_tables:
                    continue

                todo.append(
                    Data(
                        position=copy(constraint_columns[0].referenced.table),
                        path=referenced_column_path,
                        nested_path=nested_path,
                        done_relations=copy(done_relations),
                        no_nested_docs=no_nested_docs,
                    )
                )

            # NESTED OBJECTS
            if not no_nested_docs:
                for g, constraint_columns in jx.groupby(
                    jx.filter(
                        relations,
                        {
                            "eq": {
                                "referenced.table.name": position.name,
                                "referenced.table.schema": position.schema,
                            }
                        },
                    ),
                    "constraint.name",
                ):
                    g = unwrap(g)
                    constraint_columns = deepcopy(constraint_columns)
                    if g["constraint.name"] in done_relations:
                        continue
                    done_relations.add(g["constraint.name"])

                    many_table = set(constraint_columns.table.name)
                    if not (many_table - self.settings.exclude):
                        continue

                    relation_string = one_to_many_string(constraint_columns[0])
                    step = "/".join(many_table)
                    if len(constraint_columns) == 1:
                        step = coalesce(
                            self.settings.name_relations[relation_string], step
                        )

                    referenced_column_path = concat_field(path, step)
                    if self.path_not_allowed(referenced_column_path):
                        continue

                    new_nested_path = [referenced_column_path] + nested_path
                    all_nested_paths.append(new_nested_path)

                    if referenced_column_path in nested_path_to_join:
                        Log.error(
                            "{{path}} already exists, try adding entry to name_relations",
                            path=referenced_column_path,
                        )
                    # if new_path not in self.settings.include:
                    #     Log.note("Exclude nested path {{path}}", path=new_path)
                    #     continue
                    one_to_many_joins = nested_path_to_join[
                        referenced_column_path
                    ] = copy(curr_join_list)
                    index = len(one_to_many_joins)
                    alias = "t" + text_type(index)
                    for c in constraint_columns:
                        c.table.alias = alias
                        c.referenced.table = position
                    one_to_many_joins.append(
                        set_default(
                            {},
                            g,
                            {
                                "children": True,
                                "join_columns": constraint_columns,
                                "path": path,
                                "nested_path": nested_path,
                            },
                        )
                    )
                    # insert into nested1 VALUES (100, 10, 'aaa', -1); # id.about.time.nested1 .ref=10# id.about.time.nested1 .ref.name
                    for col in columns:
                        if (
                            col.table.name == constraint_columns[0].table.name
                            and col.table.schema == constraint_columns[0].table.schema
                        ):
                            col_full_name = join_field(
                                split_field(referenced_column_path)[
                                    len(split_field(new_nested_path[0])) :
                                ]
                                + [literal_field(col.column.name)]
                            )

                            if col.column.name == constraint_columns[0].column.name:
                                c_index = len(output_columns)
                                output_columns.append(
                                    {
                                        "table_alias": alias,
                                        "column_alias": "c" + text_type(c_index),
                                        "column": col,
                                        "sort": col.is_id,
                                        "path": referenced_column_path,
                                        "nested_path": new_nested_path,
                                        "put": col_full_name
                                        if self.settings.show_foreign_keys
                                        else None,
                                    }
                                )
                            elif col.is_id:
                                c_index = len(output_columns)
                                output_columns.append(
                                    {
                                        "table_alias": alias,
                                        "column_alias": "c" + text_type(c_index),
                                        "column": col,
                                        "sort": col.is_id,
                                        "path": referenced_column_path,
                                        "nested_path": new_nested_path,
                                        "put": col_full_name
                                        if self.settings.show_foreign_keys
                                        else None,
                                    }
                                )
                            else:
                                c_index = len(output_columns)
                                output_columns.append(
                                    {
                                        "table_alias": alias,
                                        "column_alias": "c" + text_type(c_index),
                                        "column": col,
                                        "sort": col.is_id,
                                        "path": referenced_column_path,
                                        "nested_path": new_nested_path,
                                        "put": col_full_name if col.include else None,
                                    }
                                )

                    todo.append(
                        Data(
                            position=constraint_columns[0].table,
                            path=referenced_column_path,
                            nested_path=new_nested_path,
                            done_relations=copy(done_relations),
                            no_nested_docs=no_nested_docs,
                        )
                    )

        path = "."
        nested_path = [path]
        nested_path_to_join["."] = [
            {
                "path": path,
                "join_columns": [{"referenced": {"table": ids_table}}],
                "nested_path": nested_path,
            }
        ]

        todo.append(
            Data(
                position=ids_table,
                path=path,
                nested_path=nested_path,
                done_relations=set(),
                no_nested_docs=False,
            )
        )

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
                    sql_joins.append(
                        SQL_FROM
                        + sql_alias(
                            sql_iso(get_ids), quote_column(rel.referenced.table.alias)
                        )
                    )
                elif curr_join.children:
                    full_name = quote_column(rel.table.name, rel.table.schema)
                    sql_joins.append(
                        SQL_JOIN
                        + sql_alias(full_name, quote_column(rel.table.alias))
                        + SQL_ON
                        + sql_and(
                            quote_column(const_col.column.name, rel.table.alias)
                            + "="
                            + quote_column(
                                const_col.referenced.column.name,
                                rel.referenced.table.alias,
                            )
                            for const_col in curr_join.join_columns
                        )
                    )
                else:
                    full_name = quote_column(
                        rel.referenced.table.name, rel.referenced.table.schema
                    )
                    sql_joins.append(
                        SQL_LEFT_JOIN
                        + sql_alias(full_name, quote_column(rel.referenced.table.alias))
                        + SQL_ON
                        + sql_and(
                            quote_column(
                                const_col.referenced.column.name,
                                rel.referenced.table.alias,
                            )
                            + "="
                            + quote_column(const_col.column.name, rel.table.alias)
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
                    s = sql_alias(
                        quote_column(c.column.column.name, c.table_alias),
                        quote_column(c.column_alias),
                    )
                    if s == None:
                        Log.error("bug")
                    selects.append(s)
                    not_null_column_seen = True
                elif startswith_field(nested_path[0], c.path):
                    # PARENT ID REFERENCES
                    if c.column.is_id:
                        s = sql_alias(
                            quote_column(c.column.column.name, c.table_alias),
                            quote_column(c.column_alias),
                        )
                        selects.append(s)
                        not_null_column_seen = True
                    else:
                        selects.append(
                            sql_alias(SQL_NULL, quote_column(c.column_alias))
                        )
                else:
                    selects.append(sql_alias(SQL_NULL, quote_column(c.column_alias)))

            if not_null_column_seen:
                sql.append(SQL_SELECT + sql_list(selects) + SQL("").join(sql_joins))
        return sql


def full_name_string(column):
    return join_field(
        [literal_field(column.table.name), literal_field(column.column.name)]
    )


def one_to_many_string(constraint):
    ref = constraint.referenced

    return full_name_string(ref) + " <- " + full_name_string(constraint)


def many_to_one_string(constraint):
    ref = constraint.referenced

    return full_name_string(constraint) + " -> " + full_name_string(ref)


def path_in_path(sub_path, full_path):
    max_length = len(full_path)
    for i, p in enumerate(full_path):
        for j, pp in enumerate(sub_path):
            ii = i + j
            if ii >= max_length:
                break
            if full_path[i + j] != pp:
                break
        else:
            return True
    return False
