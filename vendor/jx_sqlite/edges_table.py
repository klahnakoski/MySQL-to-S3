# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#


from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from jx_base.domains import DefaultDomain, TimeDomain, DurationDomain
from jx_python import jx
from jx_sqlite import get_column, _make_column_name, sql_text_array_to_set, STATS, sql_aggs, ColumnMapping, untyped_column, quoted_PARENT, quoted_UID
from jx_sqlite.expressions import Variable, sql_type_to_json_type, TupleOp
from jx_sqlite.setop_table import SetOpTable
from mo_dots import listwrap, coalesce, split_field, join_field, startswith_field, relative_field, concat_field
from mo_future import text_type, unichr
from mo_logs import Log
from mo_math import Math
from pyLibrary.sql import SQL, SQL_AND, SQL_COMMA, SQL_OR, SQL_UNION_ALL, SQL_LEFT_JOIN, SQL_INNER_JOIN, SQL_GROUPBY, SQL_WHERE, SQL_FROM, SQL_SELECT, SQL_LIMIT, SQL_ORDERBY, SQL_ON, SQL_NULL, SQL_IS_NULL, SQL_IS_NOT_NULL, sql_list, sql_iso, SQL_END, SQL_ELSE, SQL_THEN, SQL_WHEN, SQL_CASE, SQL_ONE, sql_count, SQL_DESC, SQL_STAR, SQL_TRUE, sql_alias, sql_coalesce
from pyLibrary.sql.sqlite import quote_value, quote_column, join_column

EXISTS_COLUMN = quote_column("__exists__")


class EdgesTable(SetOpTable):
    def _edges_op(self, query, frum):
        query = query.copy()  # WE WILL BE MARKING UP THE QUERY
        index_to_column = {}  # MAP FROM INDEX TO COLUMN (OR SELECT CLAUSE)
        outer_selects = []  # EVERY SELECT CLAUSE (NOT TO BE USED ON ALL TABLES, OF COURSE)
        frum_path = split_field(frum)
        base_table = join_field(frum_path[0:1])
        path = join_field(frum_path[1:])
        nest_to_alias = {
            nested_path: quote_column("__" + unichr(ord('a') + i) + "__")
            for i, (nested_path, sub_table) in enumerate(self.sf.tables.items())
        }

        schema = self.sf.tables[relative_field(frum, self.sf.fact)].schema

        tables = []
        for n, a in nest_to_alias.items():
            if startswith_field(path, n):
                tables.append({"nest": n, "alias": a})
        tables = jx.sort(tables, {"value": {"length": "nest"}})

        from_sql = quote_column(join_field([base_table] + split_field(tables[0].nest))) + tables[0].alias
        for previous, t in zip(tables, tables[1::]):
            from_sql += (
                SQL_LEFT_JOIN + quote_column(concat_field(base_table, t.nest)) + t.alias +
                SQL_ON + join_column(t.alias, quoted_PARENT) + " = " + join_column(previous.alias, quoted_UID)
            )

        main_filter = query.where.to_sql(schema, boolean=True)[0].sql.b

        # SHIFT THE COLUMN DEFINITIONS BASED ON THE NESTED QUERY DEPTH
        ons = []
        join_types = []
        wheres = []
        null_ons = [EXISTS_COLUMN + SQL_IS_NULL]
        groupby = []
        null_groupby = []
        orderby = []
        domains = []

        select_clause = [SQL_ONE + EXISTS_COLUMN] + [quote_column(c.es_column) for c in self.sf.tables['.'].columns]

        for edge_index, query_edge in enumerate(query.edges):
            edge_alias = quote_column("e" + text_type(edge_index))

            if query_edge.value:
                edge_values = [p for c in query_edge.value.to_sql(schema).sql for p in c.items()]

            elif not query_edge.value and any(query_edge.domain.partitions.where):
                case = SQL_CASE
                for pp, p in enumerate(query_edge.domain.partitions):
                    w = p.where.to_sql(schema)[0].sql.b
                    t = quote_value(pp)
                    case += SQL_WHEN + w + SQL_THEN + t
                case += SQL_ELSE + SQL_NULL + SQL_END  # quote value with length of partitions
                edge_values = [("n", case)]

            elif query_edge.range:
                edge_values = query_edge.range.min.to_sql(schema)[0].sql.items() + query_edge.range.max.to_sql(schema)[
                    0].sql.items()

            else:
                Log.error("Do not know how to handle")

            edge_names = []
            for column_index, (json_type, sql) in enumerate(edge_values):
                sql_name = quote_column("e" + text_type(edge_index) + "c" + text_type(column_index))
                edge_names.append(sql_name)

                num_sql_columns = len(index_to_column)
                if not query_edge.value and any(query_edge.domain.partitions.where):
                    def __(parts, num_sql_columns):
                        def _get(row):
                            return parts[row[num_sql_columns]].name

                        return _get

                    pull = __(query_edge.domain.partitions, num_sql_columns)
                else:
                    pull = get_column(num_sql_columns)

                if isinstance(query_edge.value, TupleOp):
                    query_edge.allowNulls = False
                    push_child = column_index
                    num_push_columns = len(query_edge.value.terms)
                else:
                    push_child = "."
                    num_push_columns = None

                index_to_column[num_sql_columns] = ColumnMapping(
                    is_edge=True,
                    push_name=query_edge.name,
                    push_column_name=query_edge.name,
                    push_column=edge_index,
                    num_push_columns=num_push_columns,
                    push_child=push_child,  # CAN NOT HANDLE TUPLES IN COLUMN
                    pull=pull,
                    sql=sql,
                    type=sql_type_to_json_type[json_type],
                    column_alias=sql_name
                )

            vals = [v for t, v in edge_values]
            if query_edge.domain.type == "set":
                domain_name = quote_column("d" + text_type(edge_index) + "c" + text_type(column_index))
                domain_names = [domain_name]
                if len(edge_names) > 1:
                    Log.error("Do not know how to handle")
                if query_edge.value:
                    domain = SQL_UNION_ALL.join(
                        SQL_SELECT +
                        sql_alias(quote_value(coalesce(p.dataIndex, i)), quote_column("rownum")) + SQL_COMMA +
                        sql_alias(quote_value(p.value), domain_name)
                        for i, p in enumerate(query_edge.domain.partitions)
                    )
                    if query_edge.allowNulls:
                        domain += (
                            SQL_UNION_ALL + SQL_SELECT +
                            sql_alias(quote_value(len(query_edge.domain.partitions)), quote_column("rownum")) + SQL_COMMA +
                            sql_alias(SQL_NULL, domain_name)
                        )
                    where = None
                    join_type = SQL_LEFT_JOIN if query_edge.allowNulls else SQL_INNER_JOIN
                    on_clause = (
                        SQL_OR.join(
                            join_column(edge_alias, k) + " = " + v
                            for k, v in zip(domain_names, vals)
                        ) +
                        SQL_OR + sql_iso(
                            join_column(edge_alias, domain_name) + SQL_IS_NULL + SQL_AND +
                            SQL_AND.join(v + SQL_IS_NULL for v in vals)
                        )
                    )
                    null_on_clause = None
                else:
                    domain = SQL_UNION_ALL.join(
                        SQL_SELECT + sql_alias(quote_value(pp), domain_name)
                        for pp, p in enumerate(query_edge.domain.partitions)
                    )
                    where = None
                    join_type = SQL_LEFT_JOIN if query_edge.allowNulls else SQL_INNER_JOIN
                    on_clause = SQL_AND.join(
                        join_column(edge_alias, k) + " = " + sql
                        for k, (t, sql) in zip(domain_names, edge_values)
                    )
                    null_on_clause = None
            elif query_edge.domain.type == "range":
                domain_name = quote_column("d" + text_type(edge_index) + "c0")
                domain_names = [domain_name]  # ONLY EVER SEEN ONE DOMAIN VALUE, DOMAIN TUPLES CERTAINLY EXIST
                d = query_edge.domain
                if d.max == None or d.min == None or d.min == d.max:
                    Log.error("Invalid range: {{range|json}}", range=d)
                if len(edge_names) == 1:
                    domain = self._make_range_domain(domain=d, column_name=domain_name)
                    limit = Math.min(query.limit, query_edge.domain.limit)
                    domain += (
                        SQL_ORDERBY + sql_list(vals) +
                        SQL_LIMIT + text_type(limit)
                    )

                    where = None
                    join_type = SQL_LEFT_JOIN if query_edge.allowNulls else SQL_INNER_JOIN
                    on_clause = SQL_AND.join(
                        join_column(edge_alias, k) + " <= " + v + SQL_AND +
                        v + " < (" + join_column(edge_alias, k) + " + " + text_type(
                            d.interval) + ")"
                        for k, (t, v) in zip(domain_names, edge_values)
                    )
                    null_on_clause = None
                elif query_edge.range:
                    query_edge.allowNulls = False
                    domain = self._make_range_domain(domain=d, column_name=domain_name)
                    limit = Math.min(query.limit, query_edge.domain.limit)
                    domain += (
                        SQL_ORDERBY + sql_list(vals) +
                        SQL_LIMIT + text_type(limit)
                    )
                    where = None
                    join_type = SQL_LEFT_JOIN if query_edge.allowNulls else SQL_INNER_JOIN
                    on_clause = (
                        join_column(edge_alias, domain_name) + " < " + edge_values[1][1] + SQL_AND +
                        edge_values[0][1] + " < " + sql_iso(join_column(edge_alias, domain_name) + " + " + text_type(d.interval))
                    )
                    null_on_clause = None
                else:
                    Log.error("do not know how to handle")
            elif len(edge_names) > 1:
                domain_names = [quote_column("d" + text_type(edge_index) + "c" + text_type(i)) for i, _ in enumerate(edge_names)]
                query_edge.allowNulls = False
                domain_columns = [c for c in self.sf.columns if quote_column(c.es_column) in vals]
                if not domain_columns:
                    domain_nested_path = "."
                    Log.note("expecting a known column")
                else:
                    domain_nested_path = domain_columns[0].nested_path
                domain_table = quote_column(concat_field(self.sf.fact, domain_nested_path[0]))
                limit = Math.min(query.limit, query_edge.domain.limit)
                domain = (
                    SQL_SELECT + sql_list(sql_alias(g, n) for n, g in zip(domain_names, vals)) +
                    SQL_FROM + domain_table + nest_to_alias["."] +
                    SQL_GROUPBY + sql_list(vals) +
                    SQL_ORDERBY + sql_count(SQL_ONE) + SQL_DESC +
                    SQL_LIMIT + text_type(limit)
                )
                where = None
                join_type = SQL_LEFT_JOIN if query_edge.allowNulls else SQL_INNER_JOIN
                on_clause = SQL_AND.join(
                    sql_iso(
                        sql_iso(
                            join_column(edge_alias, k) + SQL_IS_NULL + SQL_AND +
                            v + SQL_IS_NULL
                        ) + SQL_OR +
                        join_column(edge_alias, k) + " = " + v
                    )
                    for k, v in zip(domain_names, vals)
                )
                null_on_clause = None
            elif isinstance(query_edge.domain, DefaultDomain):
                domain_names = [quote_column("d" + text_type(edge_index) + "c" + text_type(i)) for i, _ in enumerate(edge_names)]
                domain_columns = [c for c in self.sf.columns if quote_column(c.es_column) in vals]
                if not domain_columns:
                    domain_nested_path = "."
                    Log.note("expecting a known column")
                else:
                    domain_nested_path = domain_columns[0].nested_path
                domain_table = quote_column(concat_field(self.sf.fact, domain_nested_path[0]))
                limit = Math.min(query.limit, query_edge.domain.limit)
                domain = (
                    SQL_SELECT + sql_list(sql_alias(g, n) for n, g in zip(domain_names, vals)) +
                    SQL_FROM + domain_table + " " + nest_to_alias["."] +
                    SQL_WHERE + SQL_AND.join(g + SQL_IS_NOT_NULL for g in vals) +
                    SQL_GROUPBY + sql_list(g for g in vals) +
                    SQL_ORDERBY + sql_list(sql_count(SQL_ONE) + SQL_DESC for _ in vals) +
                    SQL_LIMIT + quote_value(limit)
                )

                domain = (
                    SQL_SELECT + sql_list(domain_names) +
                    SQL_FROM + sql_iso(domain)
                )
                if query_edge.allowNulls:
                    domain += (
                        SQL_UNION_ALL +
                        SQL_SELECT + sql_list(sql_alias(SQL_NULL, n) for n in domain_names)
                    )

                where = None
                join_type = SQL_LEFT_JOIN if query_edge.allowNulls else SQL_INNER_JOIN
                on_clause = (
                    SQL_OR.join(  # "OR" IS FOR MATCHING DIFFERENT TYPES OF SAME NAME
                        join_column(edge_alias, k) + " = " + v
                        for k, v in zip(domain_names, vals)
                    ) +
                    SQL_OR + sql_iso(
                        join_column(edge_alias, domain_names[0]) + SQL_IS_NULL + SQL_AND +
                        SQL_AND.join(v + SQL_IS_NULL for v in vals)
                    )
                )
                null_on_clause = None

            elif isinstance(query_edge.domain, (DurationDomain, TimeDomain)):
                domain_name = quote_column("d" + text_type(edge_index) + "c0")
                domain_names = [domain_name]
                d = query_edge.domain
                if d.max == None or d.min == None or d.min == d.max:
                    Log.error("Invalid time domain: {{range|json}}", range=d)
                if len(edge_names) == 1:
                    domain = self._make_range_domain(domain=d, column_name=domain_name)
                    if query_edge.allowNulls:
                        domain += SQL_UNION_ALL + SQL_SELECT + sql_alias(SQL_NULL, domain_name)
                    on_clause = (
                        SQL_AND.join(
                            join_column(edge_alias, k) + " <= " + v + SQL_AND +
                            v + " < " + sql_iso(join_column(edge_alias, k) + " + " + quote_value(d.interval))
                            for k, (t, v) in zip(domain_names, edge_values)
                        ) + SQL_OR +
                        sql_iso(SQL_AND.join(
                            join_column(edge_alias, k) + SQL_IS_NULL + SQL_AND +
                            v + SQL_IS_NULL
                            for k, v in zip(domain_names, vals)
                        ))
                    )
                    where = None
                    join_type = SQL_LEFT_JOIN if query_edge.allowNulls else SQL_INNER_JOIN
                    if query_edge.allowNulls:
                        null_on_clause = None
                    else:
                        null_on_clause = SQL_AND.join(join_column(edge_alias, k) + SQL_IS_NOT_NULL for k in domain_names)
                elif query_edge.range:
                    domain = self._make_range_domain(domain=d, column_name=domain_name)
                    on_clause = (
                        join_column(edge_alias, domain_name) + " < " + edge_values[1][1] + SQL_AND +
                        edge_values[0][1] + " < " + sql_iso(join_column(edge_alias, domain_name) + " + " + quote_value(d.interval))
                    )
                else:
                    Log.error("do not know how to handle")
            else:
                Log.error("not handled")

            domains.append(domain)
            # null_domains.append(null_domain)
            ons.append(on_clause)
            wheres.append(where)
            join_types.append(join_type)
            if null_on_clause:
                null_ons.append(null_on_clause)

            groupby.append(sql_list(join_column(edge_alias, d) for d in domain_names))
            null_groupby.append(sql_list(join_column(edge_alias, d) for d in domain_names))

            for n, k in enumerate(domain_names):
                outer_selects.append(sql_alias(join_column(edge_alias, k), k))

                orderby.append(k + SQL_IS_NULL)
                if query.sort[n].sort == -1:
                    orderby.append(k + SQL_DESC)
                else:
                    orderby.append(k)

        offset = len(query.edges)
        for ssi, s in enumerate(listwrap(query.select)):
            si = ssi + offset
            if isinstance(s.value, Variable) and s.value.var == "." and s.aggregate == "count":
                # COUNT RECORDS, NOT ANY ONE VALUE
                sql = sql_alias(sql_count(EXISTS_COLUMN), quote_column(s.name))

                column_number = len(outer_selects)
                outer_selects.append(sql)
                index_to_column[column_number] = ColumnMapping(
                    push_name=s.name,
                    push_column_name=s.name,
                    push_column=si,
                    push_child=".",
                    pull=get_column(column_number),
                    sql=sql,
                    column_alias=quote_column(s.name),
                    type=sql_type_to_json_type["n"]
                )
            elif s.aggregate == "count" and (not query.edges and not query.groupby):
                value = s.value.var
                columns = [c.es_column for c in self.sf.columns if untyped_column(c.es_column)[0] == value]
                sql = SQL("+").join(sql_count(quote_column(col)) for col in columns)
                column_number = len(outer_selects)
                outer_selects.append(sql_alias(sql, _make_column_name(column_number)))
                index_to_column[column_number] = ColumnMapping(
                    push_name=s.name,
                    push_column_name=s.name,
                    push_column=si,
                    push_child=".",
                    pull=get_column(column_number),
                    sql=sql,
                    column_alias=_make_column_name(column_number),
                    type=sql_type_to_json_type["n"]
                )
            elif s.aggregate == "percentile":
                if not isinstance(s.percentile, (int, float)):
                    Log.error("Expecting percentile to be a float between 0 and 1")

                Log.error("not implemented")
            elif s.aggregate == "cardinality":
                for details in s.value.to_sql(schema):
                    for json_type, sql in details.sql.items():
                        column_number = len(outer_selects)
                        count_sql = sql_alias(sql_count("DISTINCT" + sql_iso(sql)), _make_column_name(column_number))
                        outer_selects.append(count_sql)
                        index_to_column[column_number] = ColumnMapping(
                            push_name=s.name,
                            push_column_name=s.name,
                            push_column=si,
                            push_child=".",
                            pull=get_column(column_number),
                            sql=count_sql,
                            column_alias=_make_column_name(column_number),
                            type=sql_type_to_json_type[json_type]
                        )
            elif s.aggregate == "union":
                for details in s.value.to_sql(schema):
                    for sql_type, sql in details.sql.items():
                        column_number = len(outer_selects)
                        outer_selects.append(sql_alias("JSON_GROUP_ARRAY(DISTINCT" + sql_iso(sql) + ")", _make_column_name(column_number)))
                        index_to_column[column_number] = ColumnMapping(
                            push_name=s.name,
                            push_column_name=s.name,
                            push_column=si,
                            push_child=".",  # join_field(split_field(details.name)[1::]),
                            pull=sql_text_array_to_set(column_number),
                            sql=sql,
                            column_alias=_make_column_name(column_number),
                            type=sql_type_to_json_type[sql_type]
                        )

            elif s.aggregate == "stats":  # THE STATS OBJECT
                for details in s.value.to_sql(schema):
                    sql = details.sql["n"]
                    for name, code in STATS.items():
                        full_sql = code.replace("{{value}}", sql)
                        column_number = len(outer_selects)
                        outer_selects.append(sql_alias(full_sql, _make_column_name(column_number)))
                        index_to_column[column_number] = ColumnMapping(
                            push_name=s.name,
                            push_column_name=s.name,
                            push_column=si,
                            push_child=name,
                            pull=get_column(column_number),
                            sql=full_sql,
                            column_alias=_make_column_name(column_number),
                            type="number"
                        )
            else:  # STANDARD AGGREGATES
                for details in s.value.partial_eval().to_sql(schema):
                    for sql_type, sql in details.sql.items():
                        column_number = len(outer_selects)
                        sql = sql_aggs[s.aggregate] + sql_iso(sql)
                        if s.default != None:
                            sql = sql_coalesce([sql, quote_value(s.default)])
                        outer_selects.append(sql_alias(sql, _make_column_name(column_number)))
                        index_to_column[column_number] = ColumnMapping(
                            push_name=s.name,
                            push_column_name=s.name,
                            push_column=si,
                            push_child=".",  # join_field(split_field(details.name)[1::]),
                            pull=get_column(column_number),
                            sql=sql,
                            column_alias=_make_column_name(column_number),
                            type=sql_type_to_json_type[sql_type]
                        )

        for w in query.window:
            outer_selects.append(self._window_op(self, query, w))

        all_parts = []

        primary = sql_iso(
            SQL_SELECT + sql_list(select_clause) +
            SQL_FROM + from_sql +
            SQL_WHERE + main_filter
        ) + nest_to_alias["."]

        edge_sql = []
        for edge_index, query_edge in enumerate(query.edges):
            edge_alias = quote_column("e" + text_type(edge_index))
            domain = domains[edge_index]
            edge_sql.append(sql_alias(sql_iso(domain), edge_alias))

        # COORDINATES OF ALL primary DATA
        part = (
            SQL_SELECT + sql_list(outer_selects) +
            SQL_FROM + primary
        )
        for t, s, j in zip(join_types, edge_sql, ons):
            part += " " + t + s + SQL_ON + j
        if any(wheres):
            part += SQL_WHERE + SQL_AND.join(sql_iso(w) for w in wheres if w)
        if groupby:
            part += SQL_GROUPBY + sql_list(groupby)
        all_parts.append(part)

        # ALL COORDINATES MISSED BY primary DATA
        if query.edges:
            part = SQL_SELECT + sql_list(outer_selects) + SQL_FROM + edge_sql[0]
            for s in edge_sql[1:]:
                part += SQL_LEFT_JOIN + s + SQL_ON + SQL_TRUE
            part += SQL_LEFT_JOIN + primary + SQL_ON + SQL_AND.join(sql_iso(o) for o in ons)
            part += SQL_WHERE + SQL_AND.join(sql_iso(w) for w in null_ons if w)
            if groupby:
                part += SQL_GROUPBY + sql_list(groupby)
            all_parts.append(part)

        command = SQL_SELECT + SQL_STAR + SQL_FROM + sql_iso(SQL_UNION_ALL.join(all_parts))

        if orderby:
            command += SQL_ORDERBY + sql_list(orderby)

        return command, index_to_column

    def _make_range_domain(self, domain, column_name):
        width = (domain.max - domain.min) / domain.interval
        digits = Math.floor(Math.log10(width - 1))
        if digits == 0:
            value = "a.value"
        else:
            value = SQL("+").join("1" + ("0" * j) + "*" + text_type(chr(ord(b'a') + j)) + ".value" for j in range(digits + 1))

        if domain.interval == 1:
            if domain.min == 0:
                domain = (
                    SQL_SELECT + value + column_name +
                    SQL_FROM + "__digits__ a"
                )
            else:
                domain = (
                    SQL_SELECT + sql_iso(value) + " + " + quote_value(domain.min) + column_name +
                    SQL_FROM + "__digits__ a"
                )
        else:
            if domain.min == 0:
                domain = (
                    SQL_SELECT + value + " * " + quote_value(domain.interval) +column_name +
                    SQL_FROM + "__digits__ a"
                )
            else:
                domain = (
                    SQL_SELECT + sql_iso(value + " * " + quote_value(domain.interval)) + " + " + quote_value(domain.min) + column_name +
                    SQL_FROM + "__digits__ a"
                )

        for j in range(digits):
            domain += SQL_INNER_JOIN + "__digits__" + text_type(chr(ord(b'a') + j + 1)) + " ON " +SQL_TRUE
        domain += SQL_WHERE + value + " < " + quote_value(width)
        return domain
