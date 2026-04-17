CREATE OR REPLACE MACRO pivot_table_functions.main.dq(my_varchar) AS  ((('"' || "replace"(my_varchar, '"', '""')) || '"'));
CREATE OR REPLACE MACRO pivot_table_functions.main.nq(my_varchar) AS  ("replace"(my_varchar, ';', 'No semicolons are permitted here'));
CREATE OR REPLACE MACRO pivot_table_functions.main.sq(my_varchar) AS  ((('''' || "replace"(my_varchar, '''', '''''')) || ''''));
CREATE OR REPLACE MACRO pivot_table_functions.main.nq_list(my_list) AS  (list_transform(my_list, (i -> pivot_table_functions.main.nq(i))));
CREATE OR REPLACE MACRO pivot_table_functions.main.sq_list(my_list) AS  (list_transform(my_list, (i -> pivot_table_functions.main.sq(i))));
CREATE OR REPLACE MACRO pivot_table_functions.main.dq_list(my_list) AS  (list_transform(my_list, (i -> pivot_table_functions.main.dq(i))));
CREATE OR REPLACE MACRO pivot_table_functions.main.dq_concat(my_list,separator) AS  (list_reduce(pivot_table_functions.main.dq_list(my_list), (main."row"(x, y) -> ((x || separator) || y))));
CREATE OR REPLACE MACRO pivot_table_functions.main.nq_concat(my_list,separator) AS  (CASE  WHEN ((length(my_list) = 0)) THEN (NULL) ELSE list_reduce(pivot_table_functions.main.nq_list(my_list), (main."row"(x, y) -> ((x || separator) || y))) END);
CREATE OR REPLACE MACRO pivot_table_functions.main.sq_concat(my_list,separator) AS  (list_reduce(pivot_table_functions.main.sq_list(my_list), (main."row"(x, y) -> ((x || separator) || y))));
CREATE OR REPLACE MACRO pivot_table_functions.main.replace_zzz(rows,extra_cols) AS  ((((((((('SELECT 
            replace(
                replace(
                    COLUMNS(c -> list_contains([' || pivot_table_functions.main.sq_concat("rows", ', ')) || ', ') || pivot_table_functions.main.sq_concat(extra_cols, ', ')) || '], c))::varchar,
                    ''zzzSubtotal'',
                    ''Subtotal''
                    ),
                ''zzzGrand Total'',
                ''Grand Total''),
            columns(c -> NOT list_contains([') || pivot_table_functions.main.sq_concat("rows", ', ')) || '], c) AND c NOT IN (') || pivot_table_functions.main.sq_concat(extra_cols, ', ')) || '))
        '));
CREATE OR REPLACE MACRO pivot_table_functions.main.totals_list(rows,subtotals,grand_totals) AS  (main.list_apply("range"(CASE  WHEN (subtotals) THEN (0) ELSE (length("rows") - 1) END, CASE  WHEN (grand_totals) THEN (length("rows")) ELSE (length("rows") - 1) END), (lambda i: CASE  WHEN ((i = (length("rows") - 1))) THEN (pivot_table_functions.main.nq_concat(list_transform("rows"[:-((i + 1)):-1], (j -> ('''zzzGrand Total'' as ' || pivot_table_functions.main.dq(j)))), ', ')) ELSE pivot_table_functions.main.nq_concat(list_transform("rows"[:-((i + 1)):-1], (j -> ('''zzzSubtotal'' as ' || pivot_table_functions.main.dq(j)))), ', ') END)));
CREATE OR REPLACE MACRO pivot_table_functions.main.columns_values_axis_rows(table_names,"values",rows,columns,column_names,filters,values_axis,subtotals,grand_totals) AS  (((('WITH raw_pivot AS ( ' || pivot_table_functions.main.nq_concat(list_transform("values", (i -> (((((((('
                    FROM (
                        PIVOT (
                            ' || CASE  WHEN (((subtotals OR grand_totals) AND (length("rows") > 0))) THEN (pivot_table_functions.main.nq_concat((main.list_value(((((('FROM query(' || pivot_table_functions.sq(pivot_table_functions.main.nq_concat(list_transform(table_names, (table_name -> CASE WHEN table_name NOT ILIKE '%from%' THEN 'FROM query_table('||pivot_table_functions.main.dq(table_name)||')' ELSE pivot_table_functions.main.nq(table_name) END)), ' UNION ALL BY NAME '))) || ') 
                                    SELECT *, 1 as dummy_column, ') || pivot_table_functions.main.sq(i)) || ' AS value_names 

                                    -- FILTERS
                                    ') || COALESCE(('WHERE 1=1 AND ' || pivot_table_functions.main.nq_concat(filters, ' AND ')), ''))) || list_transform(pivot_table_functions.main.totals_list("rows", subtotals := subtotals, grand_totals := grand_totals), (k -> ((((((('FROM query(' || pivot_table_functions.sq(pivot_table_functions.main.nq_concat(list_transform(table_names, (table_name -> CASE WHEN table_name NOT ILIKE '%from%' THEN 'FROM query_table('||pivot_table_functions.main.dq(table_name)||')' ELSE pivot_table_functions.main.nq(table_name) END)), ' UNION ALL BY NAME '))) || ') 
                                        SELECT * replace(') || k) || '), 1 as dummy_column, ') || pivot_table_functions.main.sq(i)) || ' AS value_names 

                                        -- FILTERS
                                        ') || COALESCE(('WHERE 1=1 AND ' || pivot_table_functions.main.nq_concat(filters, ' AND ')), ''))))), ' 
                                    UNION ALL BY NAME 
                                    ')) ELSE ((((('
                                FROM query(' || pivot_table_functions.sq(pivot_table_functions.main.nq_concat(list_transform(table_names, (table_name -> CASE WHEN table_name NOT ILIKE '%from%' THEN 'FROM query_table('||pivot_table_functions.main.dq(table_name)||')' ELSE pivot_table_functions.main.nq(table_name) END)), ' UNION ALL BY NAME '))) || ') 
                                SELECT *, 1 as dummy_column, ') || pivot_table_functions.main.sq(i)) || ' AS value_names 

                                -- FILTERS
                                ') || COALESCE(('WHERE 1=1 AND ' || pivot_table_functions.main.nq_concat(filters, ' AND ')), '')) END) || '
                        )
                        -- COLUMNS
                        -- When pivoting, do not use all combinations of values in the columns parameter,
                        -- only use the combinations that actually exist in the data. 
                        -- This is achieved by only pivoting ON one expression (that has all columns concatenated together)
                        ON ') || pivot_table_functions.main.dq_concat("columns", ' || ''_'' || ')) || ' IN (' || pivot_table_functions.main.sq_concat(column_names, ', ') || ')
                        
                        -- VALUES
                        -- Each PIVOT will use a single value metric
                        USING ') || pivot_table_functions.main.nq(i)) || '

                        -- ROWS
                        GROUP BY dummy_column') || COALESCE((', ' || pivot_table_functions.main.dq_concat("rows", ', ')), '')) || ', value_names 
                    ) 
                    '))), ' UNION ALL BY NAME ')) || '
        ), ordered_pivot AS (FROM raw_pivot ORDER BY ALL NULLS FIRST LIMIT 10000000000)
        FROM ordered_pivot 
        ') || CASE  WHEN (((subtotals OR grand_totals) AND (length("rows") > 0))) THEN (pivot_table_functions.main.replace_zzz("rows", main.list_value('dummy_column', 'value_names'))) ELSE '' END));
CREATE OR REPLACE MACRO pivot_table_functions.main.columns_values_axis_columns(table_names,"values",rows,columns,column_names,filters,values_axis,subtotals,grand_totals) AS  (((((((((('WITH raw_pivot AS (
            PIVOT (
                ' || CASE  WHEN (((subtotals OR grand_totals) AND (length("rows") > 0))) THEN (pivot_table_functions.main.nq_concat((main.list_value(((('FROM query(' || pivot_table_functions.sq(pivot_table_functions.main.nq_concat(list_transform(table_names, (table_name -> CASE WHEN table_name NOT ILIKE '%from%' THEN 'FROM query_table('||pivot_table_functions.main.dq(table_name)||')' ELSE pivot_table_functions.main.nq(table_name) END)), ' UNION ALL BY NAME '))) || ') 
                        SELECT *, 1 as dummy_column
                        
                        -- FILTERS
                        ') || COALESCE(('WHERE 1=1 AND ' || pivot_table_functions.main.nq_concat(filters, ' AND ')), ''))) || list_transform(pivot_table_functions.main.totals_list("rows", subtotals := subtotals, grand_totals := grand_totals), (k -> ((((('FROM query(' || pivot_table_functions.sq(pivot_table_functions.main.nq_concat(list_transform(table_names, (table_name -> CASE WHEN table_name NOT ILIKE '%from%' THEN 'FROM query_table('||pivot_table_functions.main.dq(table_name)||')' ELSE pivot_table_functions.main.nq(table_name) END)), ' UNION ALL BY NAME '))) || ') 
                            SELECT * replace(') || k) || '), 1 as dummy_column

                            -- FILTERS
                            ') || COALESCE(('WHERE 1=1 AND ' || pivot_table_functions.main.nq_concat(filters, ' AND ')), ''))))), ' 
                        UNION ALL BY NAME 
                        ')) ELSE ((('
                    FROM query(' || pivot_table_functions.sq(pivot_table_functions.main.nq_concat(list_transform(table_names, (table_name -> CASE WHEN table_name NOT ILIKE '%from%' THEN 'FROM query_table('||pivot_table_functions.main.dq(table_name)||')' ELSE pivot_table_functions.main.nq(table_name) END)), ' UNION ALL BY NAME '))) || ') 
                    SELECT *, 1 as dummy_column

                    -- FILTERS
                    ') || COALESCE(('WHERE 1=1 AND ' || pivot_table_functions.main.nq_concat(filters, ' AND ')), '')) END) || '
            )
            -- COLUMNS 
            -- When pivoting, do not use all combinations of values in the columns parameter,
            -- only use the combinations that actually exist in the data. 
            -- This is achieved by only pivoting ON one expression (that has all columns concatenated together)
            ON ') || pivot_table_functions.main.dq_concat("columns", ' || ''_'' || ')) || ' IN (' || pivot_table_functions.main.sq_concat(column_names, ', ') || ')

            -- VALUES
            -- If values are passed in, use one or more values as summary metrics
            ') || COALESCE(('USING ' || pivot_table_functions.main.nq_concat("values", ', ')), '')) || '

            -- ROWS
            GROUP BY dummy_column') || COALESCE((', ' || pivot_table_functions.main.dq_concat("rows", ', ')), '')) || ' 
            ORDER BY ALL NULLS FIRST LIMIT 10000000000
        ) FROM raw_pivot 
        ') || CASE  WHEN (((subtotals OR grand_totals) AND (length("rows") > 0))) THEN (pivot_table_functions.main.replace_zzz("rows", main.list_value('dummy_column'))) ELSE '' END));
CREATE OR REPLACE MACRO pivot_table_functions.main.no_columns(table_names,"values",rows,filters,values_axis,subtotals,grand_totals) AS  (((((((((((((((((('FROM query(' || pivot_table_functions.sq(pivot_table_functions.main.nq_concat(list_transform(table_names, (table_name -> CASE WHEN table_name NOT ILIKE '%from%' THEN 'FROM query_table('||pivot_table_functions.main.dq(table_name)||')' ELSE pivot_table_functions.main.nq(table_name) END)), ' UNION ALL BY NAME '))) || ') 
        SELECT 
            -- ROWS 
            -- Select a dummy column and all columns in the rows parameter
            1 as dummy_column,
            
            -- If using subtotals or grand_totals, detect which rows are subtotals and/or grand_totals
            -- using the GROUPING function, since in these cases GROUPING SETS are in use.
            -- Then replace what would have been a NULL with the text Grand Total or Subtotal.
            ') || CASE  WHEN (((subtotals OR grand_totals) AND (length("rows") > 0))) THEN ((pivot_table_functions.main.nq_concat(list_transform("rows", (r -> ((((((((('case when list_aggregate([' || pivot_table_functions.main.nq_concat(list_transform("rows", (i -> (('GROUPING(' || pivot_table_functions.main.dq(i)) || ')'))), ', ')) || '],
                        ''sum'') = ') || length("rows")) || ' then ''Grand Total''
                    when GROUPING(') || pivot_table_functions.main.dq(r)) || ') = 1 then ''Subtotal'' 
                    else ') || pivot_table_functions.main.dq(r)) || '::varchar 
                    end as ') || pivot_table_functions.main.dq(r)))), ', ') || ', ')) ELSE COALESCE((pivot_table_functions.main.dq_concat("rows", ', ') || ','), '') END) || '
            
            -- VALUES 
            -- If values_axis is columns, then just have a separate column for each value
            -- If values_axis is rows, unnest so that there is a separate row for each value
            ') || CASE  WHEN (((values_axis != 'rows') OR (length("values") = 0))) THEN ('') ELSE ((' UNNEST([' || pivot_table_functions.main.sq_concat("values", ', ')) || ']) AS value_names, 
                    UNNEST([') END) || '
                    ') || COALESCE((pivot_table_functions.main.nq_concat("values", ', ') || ' '), '')) || '
            ') || CASE  WHEN (((values_axis != 'rows') OR (length("values") = 0))) THEN ('') ELSE ']) AS "values" ' END) || '
        
        -- FILTERS 
        -- Filter the data if requested. The WHERE clause is entirely removed if filters is an empty list.
        ') || COALESCE(('WHERE 1=1 AND ' || pivot_table_functions.main.nq_concat(filters, ' AND ')), '')) || '
        
        -- If using subtotals, use a ROLLUP 
        -- (note this will include a grand_total, which is filtered out with a HAVING clause if grand_totals=0)
        -- If using grand totals and not subtotals, use GROUPING SETS to add just a total
        -- If no subtotals or grand totals, just GROUP BY ALL.
        GROUP BY ') || CASE  WHEN ((subtotals AND (length("rows") > 0))) THEN ((('ROLLUP (' || pivot_table_functions.main.dq_concat("rows", ', ')) || ') ')) WHEN ((grand_totals AND (length("rows") > 0) AND (NOT subtotals))) THEN ((('GROUPING SETS ((), (' || pivot_table_functions.main.dq_concat("rows", ', ')) || '))')) ELSE 'ALL ' END) || ' 
        
        -- If subtotals were requested, but not grand_totals, filter out the grand_totals row
        ') || CASE  WHEN (((NOT grand_totals) AND subtotals AND (length("rows") > 0))) THEN (((('HAVING 
        list_aggregate([' || pivot_table_functions.main.nq_concat(list_transform("rows", (i -> (('GROUPING(' || pivot_table_functions.main.dq(i)) || ')'))), ', ')) || '],
                        ''sum'') != ') || length("rows"))) ELSE '' END) || '
        
        -- If using subtotals or grand_totals, ensure the subtotal/grand_total rows are sorted below non-total values.
        -- If not, just ORDER BY ALL NULLS FIRST
        ORDER BY ') || CASE  WHEN (((subtotals OR grand_totals) AND (length("rows") > 0))) THEN (((pivot_table_functions.main.nq_concat(list_transform("rows", (i -> ((('GROUPING(' || pivot_table_functions.main.dq(i)) || '), ') || pivot_table_functions.main.dq(i)))), ', ') || '
                -- If we have values_axis of rows, we need to include the value_names column to maintain deterministic ordering
                ') || CASE  WHEN (((values_axis = 'rows') AND (length("values") > 0))) THEN (', value_names ') ELSE ' ' END)) ELSE 'ALL NULLS FIRST ' END));
CREATE OR REPLACE MACRO build_column_names(table_names,columns,filters) AS  (SELECT list(#1) FROM query((((((('
        FROM query(' || pivot_table_functions.sq(pivot_table_functions.main.nq_concat(list_transform(table_names, (table_name -> CASE WHEN table_name NOT ILIKE '%from%' THEN 'FROM query_table('||pivot_table_functions.main.dq(table_name)||')' ELSE pivot_table_functions.main.nq(table_name) END)), ' UNION ALL BY NAME '))) || ') 
        SELECT DISTINCT
            -- When pivoting, do not use all combinations of values in the columns parameter,
            -- only use the combinations that actually exist in the data. 
            -- This is achieved by only pivoting ON one expression (that has all columns concatenated together).
            -- Therefore, we concatenate everything together here with an _ separator.
            ') || COALESCE((pivot_table_functions.main.nq_concat(list_transform(pivot_table_functions.main.dq_list("columns"), (i -> (('coalesce(' || i) || '::varchar , ''NULL'')'))), ' || ''_'' || ') || ''), '1')) || '
        ') || COALESCE(('WHERE 1=1 AND ' || pivot_table_functions.main.nq_concat(filters, ' AND ')), '')) || '
        ORDER BY ALL
        ')));
CREATE OR REPLACE MACRO pivot_table_functions.main.pivot_table(table_names,"values",rows,columns,column_names,filters,values_axis,subtotals,grand_totals) AS  TABLE  (SELECT * EXCLUDE (dummy_column) FROM query(CASE  WHEN ((length("columns") = 0)) THEN (pivot_table_functions.main.no_columns(table_names, "values", "rows", filters, values_axis := values_axis, subtotals := subtotals, grand_totals := grand_totals)) WHEN (((values_axis = 'columns') OR (length("values") = 0))) THEN (pivot_table_functions.main.columns_values_axis_columns(table_names, "values", "rows", "columns", column_names, filters, values_axis := 'columns', subtotals := subtotals, grand_totals := grand_totals)) WHEN ((values_axis = 'rows')) THEN (pivot_table_functions.main.columns_values_axis_rows(table_names, "values", "rows", "columns", column_names, filters, values_axis := 'rows', subtotals := subtotals, grand_totals := grand_totals)) ELSE NULL END));
CREATE OR REPLACE MACRO pivot_table_functions.main.pivot_table_show_sql(table_names,"values",rows,columns,column_names,filters,values_axis,subtotals,grand_totals) AS  TABLE  (SELECT CASE  WHEN ((length("columns") = 0)) THEN (pivot_table_functions.main.no_columns(table_names, "values", "rows", filters, values_axis := values_axis, subtotals := subtotals, grand_totals := grand_totals)) WHEN (((values_axis = 'columns') OR (length("values") = 0))) THEN (pivot_table_functions.main.columns_values_axis_columns(table_names, "values", "rows", "columns", column_names, filters, values_axis := 'columns', subtotals := subtotals, grand_totals := grand_totals)) WHEN ((values_axis = 'rows')) THEN (pivot_table_functions.main.columns_values_axis_rows(table_names, "values", "rows", "columns", column_names, filters, values_axis := 'rows', subtotals := subtotals, grand_totals := grand_totals)) ELSE NULL END AS sql_string);
