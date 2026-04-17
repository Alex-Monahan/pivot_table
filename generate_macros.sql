.mode list
.header off

.output macros.sql
from
    duckdb_functions ()
select
    'CREATE OR REPLACE MACRO ' || function_name || '(' || 
        list_reduce (parameters, (x, y) -> x || ',' || y) || 
    ')' || ' AS ' || 
    CASE
        WHEN function_type = 'table_macro' THEN ' TABLE '
        ELSE ''
    END || ' (' || macro_definition || ');' as create_macro
where 1=1
    and macro_definition is not null
    and function_name in (
        'nq',
        'sq',
        'dq',
        'nq_list',
        'sq_list',
        'dq_list',
        'nq_concat',
        'sq_concat',
        'dq_concat',
        'totals_list',
        'replace_zzz',
        'no_columns',
        'columns_values_axis_columns',
        'columns_values_axis_rows',
        'build_my_enum',
        'pivot_table',
        'pivot_table_show_sql'
    )

    --"no_columns","columns_values_axis_columns","columns_values_axis_rows","totals_list","replace_zzz"
    order by 
        CASE function_name
            when 'nq' then 1
            when 'sq' then 1
            when 'dq' then 1
            when 'nq_list' then 2
            when 'sq_list' then 2
            when 'dq_list' then 2
            when 'nq_concat' then 3
            when 'sq_concat' then 3
            when 'dq_concat' then 3
            when 'replace_zzz' then 4
            when 'totals_list' then 5
            when 'columns_values_axis_rows' then 6
            when 'columns_values_axis_columns' then 7
            when 'no_columns' then 8
            when 'build_my_enum' then 9
            when 'pivot_table' then 10
            when 'pivot_table_show_sql' then 11
        end
    ;