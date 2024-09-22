#define DUCKDB_EXTENSION_MAIN

#include "pivot_table_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

#include "duckdb/catalog/default/default_functions.hpp"
#include "duckdb/catalog/default/default_table_functions.hpp"

namespace duckdb {


// To add a new scalar SQL macro, add a new macro to this array!
// Copy and paste the top item in the array into the 
// second-to-last position and make some modifications. 
// (essentially, leave the last entry in the array as {nullptr, nullptr, {nullptr}, {{nullptr, nullptr}}, nullptr} )

// Keep the DEFAULT_SCHEMA (no change needed)
// Replace "times_two" with a name for your macro
// If your function has parameters, add their names in quotes inside of the {}, with a nullptr at the end
//      If you do not have parameters, simplify to {nullptr}
// Add the text of your SQL macro as a raw string with the format R"( select 42 )"
static DefaultMacro dynamic_sql_examples_macros[] = {
    // nq = no quotes
    // sq = single quotes
    // dq = double quotes
    {DEFAULT_SCHEMA, "nq", {"my_varchar", nullptr}, {{nullptr, nullptr}}, R"( 
        -- We do not want to allow semicolons because we do not want to allow multiple statements to be run.
        -- This combines with the query function's boundaries of 
        -- only running a single statement and only running read queries only
        -- to protect against unwanted execution
        replace(my_varchar, ';', 'No semicolons are permitted here')
    )"},
    {DEFAULT_SCHEMA, "sq", {"my_varchar", nullptr}, {{nullptr, nullptr}}, R"( ''''||replace(my_varchar,'''', '''''')||'''' )"},
    {DEFAULT_SCHEMA, "dq", {"my_varchar", nullptr}, {{nullptr, nullptr}}, R"( '"'||replace(my_varchar,'"', '""')||'"' )"},
    {DEFAULT_SCHEMA, "nq_list", {"my_list", nullptr}, {{nullptr, nullptr}}, R"( list_transform(my_list, (i) -> nq(i)) )"},
    {DEFAULT_SCHEMA, "sq_list", {"my_list", nullptr}, {{nullptr, nullptr}}, R"( list_transform(my_list, (i) -> sq(i)) )"},
    {DEFAULT_SCHEMA, "dq_list", {"my_list", nullptr}, {{nullptr, nullptr}}, R"( list_transform(my_list, (i) -> dq(i)) )"},
    {DEFAULT_SCHEMA, "nq_concat", {"my_list", "separator", nullptr}, {{nullptr, nullptr}}, R"( 
        -- We want to tolerate cases where a list is blank and use it to remove entire clauses
        -- (Ex: if there are no filters, there should be no where clause at all)
        CASE WHEN length(my_list) = 0 THEN NULL
        ELSE list_reduce(nq_list(my_list), (x, y) -> x || separator || y)
        END
    )"},
    {DEFAULT_SCHEMA, "sq_concat", {"my_list", "separator", nullptr}, {{nullptr, nullptr}}, R"( list_reduce(sq_list(my_list), (x, y) -> x || separator || y) )"},
    {DEFAULT_SCHEMA, "dq_concat", {"my_list", "separator", nullptr}, {{nullptr, nullptr}}, R"( list_reduce(dq_list(my_list), (x, y) -> x || separator || y) )"},
    {DEFAULT_SCHEMA, "totals_list", {"rows", nullptr}, {{"subtotals", "1"}, {"grand_totals", "1"}, {nullptr, nullptr}}, R"( 
    -- Return a list of expressions that will be used in a "SELECT * REPLACE(" clause
    -- in order to enable subtotals and/or grand totals.
    -- This will be used to hardcode all values within specific columns into a single string
    -- so that when an aggregation is applied, it aggregates across the subtotal or grand_total level of granularity
    -- An example output would be: ['zzzSubtotal' as "subcat", 'zzzGrand Total' as "subcat", 'zzzGrand Total' as "category"]
    -- The zzz's are used to force the subtotals and grand totals to be placed at the bottom of the raw data when sorting
    [
        CASE WHEN i = length(rows) - 1 THEN 
            nq_concat(
                list_transform(
                    rows[:-(i+1):-1],
                    (j) -> '''zzzGrand Total'' as ' || dq(j)
                ),
                ', '
            )
        ELSE 
            nq_concat(
                list_transform(
                    rows[:-(i+1):-1],
                    (j) -> '''zzzSubtotal'' as ' || dq(j)
                ),
                ', '
            )
        END
        for i in range(
            CASE WHEN subtotals THEN 0 ELSE length(rows) - 1 END, /* If no subtotals, only do the all-columns case  */
            CASE WHEN grand_totals THEN length(rows) ELSE length(rows) - 1 END) /* If no grand_totals, we don't do all rows, we do rows-1 (there is no subtotal on first "row" parameter element)*/
    ]
    )"},
    {DEFAULT_SCHEMA, "replace_zzz", {"rows", "extra_cols", nullptr}, {{nullptr, nullptr}}, R"( 
        -- After sorting, remove the zzz's that forced subtotals and grand totals to the bottom
        'SELECT 
            replace(
                replace(
                    COLUMNS(c -> list_contains(['|| sq_concat(rows, ', ') ||', ' || 
                        sq_concat(extra_cols, ', ')|| '], c))::varchar,
                    ''zzzSubtotal'',
                    ''Subtotal''
                    ),
                ''zzzGrand Total'',
                ''Grand Total''),
            columns(c -> NOT list_contains(['||sq_concat(rows, ', ')||'], c) AND c NOT IN (' || 
                sq_concat(extra_cols, ', ') ||'))
        '
    )"},
    {DEFAULT_SCHEMA, "no_columns", {"table_names", "values", "rows", "filters", nullptr}, {{"values_axis", "'columns'"}, {"subtotals", "0"}, {"grand_totals", "0"}, {nullptr, nullptr}}, R"( 
        -- If no columns are being pivoted horizontally (columns parameter is an empty list), 
        -- use a group by operation to create the output table. 
        'FROM query_table(['||dq_concat(table_names, ', ')||']) 
        SELECT 
            -- ROWS 
            -- Select a dummy column and all columns in the rows parameter
            1 as dummy_column,
            
            -- If using subtotals or grand_totals, detect which rows are subtotals and/or grand_totals
            -- using the GROUPING function, since in these cases GROUPING SETS are in use.
            -- Then replace what would have been a NULL with the text Grand Total or Subtotal.
            '||CASE WHEN (subtotals OR grand_totals) AND length(rows) > 0 THEN 
                nq_concat(list_transform(rows, (r) -> 'case when list_aggregate(['||nq_concat(
                        list_transform(
                            rows,
                            (i) -> 'GROUPING('||dq(i)||')'),
                            ', ') ||'],
                        ''sum'') = '||length(rows)||' then ''Grand Total''
                    when GROUPING('||dq(r)||') = 1 then ''Subtotal'' 
                    else '||dq(r)||'::varchar 
                    end as '||dq(r)),
                    ', ')||', '
                ELSE coalesce(dq_concat(rows, ', ')||',', '') 
                END ||'
            
            -- VALUES 
            -- If values_axis is columns, then just have a separate column for each value
            -- If values_axis is rows, unnest so that there is a separate row for each value
            '||CASE WHEN values_axis != 'rows' OR length(values) = 0 THEN '' 
                ELSE ' UNNEST(['||sq_concat(values, ', ')||']) AS value_names, 
                    UNNEST([' END||'
                    '||coalesce(nq_concat(values, ', ')||' ', '') ||'
            '||CASE WHEN values_axis != 'rows' OR length(values) = 0 THEN '' ELSE ']) AS values ' END||'
        
        -- FILTERS 
        -- Filter the data if requested. The WHERE clause is entirely removed if filters is an empty list.
        '|| coalesce('WHERE 1=1 AND ' || nq_concat(filters, ' AND '), '') ||'
        
        -- If using subtotals, use a ROLLUP 
        -- (note this will include a grand_total, which is filtered out with a HAVING clause if grand_totals=0)
        -- If using grand totals and not subtotals, use GROUPING SETS to add just a total
        -- If no subtotals or grand totals, just GROUP BY ALL.
        GROUP BY ' || 
            CASE WHEN subtotals AND length(rows) > 0 THEN 'ROLLUP ('|| dq_concat(rows, ', ') ||') ' 
            WHEN grand_totals AND length(rows) > 0  AND NOT subtotals THEN 'GROUPING SETS ((), ('|| dq_concat(rows, ', ') ||'))'
            ELSE 'ALL ' 
            END ||' 
        
        -- If subtotals were requested, but not grand_totals, filter out the grand_totals row
        ' ||CASE WHEN NOT grand_totals AND subtotals AND length(rows) > 0 THEN 'HAVING 
        list_aggregate(['||nq_concat(
                        list_transform(
                            rows,
                            (i) -> 'GROUPING('||dq(i)||')'),
                            ', ') ||'],
                        ''sum'') != '||length(rows) ELSE '' END|| '
        
        -- If using subtotals or grand_totals, ensure the subtotal/grand_total rows are sorted below non-total values.
        -- If not, just ORDER BY ALL NULLS FIRST
        ORDER BY ' || 
            CASE WHEN (subtotals OR grand_totals) AND length(rows) > 0 THEN 
                nq_concat(
                    list_transform(
                        rows,
                        (i) -> 'GROUPING('||dq(i)||'), '||dq(i)),
                    ', ') || '
                -- If we have values_axis of rows, we need to include the value_names column to maintain deterministic ordering
                ' ||CASE WHEN values_axis = 'rows' AND length(values) > 0 THEN ', value_names ' ELSE ' ' END
            ELSE 'ALL NULLS FIRST ' 
            END 
    )"},
    {DEFAULT_SCHEMA, "columns_values_axis_columns", {"table_names", "values", "rows", "columns", "filters", nullptr}, {{"values_axis", "'columns'"}, {"subtotals", "0"}, {"grand_totals", "0"}, {nullptr, nullptr}}, R"( 
        -- If columns are being pivoted outward (the columns parameter is in use), and the values_axis is columns, use a PIVOT statement.
        -- The PIVOT is wrapped in a CTE so that subtotal/grand_total indicators can be renamed to friendly names (without zzz)
        -- after having been sorted correctly.
        'WITH raw_pivot AS (
            PIVOT (
                '||
                -- If using subtotals or grand_totals, add in extra copies of the raw data,
                -- but with some or all column values replaced with static strings (Ex: zzzSubtotal)
                -- so that the aggregations are at the subtotal or grand_total level instead of at the original level of granularity.
                CASE WHEN (subtotals OR grand_totals) AND length(rows) > 0 THEN 
                    nq_concat(
                        ['FROM query_table(['||dq_concat(table_names, ', ')||']) 
                        SELECT *, 1 as dummy_column
                        
                        -- FILTERS
                        '|| coalesce('WHERE 1=1 AND ' || nq_concat(filters, ' AND '), '')] || 
                        list_transform(
                            totals_list(rows, subtotals:=subtotals, grand_totals:=grand_totals),
                            k -> 
                            'FROM query_table(['||dq_concat(table_names, ', ')||']) 
                            SELECT * replace(' || k || '), 1 as dummy_column

                            -- FILTERS
                            '|| coalesce('WHERE 1=1 AND ' || nq_concat(filters, ' AND '), '')
                        ),
                        ' 
                        UNION ALL BY NAME 
                        ' 
                    )
                ELSE '
                    FROM query_table(['||dq_concat(table_names, ', ')||']) 
                    SELECT *, 1 as dummy_column

                    -- FILTERS
                    '|| coalesce('WHERE 1=1 AND ' || nq_concat(filters, ' AND '), '')
                END ||'
            )
            -- COLUMNS 
            -- When pivoting, do not use all combinations of values in the columns parameter,
            -- only use the combinations that actually exist in the data. 
            -- This is achieved by only pivoting ON one expression (that has all columns concatenated together)
            ON '||dq_concat(columns, ' || ''_'' || ')||' IN columns_parameter_enum

            -- VALUES
            -- If values are passed in, use one or more values as summary metrics
            '|| coalesce('USING '||nq_concat(values, ', '), '')||'

            -- ROWS
            GROUP BY dummy_column'||coalesce(', '||dq_concat(rows, ', '),'') || ' 
            ORDER BY ALL NULLS FIRST LIMIT 10000000000
        ) FROM raw_pivot 
        '|| CASE WHEN (subtotals OR grand_totals) AND length(rows) > 0 THEN 
            replace_zzz(rows, ['dummy_column'])
        ELSE ''
        END
    )"},
    {DEFAULT_SCHEMA, "columns_values_axis_rows", {"table_names", "values", "rows", "columns", "filters", nullptr}, {{"values_axis", "'rows'"}, {"subtotals", "0"}, {"grand_totals", "0"}, {nullptr, nullptr}}, R"( 
        -- If columns are being pivoted outward (the columns parameter is in use), and the values_axis is rows,
        -- use one PIVOT statement per value and stack them using UNION ALL BY NAME.
        -- The stack of PIVOTs is wrapped in a CTE so that subtotal/grand_total indicators can be renamed to friendly names 
        -- (without zzz) after having been sorted correctly.
        'WITH raw_pivot AS ( '||
            nq_concat(
                -- For each value, use a PIVOT statement, then stack each value together with UNION ALL BY NAME
                list_transform(values, (i) -> 
                    '
                    FROM (
                        PIVOT (
                            '||
                            -- If using subtotals or grand_totals, add in extra copies of the raw data,
                            -- but with some or all column values replaced with static strings (Ex: zzzSubtotal)
                            -- so that the aggregations are at the subtotal or grand_total level instead of at the original level of granularity.
                            CASE WHEN (subtotals OR grand_totals) AND length(rows) > 0 THEN 
                                nq_concat(
                                    ['FROM query_table(['||dq_concat(table_names, ', ')||']) 
                                    SELECT *, 1 as dummy_column, '|| sq(i)||' AS value_names 

                                    -- FILTERS
                                    '|| coalesce('WHERE 1=1 AND ' || nq_concat(filters, ' AND '), '')] || 
                                    list_transform(
                                        totals_list(rows, subtotals:=subtotals, grand_totals:=grand_totals),
                                        k -> 
                                        'FROM query_table(['||dq_concat(table_names, ', ')||']) 
                                        SELECT * replace(' || k || '), 1 as dummy_column, '|| sq(i) ||' AS value_names 

                                        -- FILTERS
                                        '|| coalesce('WHERE 1=1 AND ' || nq_concat(filters, ' AND '), '')
                                    ),
                                    ' 
                                    UNION ALL BY NAME 
                                    '
                                )
                            ELSE '
                                FROM query_table(['||dq_concat(table_names, ', ')||']) 
                                SELECT *, 1 as dummy_column, '|| sq(i) ||' AS value_names 

                                -- FILTERS
                                '|| coalesce('WHERE 1=1 AND ' || nq_concat(filters, ' AND '), '')
                            END ||'
                        )
                        -- COLUMNS
                        -- When pivoting, do not use all combinations of values in the columns parameter,
                        -- only use the combinations that actually exist in the data. 
                        -- This is achieved by only pivoting ON one expression (that has all columns concatenated together)
                        ON '||dq_concat(columns, ' || ''_'' || ')||' IN columns_parameter_enum
                        
                        -- VALUES
                        -- Each PIVOT will use a single value metric
                        USING '|| nq(i) ||'

                        -- ROWS
                        GROUP BY dummy_column' ||coalesce(', '||dq_concat(rows, ', '),'')||', value_names 
                    ) 
                    ' 
                ),
                ' UNION ALL BY NAME '
            ) || '
        ), ordered_pivot AS (FROM raw_pivot ORDER BY ALL NULLS FIRST LIMIT 10000000000)
        FROM ordered_pivot 
        '|| CASE WHEN (subtotals OR grand_totals) AND length(rows) > 0 THEN 
            replace_zzz(rows, ['dummy_column', 'value_names'])
        ELSE ''
        END
    )"},
    {nullptr, nullptr, {nullptr}, {{nullptr, nullptr}}, nullptr}};


// To add a new table SQL macro, add a new macro to this array!
// Copy and paste the top item in the array into the 
// second-to-last position and make some modifications. 
// (essentially, leave the last entry in the array as {nullptr, nullptr, {nullptr}, {{nullptr, nullptr}}, nullptr}

// Keep the DEFAULT_SCHEMA (no change needed)
// Replace "times_two_table" with a name for your macro
// If your function has parameters without default values, add their names in quotes inside of the {}, with a nullptr at the end
//      If you do not have parameters, simplify to {nullptr}
// If your function has parameters with default values, add their names and values in quotes inside of {}'s inside of the {}.
// Be sure to keep {nullptr, nullptr} at the end
//      If you do not have parameters with default values, simplify to {nullptr, nullptr}
// Add the text of your SQL macro as a raw string with the format R"( select 42; )" 

// clang-format off
static const DefaultTableMacro dynamic_sql_examples_table_macros[] = {
	{DEFAULT_SCHEMA, "build_my_enum", {"table_names", "columns", "filters", nullptr}, {{nullptr, nullptr}},  R"(
        -- DuckDB MACROs must be a single statement, and to keep the PIVOT statement a single statement also,
        -- we need to already know the names of the columns that are being pivoted out. 
        -- This function is used to create an enum (in client code that uses this library)
        -- that will contain all of those column names.
        -- Note that this is safe to call with an empty columns list, so calling code can 
        -- always create the ENUM, even if it is not going to be used.
        FROM query(
            '
        FROM query_table(['||dq_concat(table_names, ', ')||']) 
        SELECT DISTINCT
            -- When pivoting, do not use all combinations of values in the columns parameter,
            -- only use the combinations that actually exist in the data. 
            -- This is achieved by only pivoting ON one expression (that has all columns concatenated together).
            -- Therefore, we concatenate everything together here with an _ separator.
            '||coalesce(nq_concat(list_transform(dq_list(columns), (i) -> 'coalesce(' ||i||'::varchar , ''NULL'')'), ' || ''_'' || ')||'', '1')||'
        '|| coalesce('WHERE 1=1 AND ' || nq_concat(filters, ' AND '), '') ||'
        ORDER BY ALL
        '
        )
    )"},
    {DEFAULT_SCHEMA, "pivot_table", {"table_names", "values", "rows", "columns", "filters", nullptr}, {{"values_axis", "'columns'"}, {"subtotals", "0"}, {"grand_totals", "0"}, {nullptr, nullptr}}, R"( 
        -- Dynamically build up a SQL string then execute it using the query function.
        -- If the columns parameter is populated, a PIVOT statement will be executed.
        -- If an empty columns parameter is passed, then the statement will be a group by.
        -- The values_axis describes which axis to put multiple values parameters onto. 
        --    Ex: If values:=['sum(col1)', 'max(col2)'], should we have a separate column for each value or a separate row?
        -- If columns are passed in, the values axis should be handled differently, so there are 2 cases for the different values_axis parameters
        -- This function only requires one of these three lists to have at least one element: rows, values, columns. 
        -- The filters list is optional. 
        FROM query(
            CASE WHEN length(columns) = 0 THEN 
                no_columns(table_names, values, rows, filters, values_axis := values_axis, subtotals := subtotals, grand_totals := grand_totals)
            WHEN values_axis = 'columns' OR length(values) = 0 THEN 
                columns_values_axis_columns(table_names, values, rows, columns, filters, values_axis := 'columns', subtotals := subtotals, grand_totals := grand_totals)
            WHEN values_axis = 'rows' THEN 
                columns_values_axis_rows(table_names, values, rows, columns, filters, values_axis := 'rows', subtotals := subtotals, grand_totals := grand_totals)
            END
        )
        SELECT * EXCLUDE (dummy_column)
    )"},
    {DEFAULT_SCHEMA, "pivot_table_show_sql", {"table_names", "values", "rows", "columns", "filters", nullptr}, {{"values_axis", "'columns'"}, {"subtotals", "0"}, {"grand_totals", "0"}, {nullptr, nullptr}}, R"( 
        -- Show the SQL that pivot_table would have executed. 
        -- Useful for debugging or understanding the inner workings of pivot_table.
        SELECT 
                CASE WHEN length(columns) = 0 THEN 
                    no_columns(table_names, values, rows, filters, values_axis := values_axis, subtotals := subtotals, grand_totals := grand_totals)
                WHEN values_axis = 'columns' OR length(values) = 0 THEN 
                    columns_values_axis_columns(table_names, values, rows, columns, filters, values_axis := 'columns', subtotals := subtotals, grand_totals := grand_totals)
                WHEN values_axis = 'rows' THEN 
                    columns_values_axis_rows(table_names, values, rows, columns, filters, values_axis := 'rows', subtotals := subtotals, grand_totals := grand_totals)
                END AS sql_string
    )"},
	{nullptr, nullptr, {nullptr}, {{nullptr, nullptr}}, nullptr}
	};
// clang-format on

inline void PivotTableScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name) {
			return StringVector::AddString(result, "PivotTable "+name.GetString()+" 🐥");;
        });
}

inline void PivotTableOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name) {
			return StringVector::AddString(result, "PivotTable " + name.GetString() +
                                                     ", my linked OpenSSL version is " +
                                                     OPENSSL_VERSION_TEXT );;
        });
}

static void LoadInternal(DatabaseInstance &instance) {
    // Register a scalar function
    auto pivot_table_scalar_function = ScalarFunction("pivot_table", {LogicalType::VARCHAR}, LogicalType::VARCHAR, PivotTableScalarFun);
    ExtensionUtil::RegisterFunction(instance, pivot_table_scalar_function);

    // Register another scalar function
    auto pivot_table_openssl_version_scalar_function = ScalarFunction("pivot_table_openssl_version", {LogicalType::VARCHAR},
                                                LogicalType::VARCHAR, PivotTableOpenSSLVersionScalarFun);
    ExtensionUtil::RegisterFunction(instance, pivot_table_openssl_version_scalar_function);

    // Scalar Macros
	for (idx_t index = 0; dynamic_sql_examples_macros[index].name != nullptr; index++) {
		auto info = DefaultFunctionGenerator::CreateInternalMacroInfo(dynamic_sql_examples_macros[index]);
		ExtensionUtil::RegisterFunction(instance, *info);
	}
    // Table Macros
    for (idx_t index = 0; dynamic_sql_examples_table_macros[index].name != nullptr; index++) {
		auto table_info = DefaultTableFunctionGenerator::CreateTableMacroInfo(dynamic_sql_examples_table_macros[index]);
        ExtensionUtil::RegisterFunction(instance, *table_info);
	}
}

void PivotTableExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string PivotTableExtension::Name() {
	return "pivot_table";
}

std::string PivotTableExtension::Version() const {
#ifdef EXT_VERSION_PIVOT_TABLE
	return EXT_VERSION_PIVOT_TABLE;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void pivot_table_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::PivotTableExtension>();
}

DUCKDB_EXTENSION_API const char *pivot_table_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
