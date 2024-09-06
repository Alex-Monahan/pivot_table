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
// (essentially, leave the last entry in the array as {nullptr, nullptr, {nullptr}, nullptr})

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
    -- Output is in the format: ['zzzSubtotal' as "subcat", 'zzzGrand Total' as "subcat", 'zzzGrand Total' as "category"]
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
        
        'FROM query_table(['||dq_concat(table_names, ', ')||']) 
        SELECT 
            1 as dummy_column,
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
            '||CASE WHEN values_axis != 'rows' OR length(values) = 0 THEN '' 
                ELSE ' UNNEST(['||sq_concat(values, ', ')||']) AS value_names, 
                    UNNEST([' END||'
                    '||coalesce(nq_concat(values, ', ')||' ', '') ||'
            '||CASE WHEN values_axis != 'rows' OR length(values) = 0 THEN '' ELSE ']) AS values ' END||'
        '|| coalesce('WHERE 1=1 AND ' || nq_concat(filters, ' AND '), '') ||'
        GROUP BY ' || 
            CASE WHEN subtotals AND length(rows) > 0 THEN 'ROLLUP ('|| dq_concat(rows, ', ') ||') ' 
            WHEN grand_totals AND length(rows) > 0  AND NOT subtotals THEN 'GROUPING SETS ((), ('|| dq_concat(rows, ', ') ||'))'
            ELSE 'ALL ' 
            END ||' 
        ' ||CASE WHEN NOT grand_totals AND subtotals AND length(rows) > 0 THEN 'HAVING 
        list_aggregate(['||nq_concat(
                        list_transform(
                            rows,
                            (i) -> 'GROUPING('||dq(i)||')'),
                            ', ') ||'],
                        ''sum'') != '||length(rows) ELSE '' END|| '
        ORDER BY ' || 
            CASE WHEN (subtotals OR grand_totals) AND length(rows) > 0 THEN 
                nq_concat(
                    list_transform(
                        rows,
                        (i) -> 'GROUPING('||dq(i)||'), '||dq(i)),
                    ', ') 
            ELSE 'ALL NULLS FIRST ' 
            END 
    )"},
    {DEFAULT_SCHEMA, "columns_values_axis_columns", {"table_names", "values", "rows", "columns", "filters", nullptr}, {{"values_axis", "'columns'"}, {"subtotals", "0"}, {"grand_totals", "0"}, {nullptr, nullptr}}, R"( 
        'WITH raw_pivot AS (
            PIVOT (
                '||
                CASE WHEN (subtotals OR grand_totals) AND length(rows) > 0 THEN 
                    nq_concat(
                        ['FROM query_table(['||dq_concat(table_names, ', ')||']) 
                        SELECT *, 1 as dummy_column'] || 
                        list_transform(
                            totals_list(rows, subtotals:=subtotals, grand_totals:=grand_totals),
                            k -> 
                            'FROM query_table(['||dq_concat(table_names, ', ')||']) 
                            SELECT * replace(' || k || '), 1 as dummy_column'
                        ),
                        ' 
                        UNION ALL BY NAME 
                        ' 
                    )
                ELSE '
                    FROM query_table(['||dq_concat(table_names, ', ')||']) 
                    SELECT *, 1 as dummy_column
                    '|| coalesce('WHERE 1=1 AND ' || nq_concat(filters, ' AND '), '')
                END ||'
            )
            ON '||dq_concat(columns, ' || ''_'' || ')||' IN columns_parameter_enum
            '|| coalesce('USING '||nq_concat(values, ', '), '')||'
            GROUP BY dummy_column'||coalesce(', '||dq_concat(rows, ', '),'') || ' 
            ORDER BY ALL NULLS FIRST 
        ) FROM raw_pivot 
        '|| CASE WHEN (subtotals OR grand_totals) AND length(rows) > 0 THEN 
            replace_zzz(rows, ['dummy_column'])
        ELSE ''
        END
    )"},
    {DEFAULT_SCHEMA, "columns_values_axis_rows", {"table_names", "values", "rows", "columns", "filters", nullptr}, {{"values_axis", "'rows'"}, {"subtotals", "0"}, {"grand_totals", "0"}, {nullptr, nullptr}}, R"( 
        'WITH raw_pivot AS ( '||
            nq_concat(
                list_transform(values, (i) -> 
                    '
                    FROM (
                        PIVOT (
                            '||
                            CASE WHEN (subtotals OR grand_totals) AND length(rows) > 0 THEN 
                                nq_concat(
                                    ['FROM query_table(['||dq_concat(table_names, ', ')||']) 
                                    SELECT *, 1 as dummy_column, '|| sq(i)||' AS value_names '] || 
                                    list_transform(
                                        totals_list(rows, subtotals:=subtotals, grand_totals:=grand_totals),
                                        k -> 
                                        'FROM query_table(['||dq_concat(table_names, ', ')||']) 
                                        SELECT * replace(' || k || '), 1 as dummy_column, '|| sq(i) ||' AS value_names '
                                    ),
                                    ' 
                                    UNION ALL BY NAME 
                                    '
                                )
                            ELSE '
                                FROM query_table(['||dq_concat(table_names, ', ')||']) 
                                SELECT *, 1 as dummy_column, '|| sq(i) ||' AS value_names 
                                '|| coalesce('WHERE 1=1 AND ' || nq_concat(filters, ' AND '), '')
                            END ||'
                        )
                        ON '||dq_concat(columns, ' || ''_'' || ')||' IN columns_parameter_enum
                        USING '|| nq(i) ||'
                        GROUP BY dummy_column' ||coalesce(', '||dq_concat(rows, ', '),'')||', value_names 
                    ) 
                    ' 
                ),
                ' UNION ALL BY NAME '
            ) || ' ORDER BY ALL NULLS FIRST
        ) FROM raw_pivot 
        '|| CASE WHEN (subtotals OR grand_totals) AND length(rows) > 0 THEN 
            replace_zzz(rows, ['dummy_column', 'value_names'])
        ELSE ''
        END
    )"},
    {nullptr, nullptr, {nullptr}, {{nullptr, nullptr}}, nullptr}};


// To add a new table SQL macro, add a new macro to this array!
// Copy and paste the top item in the array into the 
// second-to-last position and make some modifications. 
// (essentially, leave the last entry in the array as {nullptr, nullptr, {nullptr}, nullptr})

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
	{DEFAULT_SCHEMA, "times_two_table", {"x", nullptr}, {{"two", "2"}, {nullptr, nullptr}},  R"(SELECT x * two as output_column;)"},
	{DEFAULT_SCHEMA, "build_my_enum", {"table_names", "columns", "filters", nullptr}, {{nullptr, nullptr}},  R"(
        FROM query(
            '
        FROM query_table(['||dq_concat(table_names, ', ')||']) 
        SELECT DISTINCT
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

    // Macros
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
