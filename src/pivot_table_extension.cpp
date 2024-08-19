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

#include "default_functions.hpp"
#include "default_table_functions.hpp"

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
    {DEFAULT_SCHEMA, "times_two", {"x", nullptr}, R"(x*2)"},
    {nullptr, nullptr, {nullptr}, nullptr}};


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
