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
