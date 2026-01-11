#define DUCKDB_EXTENSION_MAIN

#include "google_search_extension.hpp"
#include "google_search_secret.hpp"
#include "google_search_function.hpp"
#include "google_image_search_function.hpp"
#include "duckdb.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

// Combined optimizer for all table functions
static void GoogleSearchOptimizer(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	OptimizeGoogleSearchLimitPushdown(plan);
	OptimizeGoogleSearchOrderByPushdown(plan);
	OptimizeGoogleImageSearchLimitPushdown(plan);
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register google_search secret type
	RegisterGoogleSearchSecretType(loader);

	// Register google_search() table function
	RegisterGoogleSearchFunction(loader);

	// Register google_image_search() table function
	RegisterGoogleImageSearchFunction(loader);

	// Register optimizer extension for LIMIT pushdown
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	OptimizerExtension optimizer;
	optimizer.optimize_function = GoogleSearchOptimizer;
	config.optimizer_extensions.push_back(std::move(optimizer));
}

void GoogleSearchExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string GoogleSearchExtension::Name() {
	return "google_search";
}

std::string GoogleSearchExtension::Version() const {
#ifdef EXT_VERSION_GOOGLE_SEARCH
	return EXT_VERSION_GOOGLE_SEARCH;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(google_search, loader) {
	duckdb::LoadInternal(loader);
}
}
