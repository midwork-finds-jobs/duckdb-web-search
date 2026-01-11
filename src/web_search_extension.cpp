#define DUCKDB_EXTENSION_MAIN

#include "web_search_extension.hpp"
#include "google_search_secret.hpp"
#include "google_search_function.hpp"
#include "google_image_search_function.hpp"
#include "annotation_copy.hpp"
#include "duckdb.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension_helper.hpp"

namespace duckdb {

// Combined optimizer for all table functions
static void WebSearchOptimizer(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	OptimizeGoogleSearchLimitPushdown(plan);
	OptimizeGoogleSearchOrderByPushdown(plan);
	OptimizeGoogleImageSearchLimitPushdown(plan);
}

static void LoadInternal(ExtensionLoader &loader) {
	// Autoload JSON extension (for pagemap column JSON type)
	auto &db = loader.GetDatabaseInstance();
	ExtensionHelper::TryAutoLoadExtension(db, "json");

	// Register google_search secret type
	RegisterGoogleSearchSecretType(loader);

	// Register google_search() table function
	RegisterGoogleSearchFunction(loader);

	// Register google_image_search() table function
	RegisterGoogleImageSearchFunction(loader);

	// Register google_pse_annotation COPY function
	RegisterAnnotationCopyFunction(loader);

	// Register optimizer extension for LIMIT pushdown
	auto &config = DBConfig::GetConfig(db);
	OptimizerExtension optimizer;
	optimizer.optimize_function = WebSearchOptimizer;
	config.optimizer_extensions.push_back(std::move(optimizer));
}

void WebSearchExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string WebSearchExtension::Name() {
	return "web_search";
}

std::string WebSearchExtension::Version() const {
#ifdef EXT_VERSION_WEB_SEARCH
	return EXT_VERSION_WEB_SEARCH;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(web_search, loader) {
	duckdb::LoadInternal(loader);
}
}
