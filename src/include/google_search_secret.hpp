#pragma once

#include "duckdb.hpp"

namespace duckdb {

// Register the google_search secret type
void RegisterGoogleSearchSecretType(ExtensionLoader &loader);

// Config structure for Google Search API
struct GoogleSearchConfig {
	string api_key;
	string cx; // Search engine ID
};

// Helper to get config from secret
GoogleSearchConfig GetGoogleSearchConfigFromSecret(ClientContext &context);

} // namespace duckdb
