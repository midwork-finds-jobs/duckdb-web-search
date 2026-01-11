#include "google_search_secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

// Create a google_search secret from user input
static unique_ptr<BaseSecret> CreateGoogleSearchSecretFunction(ClientContext &context, CreateSecretInput &input) {
	auto scope = input.scope;

	// Create a KeyValueSecret with type "google_search", provider "config"
	auto result = make_uniq<KeyValueSecret>(scope, "google_search", "config", input.name);

	// Parse options
	for (const auto &named_param : input.options) {
		auto lower_name = StringUtil::Lower(named_param.first);

		if (lower_name == "key") {
			result->secret_map["key"] = named_param.second.ToString();
		} else if (lower_name == "cx") {
			result->secret_map["cx"] = named_param.second.ToString();
		} else {
			throw InvalidInputException("Unknown parameter for google_search secret: '%s'. Expected: key, cx",
			                            lower_name);
		}
	}

	// Validate required fields
	if (result->secret_map.find("key") == result->secret_map.end()) {
		throw InvalidInputException("google_search secret requires 'key' parameter (API key)");
	}
	if (result->secret_map.find("cx") == result->secret_map.end()) {
		throw InvalidInputException("google_search secret requires 'cx' parameter (Search Engine ID)");
	}

	// Set keys to redact in logs
	result->redact_keys = {"key"};

	return std::move(result);
}

// Set parameters for the create secret function
static void SetGoogleSearchSecretParameters(CreateSecretFunction &function) {
	function.named_parameters["key"] = LogicalType::VARCHAR;
	function.named_parameters["cx"] = LogicalType::VARCHAR;
}

// Register the google_search secret type
void RegisterGoogleSearchSecretType(ExtensionLoader &loader) {
	// Define the secret type
	SecretType secret_type;
	secret_type.name = "google_search";
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";

	// Register the secret type
	loader.RegisterSecretType(secret_type);

	// Define and register the create secret function
	CreateSecretFunction google_search_secret_function = {"google_search", "config", CreateGoogleSearchSecretFunction};
	SetGoogleSearchSecretParameters(google_search_secret_function);
	loader.RegisterFunction(google_search_secret_function);
}

// Helper function to get Google Search config from secret
GoogleSearchConfig GetGoogleSearchConfigFromSecret(ClientContext &context) {
	auto &secret_manager = SecretManager::Get(context);

	// Try to find a google_search secret (any name)
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	auto secret_match = secret_manager.LookupSecret(transaction, "google_search", "google_search");

	if (!secret_match.HasMatch()) {
		throw InvalidInputException(
		    "No google_search secret found. Create one with:\n\n"
		    "  CREATE SECRET google_search (\n"
		    "    TYPE google_search,\n"
		    "    key 'YOUR_API_KEY',\n"
		    "    cx 'YOUR_SEARCH_ENGINE_ID'\n"
		    "  );\n\n"
		    "Get API key: https://developers.google.com/custom-search/v1/introduction\n"
		    "Create cx:   https://programmablesearchengine.google.com/controlpanel/all");
	}

	auto &secret = secret_match.GetSecret();
	if (secret.GetType() != "google_search") {
		throw InvalidInputException("Secret is not a google_search secret (type is '%s')", secret.GetType());
	}

	// Cast to KeyValueSecret
	auto &kv_secret = dynamic_cast<const KeyValueSecret &>(secret);

	GoogleSearchConfig config;

	auto key_it = kv_secret.secret_map.find("key");
	if (key_it != kv_secret.secret_map.end()) {
		config.api_key = key_it->second.ToString();
	}

	auto cx_it = kv_secret.secret_map.find("cx");
	if (cx_it != kv_secret.secret_map.end()) {
		config.cx = cx_it->second.ToString();
	}

	return config;
}

} // namespace duckdb
