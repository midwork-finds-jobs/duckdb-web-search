#include "google_image_search_function.hpp"
#include "google_search_secret.hpp"
#include "http_client.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/common/types/value.hpp"
#include "yyjson.hpp"
#include <mutex>

using namespace duckdb_yyjson;

namespace duckdb {

// Image search result from Google API
struct GoogleImageSearchResult {
	string title;
	string link;           // Page URL
	string image_url;      // Direct image URL
	string thumbnail_url;  // Thumbnail URL
	int width = 0;
	int height = 0;
	int thumbnail_width = 0;
	int thumbnail_height = 0;
	string context_link; // Page containing image
	string mime;
	string snippet;
};

// Bind data for google_image_search() table function
struct GoogleImageSearchBindData : public TableFunctionData {
	string query;
	string api_key;
	string cx;
	idx_t max_results = 100; // For LIMIT pushdown
	GoogleImageSearchFilters filters;
};

// Global state for google_image_search() table function
struct GoogleImageSearchGlobalState : public GlobalTableFunctionState {
	vector<GoogleImageSearchResult> results;
	idx_t current_idx = 0;
	int next_start = 1;
	bool fetch_complete = false;
	mutex state_mutex;

	idx_t MaxThreads() const override {
		return 1;
	}
};

// Build the Google Image Search API URL
static string BuildGoogleImageSearchUrl(const GoogleImageSearchBindData &bind_data, int start) {
	string url = "https://www.googleapis.com/customsearch/v1";
	url += "?key=" + UrlEncode(bind_data.api_key);
	url += "&cx=" + UrlEncode(bind_data.cx);
	url += "&q=" + UrlEncode(bind_data.query);
	url += "&searchType=image"; // Key difference: image search
	url += "&num=10";
	url += "&start=" + std::to_string(start);

	auto &f = bind_data.filters;
	if (!f.exact_terms.empty()) {
		url += "&exactTerms=" + UrlEncode(f.exact_terms);
	}
	if (!f.exclude_terms.empty()) {
		url += "&excludeTerms=" + UrlEncode(f.exclude_terms);
	}
	if (!f.site_search.empty()) {
		url += "&siteSearch=" + UrlEncode(f.site_search);
		url += "&siteSearchFilter=i";
	}
	if (!f.date_restrict.empty()) {
		url += "&dateRestrict=" + UrlEncode(f.date_restrict);
	}
	if (!f.safe.empty()) {
		url += "&safe=" + UrlEncode(f.safe);
	}
	if (!f.rights.empty()) {
		url += "&rights=" + UrlEncode(f.rights);
	}

	// Image-specific filters
	if (!f.img_size.empty()) {
		url += "&imgSize=" + UrlEncode(f.img_size);
	}
	if (!f.img_type.empty()) {
		url += "&imgType=" + UrlEncode(f.img_type);
	}
	if (!f.img_color_type.empty()) {
		url += "&imgColorType=" + UrlEncode(f.img_color_type);
	}
	if (!f.img_dominant_color.empty()) {
		url += "&imgDominantColor=" + UrlEncode(f.img_dominant_color);
	}

	// Request only needed fields for better performance
	// See: https://developers.google.com/custom-search/v1/performance
	url += "&fields=" + UrlEncode(
	    "items(title,link,snippet,mime,image),"
	    "queries(nextPage)");

	return url;
}

// Parse JSON string helper
static string GetJsonString(yyjson_val *obj, const char *key) {
	yyjson_val *val = yyjson_obj_get(obj, key);
	if (val && yyjson_is_str(val)) {
		return yyjson_get_str(val);
	}
	return "";
}

// Parse JSON int helper
static int GetJsonInt(yyjson_val *obj, const char *key) {
	yyjson_val *val = yyjson_obj_get(obj, key);
	if (val && yyjson_is_int(val)) {
		return static_cast<int>(yyjson_get_int(val));
	}
	return 0;
}

// Fetch results from Google Image Search API
static void FetchGoogleImageSearchResults(ClientContext &context, GoogleImageSearchGlobalState &state,
                                          const GoogleImageSearchBindData &bind_data) {
	RetryConfig retry_config;

	while (state.results.size() < bind_data.max_results && !state.fetch_complete) {
		string url = BuildGoogleImageSearchUrl(bind_data, state.next_start);
		auto response = HttpClient::Fetch(context, url, retry_config);

		if (!response.success) {
			if (response.status_code == 401) {
				throw InvalidInputException("Google Search API: Invalid API key");
			} else if (response.status_code == 403) {
				throw InvalidInputException("Google Search API: Access denied or quota exceeded");
			} else if (response.status_code == 400) {
				throw InvalidInputException("Google Search API: Invalid request - %s", response.error);
			}
			throw IOException("Google Search API error: %s (status %d)", response.error, response.status_code);
		}

		// Parse JSON response
		yyjson_doc *doc = yyjson_read(response.body.c_str(), response.body.size(), 0);
		if (!doc) {
			throw IOException("Failed to parse Google Search API response as JSON");
		}

		yyjson_val *root = yyjson_doc_get_root(doc);

		// Check for API error
		yyjson_val *error = yyjson_obj_get(root, "error");
		if (error) {
			yyjson_val *message = yyjson_obj_get(error, "message");
			string err_msg = message && yyjson_is_str(message) ? yyjson_get_str(message) : "Unknown error";
			yyjson_doc_free(doc);
			throw InvalidInputException("Google Search API error: %s", err_msg);
		}

		// Get items array
		yyjson_val *items = yyjson_obj_get(root, "items");
		if (!items || !yyjson_is_arr(items) || yyjson_arr_size(items) == 0) {
			state.fetch_complete = true;
			yyjson_doc_free(doc);
			break;
		}

		// Process each item
		size_t idx, max;
		yyjson_val *item;
		yyjson_arr_foreach(items, idx, max, item) {
			if (state.results.size() >= bind_data.max_results) {
				break;
			}

			GoogleImageSearchResult result;
			result.title = GetJsonString(item, "title");
			result.link = GetJsonString(item, "link");
			result.snippet = GetJsonString(item, "snippet");
			result.mime = GetJsonString(item, "mime");

			// Get image object
			yyjson_val *image = yyjson_obj_get(item, "image");
			if (image) {
				result.context_link = GetJsonString(image, "contextLink");
				result.width = GetJsonInt(image, "width");
				result.height = GetJsonInt(image, "height");
				result.thumbnail_url = GetJsonString(image, "thumbnailLink");
				result.thumbnail_width = GetJsonInt(image, "thumbnailWidth");
				result.thumbnail_height = GetJsonInt(image, "thumbnailHeight");
			}

			// The link field IS the image URL for image search
			result.image_url = result.link;

			state.results.push_back(result);
		}

		// Check for next page and extract startIndex
		yyjson_val *queries = yyjson_obj_get(root, "queries");
		if (queries) {
			yyjson_val *next_page = yyjson_obj_get(queries, "nextPage");
			if (next_page && yyjson_is_arr(next_page) && yyjson_arr_size(next_page) > 0) {
				yyjson_val *next_page_obj = yyjson_arr_get_first(next_page);
				if (next_page_obj) {
					yyjson_val *start_index = yyjson_obj_get(next_page_obj, "startIndex");
					if (start_index && yyjson_is_int(start_index)) {
						state.next_start = static_cast<int>(yyjson_get_int(start_index));
					} else {
						state.fetch_complete = true;
					}
				} else {
					state.fetch_complete = true;
				}
			} else {
				state.fetch_complete = true;
			}
		} else {
			state.fetch_complete = true;
		}

		yyjson_doc_free(doc);
	}
}

// Bind function
static unique_ptr<FunctionData> GoogleImageSearchBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<GoogleImageSearchBindData>();

	if (input.inputs.empty()) {
		throw InvalidInputException("google_image_search() requires a search query");
	}
	bind_data->query = input.inputs[0].GetValue<string>();

	// Get API credentials from secret
	auto config = GetGoogleSearchConfigFromSecret(context);
	bind_data->api_key = config.api_key;
	bind_data->cx = config.cx;

	// Parse named parameters
	for (auto &kv : input.named_parameters) {
		auto key = StringUtil::Lower(kv.first);
		string value = kv.second.GetValue<string>();

		if (key == "exact_terms") {
			bind_data->filters.exact_terms = value;
		} else if (key == "exclude_terms") {
			bind_data->filters.exclude_terms = value;
		} else if (key == "site") {
			bind_data->filters.site_search = value;
		} else if (key == "date_restrict") {
			bind_data->filters.date_restrict = value;
		} else if (key == "safe") {
			bind_data->filters.safe = value;
		} else if (key == "rights") {
			bind_data->filters.rights = value;
		} else if (key == "img_size") {
			bind_data->filters.img_size = value;
		} else if (key == "img_type") {
			bind_data->filters.img_type = value;
		} else if (key == "img_color_type") {
			bind_data->filters.img_color_type = value;
		} else if (key == "img_dominant_color") {
			bind_data->filters.img_dominant_color = value;
		}
	}

	// Set output schema for image search
	names.emplace_back("title");
	names.emplace_back("link");
	names.emplace_back("image_url");
	names.emplace_back("thumbnail_url");
	names.emplace_back("width");
	names.emplace_back("height");
	names.emplace_back("thumbnail_width");
	names.emplace_back("thumbnail_height");
	names.emplace_back("context_link");
	names.emplace_back("mime");
	names.emplace_back("snippet");

	return_types.emplace_back(LogicalType::VARCHAR);  // title
	return_types.emplace_back(LogicalType::VARCHAR);  // link (page URL)
	return_types.emplace_back(LogicalType::VARCHAR);  // image_url
	return_types.emplace_back(LogicalType::VARCHAR);  // thumbnail_url
	return_types.emplace_back(LogicalType::INTEGER);  // width
	return_types.emplace_back(LogicalType::INTEGER);  // height
	return_types.emplace_back(LogicalType::INTEGER);  // thumbnail_width
	return_types.emplace_back(LogicalType::INTEGER);  // thumbnail_height
	return_types.emplace_back(LogicalType::VARCHAR);  // context_link
	return_types.emplace_back(LogicalType::VARCHAR);  // mime
	return_types.emplace_back(LogicalType::VARCHAR);  // snippet

	return std::move(bind_data);
}

// Global init function
static unique_ptr<GlobalTableFunctionState> GoogleImageSearchInitGlobal(ClientContext &context,
                                                                        TableFunctionInitInput &input) {
	auto state = make_uniq<GoogleImageSearchGlobalState>();
	auto &bind_data = input.bind_data->Cast<GoogleImageSearchBindData>();

	FetchGoogleImageSearchResults(context, *state, bind_data);

	return std::move(state);
}

// Scan function
static void GoogleImageSearchScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<GoogleImageSearchGlobalState>();

	idx_t count = 0;
	idx_t max_count = STANDARD_VECTOR_SIZE;

	while (count < max_count && state.current_idx < state.results.size()) {
		auto &result = state.results[state.current_idx];

		output.SetValue(0, count, Value(result.title));
		output.SetValue(1, count, Value(result.link));
		output.SetValue(2, count, Value(result.image_url));
		output.SetValue(3, count, Value(result.thumbnail_url));
		output.SetValue(4, count, Value::INTEGER(result.width));
		output.SetValue(5, count, Value::INTEGER(result.height));
		output.SetValue(6, count, Value::INTEGER(result.thumbnail_width));
		output.SetValue(7, count, Value::INTEGER(result.thumbnail_height));
		output.SetValue(8, count, Value(result.context_link));
		output.SetValue(9, count, Value(result.mime));
		output.SetValue(10, count, Value(result.snippet));

		state.current_idx++;
		count++;
	}

	output.SetCardinality(count);
}

// LIMIT pushdown optimizer
void OptimizeGoogleImageSearchLimitPushdown(unique_ptr<LogicalOperator> &op) {
	if (op->type == LogicalOperatorType::LOGICAL_LIMIT) {
		auto &limit = op->Cast<LogicalLimit>();
		reference<LogicalOperator> child = *op->children[0];

		while (child.get().type == LogicalOperatorType::LOGICAL_PROJECTION) {
			child = *child.get().children[0];
		}

		if (child.get().type != LogicalOperatorType::LOGICAL_GET) {
			OptimizeGoogleImageSearchLimitPushdown(op->children[0]);
			return;
		}

		auto &get = child.get().Cast<LogicalGet>();
		if (get.function.name != "google_image_search") {
			OptimizeGoogleImageSearchLimitPushdown(op->children[0]);
			return;
		}

		switch (limit.limit_val.Type()) {
		case LimitNodeType::CONSTANT_VALUE:
		case LimitNodeType::UNSET:
			break;
		default:
			OptimizeGoogleImageSearchLimitPushdown(op->children[0]);
			return;
		}

		auto &bind_data = get.bind_data->Cast<GoogleImageSearchBindData>();
		if (limit.limit_val.Type() == LimitNodeType::CONSTANT_VALUE) {
			auto limit_value = limit.limit_val.GetConstantValue();
			bind_data.max_results = std::min(limit_value, (idx_t)100);
		}
		return;
	}

	for (auto &child : op->children) {
		OptimizeGoogleImageSearchLimitPushdown(child);
	}
}

// Register the table function
void RegisterGoogleImageSearchFunction(ExtensionLoader &loader) {
	TableFunction func("google_image_search", {LogicalType::VARCHAR}, GoogleImageSearchScan, GoogleImageSearchBind,
	                   GoogleImageSearchInitGlobal);

	// Named parameters for filter pushdown
	func.named_parameters["exact_terms"] = LogicalType::VARCHAR;
	func.named_parameters["exclude_terms"] = LogicalType::VARCHAR;
	func.named_parameters["site"] = LogicalType::VARCHAR;
	func.named_parameters["date_restrict"] = LogicalType::VARCHAR;
	func.named_parameters["safe"] = LogicalType::VARCHAR;
	func.named_parameters["rights"] = LogicalType::VARCHAR;

	// Image-specific parameters
	func.named_parameters["img_size"] = LogicalType::VARCHAR;
	func.named_parameters["img_type"] = LogicalType::VARCHAR;
	func.named_parameters["img_color_type"] = LogicalType::VARCHAR;
	func.named_parameters["img_dominant_color"] = LogicalType::VARCHAR;

	loader.RegisterFunction(func);
}

} // namespace duckdb
