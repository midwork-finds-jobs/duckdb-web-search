#include "google_search_function.hpp"
#include "google_search_secret.hpp"
#include "http_client.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "yyjson.hpp"
#include <mutex>
#include <ctime>
#include <sstream>
#include <iomanip>

using namespace duckdb_yyjson;

namespace duckdb {

// Search result from Google API
struct GoogleSearchResult {
	string title;
	string link;
	string snippet;
	string display_link;
	string formatted_url;
	string html_formatted_url;
	string html_title;
	string html_snippet;
	string mime;
	string file_format;
	string pagemap; // JSON string
	string site;    // Extracted from link for filtering
	string date;    // Page date (for ORDER BY pushdown)
};

// Bind data for google_search() table function
struct GoogleSearchBindData : public TableFunctionData {
	string query;
	string api_key;
	string cx;
	idx_t max_results = 100; // For LIMIT pushdown (Google max is 100)

	// Columns for output schema
	vector<string> column_names;
	vector<LogicalType> column_types;

	// Pushdown filter data
	vector<string> site_includes;     // Sites to include (OR'd into query as site:domain)
	vector<string> site_excludes;     // Sites to exclude (OR'd into query as -site:domain)
	timestamp_t date_from;            // Date range start
	timestamp_t date_to;              // Date range end
	bool has_date_filter = false;

	// Other filters (via named params)
	GoogleSearchFilters filters;
};

// Per-site pagination state
struct SitePaginationState {
	int next_start = 1;
	bool exhausted = false;
};

// Global state for google_search() table function
struct GoogleSearchGlobalState : public GlobalTableFunctionState {
	vector<GoogleSearchResult> results;
	idx_t current_idx = 0;

	// For multi-site queries: track pagination per site
	vector<SitePaginationState> site_states;
	idx_t current_site_idx = 0;

	// For single query (no site filter)
	int next_start = 1;
	bool fetch_complete = false;

	mutex state_mutex;

	idx_t MaxThreads() const override {
		return 1; // Single-threaded for HTTP fetching
	}
};

// Extract domain from URL
static string ExtractDomain(const string &url) {
	// Find ://
	size_t protocol_end = url.find("://");
	size_t start = (protocol_end != string::npos) ? protocol_end + 3 : 0;

	// Find end of domain (first / or end of string)
	size_t end = url.find('/', start);
	if (end == string::npos) {
		end = url.length();
	}

	return url.substr(start, end - start);
}

// Convert timestamp to Google dateRestrict format
// Google uses: d[number] for days, w[number] for weeks, m[number] for months, y[number] for years
static string TimestampToDateRestrict(timestamp_t from_ts, timestamp_t to_ts) {
	// Get current time
	auto now = std::chrono::system_clock::now();
	auto now_time_t = std::chrono::system_clock::to_time_t(now);

	// Convert from_ts to time_t (DuckDB timestamp is microseconds since epoch)
	time_t from_time_t = from_ts.value / 1000000;

	// Calculate difference in days
	double diff_seconds = difftime(now_time_t, from_time_t);
	int diff_days = static_cast<int>(diff_seconds / 86400);

	if (diff_days <= 0) {
		return ""; // Future date, no restriction
	} else if (diff_days <= 7) {
		return "d" + std::to_string(diff_days);
	} else if (diff_days <= 31) {
		int weeks = (diff_days + 6) / 7;
		return "w" + std::to_string(weeks);
	} else if (diff_days <= 365) {
		int months = (diff_days + 29) / 30;
		return "m" + std::to_string(months);
	} else {
		int years = (diff_days + 364) / 365;
		return "y" + std::to_string(years);
	}
}

// Build the Google Search API URL
// site_filter: single site for siteSearch param (used when LIMIT > 100)
// use_or_sites: if true, add all site_includes as (site:a OR site:b) to query (used when LIMIT <= 100)
static string BuildGoogleSearchUrl(const GoogleSearchBindData &bind_data, int start,
                                    const string &site_filter = "", bool use_or_sites = false) {
	string url = "https://www.googleapis.com/customsearch/v1";
	url += "?key=" + UrlEncode(bind_data.api_key);
	url += "&cx=" + UrlEncode(bind_data.cx);

	// Build query
	string full_query = bind_data.query;

	// Prepend structured data filter (e.g., "more:pagemap:document-author:john")
	if (!bind_data.filters.structured_data.empty()) {
		full_query = bind_data.filters.structured_data + " " + full_query;
	}

	// Add site includes as OR clause (for LIMIT <= 100 with multiple sites)
	if (use_or_sites && bind_data.site_includes.size() > 1) {
		full_query += " (";
		for (size_t i = 0; i < bind_data.site_includes.size(); i++) {
			if (i > 0) {
				full_query += " OR ";
			}
			full_query += "site:" + bind_data.site_includes[i];
		}
		full_query += ")";
	} else if (use_or_sites && bind_data.site_includes.size() == 1) {
		full_query += " site:" + bind_data.site_includes[0];
	}

	// Add site excludes to query (-site:domain)
	for (const auto &site : bind_data.site_excludes) {
		full_query += " -site:" + site;
	}

	url += "&q=" + UrlEncode(full_query);
	url += "&num=10"; // Google max per page
	url += "&start=" + std::to_string(start);

	// Add site filter via siteSearch param (for LIMIT > 100, per-site queries)
	if (!site_filter.empty()) {
		url += "&siteSearch=" + UrlEncode(site_filter);
		url += "&siteSearchFilter=i"; // i = include
	}

	// Add date restriction if set
	if (bind_data.has_date_filter && bind_data.date_from.value != 0) {
		string date_restrict = TimestampToDateRestrict(bind_data.date_from, bind_data.date_to);
		if (!date_restrict.empty()) {
			url += "&dateRestrict=" + UrlEncode(date_restrict);
		}
	}

	// Add other filters from named params
	auto &f = bind_data.filters;
	if (!f.exact_terms.empty()) {
		url += "&exactTerms=" + UrlEncode(f.exact_terms);
	}
	if (!f.exclude_terms.empty()) {
		url += "&excludeTerms=" + UrlEncode(f.exclude_terms);
	}
	if (!f.or_terms.empty()) {
		url += "&orTerms=" + UrlEncode(f.or_terms);
	}
	if (!f.file_type.empty()) {
		url += "&fileType=" + UrlEncode(f.file_type);
	}
	if (!f.gl.empty()) {
		url += "&gl=" + UrlEncode(f.gl);
	}
	if (!f.hl.empty()) {
		url += "&hl=" + UrlEncode(f.hl);
	}
	if (!f.language.empty()) {
		url += "&lr=" + UrlEncode(f.language);
	}
	if (!f.safe.empty()) {
		url += "&safe=" + UrlEncode(f.safe);
	}
	if (!f.rights.empty()) {
		url += "&rights=" + UrlEncode(f.rights);
	}
	if (!f.sort.empty()) {
		url += "&sort=" + UrlEncode(f.sort);
	}

	// Request only needed fields for better performance
	// See: https://developers.google.com/custom-search/v1/performance
	url += "&fields=" + UrlEncode(
	    "items(title,link,snippet,displayLink,formattedUrl,htmlFormattedUrl,htmlTitle,htmlSnippet,mime,fileFormat,pagemap),"
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

// Parse a single API response and add results to state
// Returns the next startIndex, or -1 if no more pages
static int ParseGoogleSearchResponse(const string &response_body, GoogleSearchGlobalState &state,
                                      const GoogleSearchBindData &bind_data) {
	yyjson_doc *doc = yyjson_read(response_body.c_str(), response_body.size(), 0);
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
		yyjson_doc_free(doc);
		return -1; // No results
	}

	// Process each item
	size_t idx, max;
	yyjson_val *item;
	yyjson_arr_foreach(items, idx, max, item) {
		if (state.results.size() >= bind_data.max_results) {
			break;
		}

		GoogleSearchResult result;
		result.title = GetJsonString(item, "title");
		result.link = GetJsonString(item, "link");
		result.snippet = GetJsonString(item, "snippet");
		result.display_link = GetJsonString(item, "displayLink");
		result.formatted_url = GetJsonString(item, "formattedUrl");
		result.html_formatted_url = GetJsonString(item, "htmlFormattedUrl");
		result.html_title = GetJsonString(item, "htmlTitle");
		result.html_snippet = GetJsonString(item, "htmlSnippet");
		result.mime = GetJsonString(item, "mime");
		result.file_format = GetJsonString(item, "fileFormat");

		// Extract site from link
		result.site = ExtractDomain(result.link);

		// Get pagemap as JSON string
		yyjson_val *pagemap = yyjson_obj_get(item, "pagemap");
		if (pagemap) {
			char *pagemap_str = yyjson_val_write(pagemap, 0, nullptr);
			if (pagemap_str) {
				result.pagemap = pagemap_str;
				free(pagemap_str);
			}
		}

		state.results.push_back(result);
	}

	// Check for next page and extract startIndex
	int next_start = -1;
	yyjson_val *queries = yyjson_obj_get(root, "queries");
	if (queries) {
		yyjson_val *next_page = yyjson_obj_get(queries, "nextPage");
		if (next_page && yyjson_is_arr(next_page) && yyjson_arr_size(next_page) > 0) {
			yyjson_val *next_page_obj = yyjson_arr_get_first(next_page);
			if (next_page_obj) {
				yyjson_val *start_index = yyjson_obj_get(next_page_obj, "startIndex");
				if (start_index && yyjson_is_int(start_index)) {
					next_start = static_cast<int>(yyjson_get_int(start_index));
				}
			}
		}
	}

	yyjson_doc_free(doc);
	return next_start;
}

// Fetch results from Google Search API
static void FetchGoogleSearchResults(ClientContext &context, GoogleSearchGlobalState &state,
                                     const GoogleSearchBindData &bind_data) {
	RetryConfig retry_config;

	// Decide mode based on LIMIT and number of sites
	// - LIMIT <= 100: single query with (site:a OR site:b) syntax
	// - LIMIT > 100 with multiple sites: separate queries per site (up to 100 each)
	bool use_per_site_queries = bind_data.site_includes.size() > 1 && bind_data.max_results > 100;

	if (use_per_site_queries) {
		// Multi-site query mode: separate queries per site, round-robin
		// Each site can return up to 100 results
		state.site_states.resize(bind_data.site_includes.size());

		while (state.results.size() < bind_data.max_results) {
			// Count how many sites still have results
			idx_t active_sites = 0;
			for (const auto &ss : state.site_states) {
				if (!ss.exhausted) {
					active_sites++;
				}
			}
			if (active_sites == 0) {
				break; // All sites exhausted
			}

			// Find next non-exhausted site (round-robin)
			idx_t attempts = 0;
			while (state.site_states[state.current_site_idx].exhausted && attempts < bind_data.site_includes.size()) {
				state.current_site_idx = (state.current_site_idx + 1) % bind_data.site_includes.size();
				attempts++;
			}

			if (state.site_states[state.current_site_idx].exhausted) {
				break; // All exhausted
			}

			auto &site_state = state.site_states[state.current_site_idx];
			const string &site = bind_data.site_includes[state.current_site_idx];

			// Per-site query: use siteSearch param, no OR syntax
			string url = BuildGoogleSearchUrl(bind_data, site_state.next_start, site, false);
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

			int next_start = ParseGoogleSearchResponse(response.body, state, bind_data);
			if (next_start < 0 || site_state.next_start >= 100) {
				site_state.exhausted = true; // Google max 100 per site
			} else {
				site_state.next_start = next_start;
			}

			// Move to next site for round-robin
			state.current_site_idx = (state.current_site_idx + 1) % bind_data.site_includes.size();
		}
	} else {
		// Single query mode: use (site:a OR site:b) syntax in query string
		// This handles: no sites, single site, or multiple sites with LIMIT <= 100
		bool has_sites = !bind_data.site_includes.empty();

		while (state.results.size() < bind_data.max_results && !state.fetch_complete) {
			// Single query with OR sites in query string
			string url = BuildGoogleSearchUrl(bind_data, state.next_start, "", has_sites);
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

			int next_start = ParseGoogleSearchResponse(response.body, state, bind_data);
			if (next_start < 0 || state.next_start >= 100) {
				state.fetch_complete = true; // Google max 100 total
			} else {
				state.next_start = next_start;
			}
		}
	}
}

// Bind function
static unique_ptr<FunctionData> GoogleSearchBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<GoogleSearchBindData>();

	// First positional argument is the query
	if (input.inputs.empty()) {
		throw InvalidInputException("google_search() requires a search query");
	}
	bind_data->query = input.inputs[0].GetValue<string>();

	// Get API credentials from secret
	auto config = GetGoogleSearchConfigFromSecret(context);
	bind_data->api_key = config.api_key;
	bind_data->cx = config.cx;

	// Parse named parameters (non-pushdown filters)
	for (auto &kv : input.named_parameters) {
		auto key = StringUtil::Lower(kv.first);
		string value = kv.second.GetValue<string>();

		if (key == "exact_terms") {
			bind_data->filters.exact_terms = value;
		} else if (key == "exclude_terms") {
			bind_data->filters.exclude_terms = value;
		} else if (key == "or_terms") {
			bind_data->filters.or_terms = value;
		} else if (key == "file_type") {
			bind_data->filters.file_type = value;
		} else if (key == "country") {
			bind_data->filters.gl = value;
		} else if (key == "language") {
			bind_data->filters.language = value;
		} else if (key == "interface_language") {
			bind_data->filters.hl = value;
		} else if (key == "safe") {
			bind_data->filters.safe = value;
		} else if (key == "rights") {
			bind_data->filters.rights = value;
		} else if (key == "sort") {
			bind_data->filters.sort = value;
		} else if (key == "structured_data") {
			bind_data->filters.structured_data = value;
		}
	}

	// Set output schema - includes site and date for pushdown filtering
	bind_data->column_names = {"title",      "link",         "snippet",    "display_link",  "formatted_url",
	                           "html_formatted_url", "html_title", "html_snippet", "mime",
	                           "file_format", "pagemap",      "site",       "date"};

	for (const auto &name : bind_data->column_names) {
		names.emplace_back(name);
	}

	// All VARCHAR for now
	for (size_t i = 0; i < bind_data->column_names.size(); i++) {
		return_types.emplace_back(LogicalType::VARCHAR);
		bind_data->column_types.emplace_back(LogicalType::VARCHAR);
	}

	return std::move(bind_data);
}

// Helper: Extract pattern from LIKE (e.g., '%.google.com' -> 'google.com')
static string ExtractLikePattern(const string &pattern, bool &is_prefix, bool &is_suffix) {
	is_prefix = false;
	is_suffix = false;

	if (pattern.empty()) {
		return pattern;
	}

	string result = pattern;

	// Check for leading %
	if (result[0] == '%') {
		is_suffix = true;
		result = result.substr(1);
	}

	// Check for trailing %
	if (!result.empty() && result.back() == '%') {
		is_prefix = true;
		result = result.substr(0, result.length() - 1);
	}

	return result;
}

// Filter pushdown function
static void GoogleSearchPushdownComplexFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                                              vector<unique_ptr<Expression>> &filters) {
	auto &bind_data = bind_data_p->Cast<GoogleSearchBindData>();

	// Build column name to index map
	std::unordered_map<string, idx_t> column_map;
	for (idx_t i = 0; i < bind_data.column_names.size(); i++) {
		column_map[bind_data.column_names[i]] = i;
	}

	vector<idx_t> filters_to_remove;

	for (idx_t i = 0; i < filters.size(); i++) {
		auto &filter = filters[i];

		// Handle BOUND_FUNCTION for LIKE expressions (suffix, prefix, contains)
		if (filter->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
			auto &func = filter->Cast<BoundFunctionExpression>();

			// Handle suffix (LIKE '%string')
			if (func.function.name == "suffix" && func.children.size() >= 2) {
				if (func.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
				    func.children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
					auto &col_ref = func.children[0]->Cast<BoundColumnRefExpression>();
					auto &constant = func.children[1]->Cast<BoundConstantExpression>();

					if (col_ref.GetName() == "site" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
						string suffix = constant.value.ToString();
						// site LIKE '%.google.com' -> include google.com
						bind_data.site_includes.push_back(suffix);
						filters_to_remove.push_back(i);
						continue;
					}
				}
			}

			// Handle prefix (LIKE 'string%')
			if (func.function.name == "prefix" && func.children.size() >= 2) {
				if (func.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
				    func.children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
					auto &col_ref = func.children[0]->Cast<BoundColumnRefExpression>();
					auto &constant = func.children[1]->Cast<BoundConstantExpression>();

					if (col_ref.GetName() == "site" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
						string prefix = constant.value.ToString();
						bind_data.site_includes.push_back(prefix);
						filters_to_remove.push_back(i);
						continue;
					}
				}
			}
		}

		// Handle IN clauses (site IN ('google.com', 'microsoft.com'))
		if (filter->GetExpressionClass() == ExpressionClass::BOUND_OPERATOR) {
			auto &op = filter->Cast<BoundOperatorExpression>();

			if (op.children.size() >= 2 && op.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
				auto &col_ref = op.children[0]->Cast<BoundColumnRefExpression>();

				if (col_ref.GetName() == "site") {
					vector<string> site_values;
					bool all_constants = true;

					for (size_t j = 1; j < op.children.size(); j++) {
						if (op.children[j]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
							auto &constant = op.children[j]->Cast<BoundConstantExpression>();
							if (constant.value.type().id() == LogicalTypeId::VARCHAR) {
								site_values.push_back(constant.value.ToString());
								continue;
							}
						}
						all_constants = false;
						break;
					}

					if (all_constants && !site_values.empty()) {
						for (const auto &site : site_values) {
							bind_data.site_includes.push_back(site);
						}
						filters_to_remove.push_back(i);
						continue;
					}
				}
			}
		}

		// Handle comparison expressions
		if (filter->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
			auto &comparison = filter->Cast<BoundComparisonExpression>();

			// Check for site = 'value'
			if (filter->type == ExpressionType::COMPARE_EQUAL) {
				if (comparison.left->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
				    comparison.right->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
					auto &col_ref = comparison.left->Cast<BoundColumnRefExpression>();
					auto &constant = comparison.right->Cast<BoundConstantExpression>();

					if (col_ref.GetName() == "site" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
						bind_data.site_includes.push_back(constant.value.ToString());
						filters_to_remove.push_back(i);
						continue;
					}
				}
			}

			// Check for site != 'value' (exclude)
			if (filter->type == ExpressionType::COMPARE_NOTEQUAL) {
				if (comparison.left->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
				    comparison.right->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
					auto &col_ref = comparison.left->Cast<BoundColumnRefExpression>();
					auto &constant = comparison.right->Cast<BoundConstantExpression>();

					if (col_ref.GetName() == "site" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
						bind_data.site_excludes.push_back(constant.value.ToString());
						filters_to_remove.push_back(i);
						continue;
					}
				}
			}

			// Handle timestamp/date comparisons for dateRestrict
			if (filter->type == ExpressionType::COMPARE_GREATERTHAN ||
			    filter->type == ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
				if (comparison.left->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
				    comparison.right->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
					auto &col_ref = comparison.left->Cast<BoundColumnRefExpression>();
					auto &constant = comparison.right->Cast<BoundConstantExpression>();

					if (col_ref.GetName() == "timestamp" || col_ref.GetName() == "date") {
						timestamp_t ts_value;
						if (constant.value.type().id() == LogicalTypeId::TIMESTAMP ||
						    constant.value.type().id() == LogicalTypeId::TIMESTAMP_TZ) {
							ts_value = timestamp_t(constant.value.GetValue<int64_t>());
						} else if (constant.value.type().id() == LogicalTypeId::DATE) {
							// Convert date to timestamp
							auto date_val = constant.value.GetValue<date_t>();
							ts_value = Timestamp::FromDatetime(date_val, dtime_t(0));
						} else if (constant.value.type().id() == LogicalTypeId::VARCHAR) {
							string ts_str = constant.value.ToString();
							ts_value = Timestamp::FromString(ts_str, false);
						} else {
							continue;
						}

						bind_data.date_from = ts_value;
						bind_data.has_date_filter = true;
						// Don't remove - DuckDB still applies for exact filtering
						continue;
					}
				}
			}

			if (filter->type == ExpressionType::COMPARE_LESSTHAN ||
			    filter->type == ExpressionType::COMPARE_LESSTHANOREQUALTO) {
				if (comparison.left->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
				    comparison.right->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
					auto &col_ref = comparison.left->Cast<BoundColumnRefExpression>();
					auto &constant = comparison.right->Cast<BoundConstantExpression>();

					if (col_ref.GetName() == "timestamp" || col_ref.GetName() == "date") {
						timestamp_t ts_value;
						if (constant.value.type().id() == LogicalTypeId::TIMESTAMP ||
						    constant.value.type().id() == LogicalTypeId::TIMESTAMP_TZ) {
							ts_value = timestamp_t(constant.value.GetValue<int64_t>());
						} else if (constant.value.type().id() == LogicalTypeId::DATE) {
							auto date_val = constant.value.GetValue<date_t>();
							ts_value = Timestamp::FromDatetime(date_val, dtime_t(0));
						} else if (constant.value.type().id() == LogicalTypeId::VARCHAR) {
							string ts_str = constant.value.ToString();
							ts_value = Timestamp::FromString(ts_str, false);
						} else {
							continue;
						}

						bind_data.date_to = ts_value;
						bind_data.has_date_filter = true;
						continue;
					}
				}
			}
		}

		// Handle BETWEEN (shows as BOUND_BETWEEN)
		if (filter->type == ExpressionType::COMPARE_BETWEEN) {
			// BETWEEN is typically rewritten to >= AND <=, but just in case
			continue;
		}
	}

	// Remove pushed down filters (iterate in reverse to preserve indices)
	for (auto it = filters_to_remove.rbegin(); it != filters_to_remove.rend(); ++it) {
		filters.erase(filters.begin() + *it);
	}
}

// Global init function
static unique_ptr<GlobalTableFunctionState> GoogleSearchInitGlobal(ClientContext &context,
                                                                   TableFunctionInitInput &input) {
	auto state = make_uniq<GoogleSearchGlobalState>();
	auto &bind_data = input.bind_data->Cast<GoogleSearchBindData>();

	// Fetch all results up front
	FetchGoogleSearchResults(context, *state, bind_data);

	return std::move(state);
}

// Scan function
static void GoogleSearchScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<GoogleSearchGlobalState>();

	idx_t count = 0;
	idx_t max_count = STANDARD_VECTOR_SIZE;

	while (count < max_count && state.current_idx < state.results.size()) {
		auto &result = state.results[state.current_idx];

		output.SetValue(0, count, Value(result.title));
		output.SetValue(1, count, Value(result.link));
		output.SetValue(2, count, Value(result.snippet));
		output.SetValue(3, count, Value(result.display_link));
		output.SetValue(4, count, Value(result.formatted_url));
		output.SetValue(5, count, Value(result.html_formatted_url));
		output.SetValue(6, count, Value(result.html_title));
		output.SetValue(7, count, Value(result.html_snippet));
		output.SetValue(8, count, Value(result.mime));
		output.SetValue(9, count, Value(result.file_format));
		output.SetValue(10, count, Value(result.pagemap));
		output.SetValue(11, count, Value(result.site));
		output.SetValue(12, count, result.date.empty() ? Value() : Value(result.date));

		state.current_idx++;
		count++;
	}

	output.SetCardinality(count);
}

// LIMIT pushdown optimizer
void OptimizeGoogleSearchLimitPushdown(unique_ptr<LogicalOperator> &op) {
	if (op->type == LogicalOperatorType::LOGICAL_LIMIT) {
		auto &limit = op->Cast<LogicalLimit>();
		reference<LogicalOperator> child = *op->children[0];

		// Skip projection operators to find the GET
		while (child.get().type == LogicalOperatorType::LOGICAL_PROJECTION) {
			child = *child.get().children[0];
		}

		if (child.get().type != LogicalOperatorType::LOGICAL_GET) {
			OptimizeGoogleSearchLimitPushdown(op->children[0]);
			return;
		}

		auto &get = child.get().Cast<LogicalGet>();
		if (get.function.name != "google_search") {
			OptimizeGoogleSearchLimitPushdown(op->children[0]);
			return;
		}

		switch (limit.limit_val.Type()) {
		case LimitNodeType::CONSTANT_VALUE:
		case LimitNodeType::UNSET:
			break;
		default:
			OptimizeGoogleSearchLimitPushdown(op->children[0]);
			return;
		}

		auto &bind_data = get.bind_data->Cast<GoogleSearchBindData>();
		if (limit.limit_val.Type() == LimitNodeType::CONSTANT_VALUE) {
			auto limit_value = limit.limit_val.GetConstantValue();
			// Cap at 100 (Google API max)
			bind_data.max_results = std::min(limit_value, (idx_t)100);
		}
		return;
	}

	// Recurse into children
	for (auto &child : op->children) {
		OptimizeGoogleSearchLimitPushdown(child);
	}
}

// ORDER BY pushdown optimizer - converts ORDER BY date to API sort parameter
void OptimizeGoogleSearchOrderByPushdown(unique_ptr<LogicalOperator> &op) {
	if (op->type == LogicalOperatorType::LOGICAL_ORDER_BY) {
		auto &order = op->Cast<LogicalOrder>();
		reference<LogicalOperator> child = *op->children[0];

		// Skip projection operators to find the GET
		while (child.get().type == LogicalOperatorType::LOGICAL_PROJECTION) {
			child = *child.get().children[0];
		}

		if (child.get().type != LogicalOperatorType::LOGICAL_GET) {
			OptimizeGoogleSearchOrderByPushdown(op->children[0]);
			return;
		}

		auto &get = child.get().Cast<LogicalGet>();
		if (get.function.name != "google_search") {
			OptimizeGoogleSearchOrderByPushdown(op->children[0]);
			return;
		}

		// Check if ordering by a single column
		if (order.orders.size() != 1) {
			OptimizeGoogleSearchOrderByPushdown(op->children[0]);
			return;
		}

		auto &order_node = order.orders[0];
		auto &expr = order_node.expression;

		// Check if it's a column reference
		if (expr->type != ExpressionType::BOUND_COLUMN_REF) {
			OptimizeGoogleSearchOrderByPushdown(op->children[0]);
			return;
		}

		auto &col_ref = expr->Cast<BoundColumnRefExpression>();

		// Get column name from bind data
		auto &bind_data = get.bind_data->Cast<GoogleSearchBindData>();
		idx_t col_idx = col_ref.binding.column_index;

		if (col_idx >= bind_data.column_names.size()) {
			OptimizeGoogleSearchOrderByPushdown(op->children[0]);
			return;
		}

		string col_name = bind_data.column_names[col_idx];

		// Map column name to Google sort parameter
		string sort_param;
		if (col_name == "date") {
			// Map to Google's date sort (estimated page date)
			// See: https://developers.google.com/custom-search/docs/structured_search
			if (order_node.type == OrderType::DESCENDING) {
				sort_param = "date:d";
			} else {
				sort_param = "date:a";
			}
		} else {
			// Column not supported for ORDER BY pushdown
			OptimizeGoogleSearchOrderByPushdown(op->children[0]);
			return;
		}

		// Set the sort parameter (only if not already set via named param)
		if (bind_data.filters.sort.empty()) {
			bind_data.filters.sort = sort_param;
		}

		return;
	}

	// Recurse into children
	for (auto &child : op->children) {
		OptimizeGoogleSearchOrderByPushdown(child);
	}
}

// Register the table function
void RegisterGoogleSearchFunction(ExtensionLoader &loader) {
	TableFunction google_search_func("google_search", {LogicalType::VARCHAR}, GoogleSearchScan, GoogleSearchBind,
	                                 GoogleSearchInitGlobal);

	// Enable filter pushdown
	google_search_func.pushdown_complex_filter = GoogleSearchPushdownComplexFilter;

	// Named parameters for non-pushdown filters
	google_search_func.named_parameters["exact_terms"] = LogicalType::VARCHAR;
	google_search_func.named_parameters["exclude_terms"] = LogicalType::VARCHAR;
	google_search_func.named_parameters["or_terms"] = LogicalType::VARCHAR;
	google_search_func.named_parameters["file_type"] = LogicalType::VARCHAR;
	google_search_func.named_parameters["country"] = LogicalType::VARCHAR;
	google_search_func.named_parameters["language"] = LogicalType::VARCHAR;
	google_search_func.named_parameters["interface_language"] = LogicalType::VARCHAR;
	google_search_func.named_parameters["safe"] = LogicalType::VARCHAR;
	google_search_func.named_parameters["rights"] = LogicalType::VARCHAR;
	google_search_func.named_parameters["sort"] = LogicalType::VARCHAR;
	google_search_func.named_parameters["structured_data"] = LogicalType::VARCHAR;

	loader.RegisterFunction(google_search_func);
}

} // namespace duckdb
