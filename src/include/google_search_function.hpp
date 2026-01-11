#pragma once

#include "duckdb.hpp"

namespace duckdb {

// Register the google_search() table function
void RegisterGoogleSearchFunction(ExtensionLoader &loader);

// LIMIT pushdown optimizer for google_search
void OptimizeGoogleSearchLimitPushdown(unique_ptr<LogicalOperator> &op);

// ORDER BY pushdown optimizer for google_search (ORDER BY date -> &sort=date-sdate)
void OptimizeGoogleSearchOrderByPushdown(unique_ptr<LogicalOperator> &op);

// Filter parameters for Google Search API
struct GoogleSearchFilters {
	// Text filters
	string exact_terms;   // exactTerms - phrase that must appear
	string exclude_terms; // excludeTerms - words to exclude
	string or_terms;      // orTerms - alternative terms

	// Date filter
	string date_restrict; // dateRestrict (d5, w2, m1, y1)

	// File type
	string file_type; // fileType (pdf, doc, etc.)

	// Site filters
	string site_search;        // siteSearch
	string site_search_filter; // siteSearchFilter (e=exclude, i=include)

	// Language/region
	string gl;       // geolocation boost
	string hl;       // host language
	string language; // lr - document language (lang_en, lang_de, etc.)

	// Safety
	string safe; // SafeSearch (active, off)

	// Rights
	string rights; // Creative Commons license

	// Sort/bias for structured data (e.g., "date-sdate:d", "review-rating:d:s")
	string sort;

	// Structured data filter (prepended to query, e.g., "more:pagemap:document-author:john")
	string structured_data;
};

} // namespace duckdb
