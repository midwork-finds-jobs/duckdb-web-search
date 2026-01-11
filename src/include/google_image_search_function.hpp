#pragma once

#include "duckdb.hpp"

namespace duckdb {

// Register the google_image_search() table function
void RegisterGoogleImageSearchFunction(ExtensionLoader &loader);

// LIMIT pushdown optimizer for google_image_search
void OptimizeGoogleImageSearchLimitPushdown(unique_ptr<LogicalOperator> &op);

// Image-specific filters
struct GoogleImageSearchFilters {
	// Inherited from base search
	string exact_terms;
	string exclude_terms;
	string date_restrict;
	string site_search;
	string safe;
	string rights;

	// Image-specific
	string img_size;           // imgSize: huge/icon/large/medium/small/xlarge/xxlarge
	string img_type;           // imgType: clipart/face/lineart/stock/photo/animated
	string img_color_type;     // imgColorType: color/gray/mono/trans
	string img_dominant_color; // imgDominantColor: black/blue/brown/etc.
};

} // namespace duckdb
