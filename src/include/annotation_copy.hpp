#pragma once

#include "duckdb.hpp"

namespace duckdb {

// Register the google_pse_annotation COPY function
void RegisterAnnotationCopyFunction(ExtensionLoader &loader);

} // namespace duckdb
