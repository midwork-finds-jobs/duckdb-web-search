# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(web_search
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)
# Link into the test binaries: ci-tools v2.0 no longer does, so `require web_search` tests would be skipped
duckdb_extension_statically_link(web_search)

# Any extra extensions that should be built
# e.g.: duckdb_extension_load(json)
