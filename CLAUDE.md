# DuckDB Google Search Extension

## Overview
DuckDB extension providing `google_search()` and `google_image_search()` table functions for Google Custom Search API.

## Build
```bash
make release GEN=ninja VCPKG_TOOLCHAIN_PATH=$(pwd)/vcpkg/scripts/buildsystems/vcpkg.cmake
```

## Usage
```sql
LOAD 'build/release/extension/google_search/google_search.duckdb_extension';
CREATE SECRET google_search (TYPE google_search, key 'API_KEY', cx 'SEARCH_ENGINE_ID');
SELECT * FROM google_search('query') LIMIT 10;
```

## API Requirements

### Pagination
- Google API returns max 10 results per request (`num=10`)
- Use `start` parameter for pagination: `start=1`, `start=11`, `start=21`, etc.
- Parse `queries.nextPage[0].startIndex` from response for next page
- Max 100 results total per query (Google API limit)

### Site Filtering Modes

#### LIMIT <= 100 (single query mode)
Use `(site:a.com OR site:b.com)` syntax in query string:
```
q=programming (site:github.com OR site:stackoverflow.com) -site:spam.com
```

#### LIMIT > 100 (per-site query mode)
Make separate API calls for each site using `siteSearch` parameter:
```
siteSearch=github.com&siteSearchFilter=i&q=programming -site:spam.com
siteSearch=stackoverflow.com&siteSearchFilter=i&q=programming -site:spam.com
```
Round-robin between sites. Each site can return up to 100 results.

### Exclusions
Site exclusions (`site != 'spam.com'`) are added to query string as `-site:spam.com`.
Exclusions apply to ALL API calls in both modes.

## SQL Filter Pushdown

### Supported Filters
- `site = 'github.com'` → single site query
- `site IN ('a.com', 'b.com')` → multi-site (OR mode or per-site mode based on LIMIT)
- `site != 'spam.com'` → exclusion via `-site:spam.com`
- `site LIKE '%.github.com'` → suffix matching (pushed to site filter)
- LIMIT pushdown → reduces API calls

### Named Parameters
Additional filters via named parameters:
- `exact_terms`, `exclude_terms`, `or_terms`
- `file_type`, `country`, `language`, `safe`, `rights`, `sort`

## Image Search
`google_image_search()` returns image-specific columns:
- `image_url`, `thumbnail_url`, `width`, `height`, `mime`, etc.
- Additional filters: `img_size`, `img_type`, `img_color_type`, `img_dominant_color`

## JSON Columns
The `pagemap` column is JSON type. Use arrow operators:
```sql
pagemap->'product'->0->>'name'  -- get string
pagemap->'offer' IS NOT NULL    -- check existence
```

**NEVER use LIKE on JSON/structured data columns.** Use proper JSON operators instead.
