# DuckDB Web Search Extension

## Overview

DuckDB extension providing `google_search()` and `google_image_search()` table functions for Google Custom Search API.

## Build

```bash
make release GEN=ninja VCPKG_TOOLCHAIN_PATH=$(pwd)/vcpkg/scripts/buildsystems/vcpkg.cmake
```

## Usage

```sql
LOAD 'build/release/extension/google_search/web_search.duckdb_extension';
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

```text
q=programming (site:github.com OR site:stackoverflow.com) -site:spam.com
```

#### LIMIT > 100 (per-site query mode)

Make separate API calls for each site using `siteSearch` parameter:

```text
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

## Structured Data & Pagemap

### Google's JSON-LD Extraction Limits

Google Custom Search only extracts these schema.org **type trees** from JSON-LD:

- **Event** (includes nested AggregateRating, etc.)
- **ClaimReview**
- **EducationalOrganization**

Nested properties within these types are extracted. Example filter for Event with rating:

```text
more:pagemap:aggregaterating-itemreviewed-event-startdate:2022-05-24
```

**Product, Offer, Article, etc. are NOT extracted.** Don't try to filter by product availability or price - it won't work.

### What's Actually in Pagemap

Pagemap typically contains only:

- `metatags` - OpenGraph, Twitter cards, meta tags
- `cse_image` - Main page image
- `cse_thumbnail` - Thumbnail image

### Structured Data Filtering (Query-Time)

Use `structured_data` parameter with syntax: `more:pagemap:TYPE-NAME:VALUE`

```sql
-- Filter by metatag (works)
structured_data:='more:pagemap:metatags-og\\:type:article'

-- Filter by aggregaterating (works for Event type)
structured_data:='more:pagemap:aggregaterating-ratingcount:22'
```

### Sorting by Structured Data

Use `sort` parameter: `TYPE-NAME:DIRECTION:STRENGTH`

- Direction: `:d` (desc), `:a` (asc), `:r:LOW:HIGH` (range)
- Strength: `:h` (hard), `:s` (strong bias), `:w` (weak bias)

```sql
sort:='review-rating:d:s'           -- bias toward higher ratings
sort:='date:r:20240101:20240331'    -- date range filter
```

### JSON Column Access

The `pagemap` column is JSON type. Use arrow operators:

```sql
pagemap->'metatags'->0->>'og:title'  -- get string
pagemap->'cse_image' IS NOT NULL     -- check existence
```

**NEVER use LIKE on JSON/structured data columns.** Use proper JSON operators instead.
