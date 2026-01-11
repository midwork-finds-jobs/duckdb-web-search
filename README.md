# DuckDB Google Search Extension

DuckDB extension for querying Google Custom Search API directly from SQL.

## Installation

```sql
-- Install from community extensions (when published)
INSTALL google_search FROM community;
LOAD google_search;
```

## Setup

Create a Google Custom Search API secret:

```sql
CREATE SECRET google_search (
  TYPE google_search,
  key 'YOUR_API_KEY',
  cx 'YOUR_SEARCH_ENGINE_ID'
);
```

Get your credentials at:
- API Key: https://console.cloud.google.com/apis/credentials
- Search Engine ID (cx): https://programmablesearchengine.google.com/

## Usage

### Web Search

```sql
-- Basic search
SELECT title, link, snippet FROM google_search('duckdb database') LIMIT 10;

-- Filter by site
SELECT * FROM google_search('programming') WHERE site = 'github.com' LIMIT 20;

-- Multiple sites
SELECT * FROM google_search('rust tutorial')
WHERE site IN ('github.com', 'stackoverflow.com', 'reddit.com')
LIMIT 50;

-- Exclude sites
SELECT * FROM google_search('python')
WHERE site != 'pinterest.com' AND site != 'quora.com'
LIMIT 30;

-- Combined filters
SELECT title, link, site FROM google_search('machine learning')
WHERE site IN ('arxiv.org', 'github.com') AND site != 'gist.github.com'
LIMIT 100;
```

### Image Search

```sql
SELECT title, image_url, width, height
FROM google_image_search('sunset photography')
LIMIT 20;

-- With image filters
SELECT * FROM google_image_search('logo design', img_size:='large', img_type:='clipart')
LIMIT 10;
```

## Output Columns

### google_search()

| Column | Type | Description |
|--------|------|-------------|
| title | VARCHAR | Page title |
| link | VARCHAR | Page URL |
| snippet | VARCHAR | Text snippet |
| display_link | VARCHAR | Display URL |
| formatted_url | VARCHAR | Formatted URL |
| html_title | VARCHAR | HTML-formatted title |
| html_snippet | VARCHAR | HTML-formatted snippet |
| mime | VARCHAR | MIME type (for files) |
| file_format | VARCHAR | File format |
| pagemap | VARCHAR | Page metadata (JSON) |
| site | VARCHAR | Domain (extracted from link) |

### google_image_search()

| Column | Type | Description |
|--------|------|-------------|
| title | VARCHAR | Image title |
| link | VARCHAR | Page URL |
| image_url | VARCHAR | Direct image URL |
| thumbnail_url | VARCHAR | Thumbnail URL |
| width | INTEGER | Image width |
| height | INTEGER | Image height |
| thumbnail_width | INTEGER | Thumbnail width |
| thumbnail_height | INTEGER | Thumbnail height |
| context_link | VARCHAR | Page containing image |
| mime | VARCHAR | Image MIME type |
| snippet | VARCHAR | Description |

## Named Parameters

### Common Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| exact_terms | Exact phrase match | `exact_terms:='machine learning'` |
| exclude_terms | Exclude words | `exclude_terms:='beginner'` |
| or_terms | OR terms | `or_terms:='python rust'` |
| file_type | File type filter | `file_type:='pdf'` |
| country | Country (gl param) | `country:='us'` |
| language | Language (lr param) | `language:='lang_en'` |
| safe | Safe search | `safe:='active'` |
| rights | Usage rights | `rights:='cc_publicdomain'` |
| sort | Sort/bias by structured data | `sort:='date-sdate:d'` |
| structured_data | Filter by pagemap | `structured_data:='more:pagemap:document-author:john'` |

### Image-Specific Parameters

| Parameter | Values |
|-----------|--------|
| img_size | `huge`, `icon`, `large`, `medium`, `small`, `xlarge`, `xxlarge` |
| img_type | `clipart`, `face`, `lineart`, `stock`, `photo`, `animated` |
| img_color_type | `color`, `gray`, `mono`, `trans` |
| img_dominant_color | `black`, `blue`, `brown`, `gray`, `green`, `orange`, `pink`, `purple`, `red`, `teal`, `white`, `yellow` |

## Structured Data Search

Google Custom Search can filter and sort results based on structured data embedded in web pages (PageMaps, meta tags, JSON-LD, Microdata, RDFa).

See: https://developers.google.com/custom-search/docs/structured_search

### Filtering by Structured Data

Use the `structured_data` parameter to filter by pagemap attributes:

```sql
-- Filter by author
SELECT * FROM google_search('machine learning',
    structured_data:='more:pagemap:document-author:smith')
LIMIT 10;

-- Filter by any document with keywords
SELECT * FROM google_search('programming',
    structured_data:='more:pagemap:document-keywords:python')
LIMIT 10;

-- Filter by metatag
SELECT * FROM google_search('news',
    structured_data:='more:pagemap:metatags-og\\:type:article')
LIMIT 10;
```

Filter syntax: `more:pagemap:TYPE-NAME:VALUE`
- Omit VALUE to match any instance of a field
- Text values are tokenized (split into words)
- Use `*` to combine multiple tokens: `more:p:document-keywords:irish*fiction`

### Sorting by Structured Data

Use the `sort` parameter to sort results by pagemap attributes:

```sql
-- Sort by date (descending)
SELECT * FROM google_search('technology news',
    sort:='date-sdate:d')
LIMIT 20;

-- Sort by date (ascending)
SELECT * FROM google_search('historical events',
    sort:='date-sdate:a')
LIMIT 20;

-- Sort by rating with strong bias (keeps results without rating)
SELECT * FROM google_search('product reviews',
    sort:='review-rating:d:s')
LIMIT 20;

-- Filter by date range (YYYYMMDD format)
SELECT * FROM google_search('news',
    sort:='date-sdate:r:20240101:20240331')
LIMIT 20;
```

Sort syntax: `TYPE-NAME:DIRECTION:STRENGTH`

| Direction | Meaning |
|-----------|---------|
| `:d` | Descending (default) |
| `:a` | Ascending |
| `:r:LOW:HIGH` | Range filter |

| Strength | Meaning |
|----------|---------|
| `:h` | Hard sort (default, excludes results without attribute) |
| `:s` | Strong bias (promotes but doesn't exclude) |
| `:w` | Weak bias |

### Common Structured Data Types

| Type | Description | Example |
|------|-------------|---------|
| `date-sdate` | Page date | `sort:='date-sdate:d'` |
| `metatags-*` | Meta tags | `more:pagemap:metatags-author:john` |
| `document-*` | Document properties | `more:pagemap:document-keywords:python` |
| `review-rating` | Review ratings | `sort:='review-rating:d:s'` |
| `product-price` | Product prices | `sort:='product-price:a'` |

### Note on SQL ORDER BY

SQL `ORDER BY` on result columns (title, link, etc.) sorts already-fetched results locally.
Google's `sort` parameter affects which results the API returns first.

For API-level sorting, use the `sort` named parameter instead of SQL `ORDER BY`.

## API Behavior

### Pagination
- Google returns max 10 results per API call
- Extension automatically paginates to fulfill LIMIT
- Max 100 results per query (Google API limit)

### Multi-Site Queries
- **LIMIT ≤ 100**: Single query with `(site:a OR site:b)` syntax
- **LIMIT > 100**: Separate queries per site (up to 100 each), round-robin

### Filter Pushdown
Site filters in WHERE clause are pushed down to the API:
- `site = 'x.com'` → `siteSearch=x.com`
- `site IN (...)` → OR syntax or per-site queries
- `site != 'x.com'` → `-site:x.com` in query

LIMIT is pushed down to minimize API calls.

## Building from Source

```bash
# Clone with submodules
git clone --recursive https://github.com/midwork-finds-jobs/duckdb-search.git
cd duckdb-search

# Build
make release GEN=ninja VCPKG_TOOLCHAIN_PATH=$(pwd)/vcpkg/scripts/buildsystems/vcpkg.cmake

# Test
./build/release/duckdb -c "LOAD 'build/release/extension/google_search/google_search.duckdb_extension';"
```

## Dependencies

- DuckDB (via git submodule)
- [http_request](https://community-extensions.duckdb.org/extensions/http_request.html) extension (auto-loaded)

## License

MIT
