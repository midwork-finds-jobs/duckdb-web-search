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
| date | VARCHAR | Page date (NULL, for ORDER BY pushdown) |

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
| country | Country boost (gl) | `country:='us'` |
| cr | Country restrict | `cr:='countryUS'` |
| lr | Language restrict | `lr:='lang_en'` |
| language | Language restrict (alias) | `language:='lang_en'` |
| safe | Safe search | `safe:=true` |
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

## Sorting by Date

Use `ORDER BY date` to sort results by Google's estimated page date:

```sql
-- Sort by date (newest first)
SELECT title, link FROM google_search('technology news')
ORDER BY date DESC LIMIT 10;

-- Sort by date (oldest first)
SELECT title, link FROM google_search('historical events')
ORDER BY date ASC LIMIT 10;
```

The `ORDER BY date` clause is pushed down to the Google API (`&sort=date`), affecting which results are returned.

**Note:** The `date` column is NULL in results - Google estimates the date from page features (URL, title, byline) for sorting but doesn't return it in the API response.

### Date Biasing and Range Filtering

For more control, use the `sort` parameter directly:

```sql
-- Bias strongly towards newer dates (keeps older results too)
SELECT * FROM google_search('oil spill', sort:='date:d:s') LIMIT 10;

-- Bias weakly towards older dates
SELECT * FROM google_search('history', sort:='date:a:w') LIMIT 10;

-- Date range: Jan 1 to Feb 1, 2024
SELECT * FROM google_search('news', sort:='date:r:20240101:20240201') LIMIT 10;
```

## Structured Data Search

Google Custom Search can filter results based on structured data embedded in web pages (PageMaps, meta tags, JSON-LD, Microdata, RDFa).

See: https://developers.google.com/custom-search/docs/structured_search

### Filtering by Structured Data

Use the `structured_data` parameter to filter by pagemap attributes:

```sql
-- Filter by author
SELECT * FROM google_search('machine learning',
    structured_data:='more:pagemap:document-author:smith')
LIMIT 10;

-- Filter by metatag
SELECT * FROM google_search('news',
    structured_data:='more:pagemap:metatags-og\\:type:article')
LIMIT 10;
```

Filter syntax: `more:pagemap:TYPE-NAME:VALUE`

### Advanced Sorting (sort parameter)

For advanced sorting beyond date, use the `sort` named parameter:

```sql
-- Sort by rating with strong bias (keeps results without rating)
SELECT * FROM google_search('product reviews',
    sort:='review-rating:d:s')
LIMIT 20;

-- Filter by date range (YYYYMMDD format)
SELECT * FROM google_search('news',
    sort:='date:r:20240101:20240331')
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
| `:h` | Hard sort (default) |
| `:s` | Strong bias |
| `:w` | Weak bias |

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
