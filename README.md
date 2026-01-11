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

### Image-Specific Parameters

| Parameter | Values |
|-----------|--------|
| img_size | `huge`, `icon`, `large`, `medium`, `small`, `xlarge`, `xxlarge` |
| img_type | `clipart`, `face`, `lineart`, `stock`, `photo`, `animated` |
| img_color_type | `color`, `gray`, `mono`, `trans` |
| img_dominant_color | `black`, `blue`, `brown`, `gray`, `green`, `orange`, `pink`, `purple`, `red`, `teal`, `white`, `yellow` |

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
