-- Sample: Search fiksuruoka.fi for products with structured data
--
-- Load extension (json extension is autoloaded)
LOAD 'build/release/extension/google_search/google_search.duckdb_extension';

-- Setup API credentials
CREATE SECRET IF NOT EXISTS google_search (
    TYPE google_search,
    key 'YOUR_API_KEY',
    cx 'YOUR_SEARCH_ENGINE_ID'
);

-- Search for product pages on fiksuruoka.fi
-- Google returns structured data (JSON-LD, microdata) in the pagemap column
-- Since pagemap is JSON type, use arrow syntax: -> for JSON, ->> for string
SELECT
    title,
    link,
    snippet,
    -- The pagemap contains structured data extracted by Google
    -- Common paths: product[0], offer[0], metatags[0]
    pagemap->'product'->0->>'name' AS product_name,
    pagemap->'offer'->0->>'price' AS price,
    pagemap->'offer'->0->>'pricecurrency' AS currency,
    pagemap->'offer'->0->>'availability' AS availability
FROM google_search('tuote')
WHERE site = 'www.fiksuruoka.fi'
LIMIT 20;

-- Alternative: Get raw pagemap to inspect structure
-- SELECT title, link, pagemap
-- FROM google_search('tuote')
-- WHERE site = 'www.fiksuruoka.fi'
-- LIMIT 5;

-- Filter for pages with product structured data
-- SELECT title, link, pagemap
-- FROM google_search('tuote')
-- WHERE site = 'www.fiksuruoka.fi'
--   AND pagemap->'product' IS NOT NULL
-- LIMIT 20;
