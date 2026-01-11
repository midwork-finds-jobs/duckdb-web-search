-- Load the google_search extension
LOAD 'build/release/extension/google_search/google_search.duckdb_extension';

-- Create secret with API credentials
CREATE SECRET google_search (TYPE google_search, key 'AIzaSyDAutDm-wLe-sEdDihDgVakSuqUcQnvl8g', cx 'f0fe080a20a944924');

-- Basic search (10 results = 1 API call)
SELECT title, link, site FROM google_search('elixir') LIMIT 5;

-- Single site filter
SELECT title, link FROM google_search('programming') WHERE site = 'github.com' LIMIT 10;

-- Multi-site LIMIT <= 100: single query with (site:a OR site:b) syntax
-- Query: q=programming (site:github.com OR site:stackoverflow.com)
SELECT site, COUNT(*) as count FROM (
  SELECT * FROM google_search('programming')
  WHERE site IN ('github.com', 'stackoverflow.com')
  LIMIT 30
) GROUP BY site ORDER BY count DESC;

-- Exclusion: -site:spam.com added to query string
SELECT site, COUNT(*) as count FROM (
  SELECT * FROM google_search('programming reddit')
  WHERE site != 'reddit.com'
  LIMIT 20
) GROUP BY site ORDER BY count DESC;

-- Complex filter: exclusion applies to all queries
SELECT site, COUNT(*) as count FROM (
  SELECT * FROM google_search('code')
  WHERE site != 'gist.github.com' AND site IN ('github.com', 'stackoverflow.com')
  LIMIT 30
) GROUP BY site ORDER BY count DESC;

-- Pagination: 25 results = 3 API calls (10+10+5)
SELECT title, link FROM google_search('duckdb database') LIMIT 25;

-- Image search
SELECT title, image_url, width, height FROM google_image_search('elixir programming') LIMIT 15;
