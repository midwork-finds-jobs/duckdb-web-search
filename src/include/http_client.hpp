#pragma once

#include "duckdb.hpp"
#include <string>

namespace duckdb {

struct HttpResponse {
	int status_code = 0;
	std::string body;
	std::string content_type;
	std::string retry_after;
	std::string error;
	bool success = false;
};

struct RetryConfig {
	int max_retries = 3;
	int initial_backoff_ms = 100;
	double backoff_multiplier = 2.0;
	int max_backoff_ms = 10000;
};

class HttpClient {
public:
	static HttpResponse Fetch(ClientContext &context, const std::string &url, const RetryConfig &config);

private:
	static HttpResponse ExecuteHttpGet(DatabaseInstance &db, const std::string &url);
	static bool IsRetryable(int status_code);
	static int ParseRetryAfter(const std::string &retry_after);
};

// URL encoding helper
std::string UrlEncode(const std::string &value);

} // namespace duckdb
