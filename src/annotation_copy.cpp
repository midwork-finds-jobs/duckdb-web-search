#include "annotation_copy.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include <mutex>
#include <cstdio>

namespace duckdb {

// Limits from Google PSE documentation
static constexpr idx_t MAX_ANNOTATIONS = 5000;
static constexpr idx_t MAX_FILE_SIZE_BYTES = 30 * 1024; // 30KB

// Column indices
static constexpr idx_t COL_URL_PATTERN = 0;
static constexpr idx_t COL_ACTION = 1;
static constexpr idx_t COL_COMMENT = 2;
static constexpr idx_t COL_SCORE = 3;

// Bind data
struct AnnotationCopyBindData : public FunctionData {
	idx_t url_pattern_idx = 0;
	idx_t action_idx = 1;
	idx_t comment_idx = DConstants::INVALID_INDEX;
	idx_t score_idx = DConstants::INVALID_INDEX;
	bool has_comment = false;
	bool has_score = false;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<AnnotationCopyBindData>();
		result->url_pattern_idx = url_pattern_idx;
		result->action_idx = action_idx;
		result->comment_idx = comment_idx;
		result->score_idx = score_idx;
		result->has_comment = has_comment;
		result->has_score = has_score;
		return std::move(result);
	}

	bool Equals(const FunctionData &other) const override {
		auto &o = other.Cast<AnnotationCopyBindData>();
		return url_pattern_idx == o.url_pattern_idx && action_idx == o.action_idx && comment_idx == o.comment_idx &&
		       score_idx == o.score_idx && has_comment == o.has_comment && has_score == o.has_score;
	}
};

// Global state
struct AnnotationCopyGlobalState : public GlobalFunctionData {
	unique_ptr<FileHandle> handle;
	mutex lock;
	idx_t annotation_count = 0;
	idx_t bytes_written = 0;
};

// Local state
struct AnnotationCopyLocalState : public LocalFunctionData {};

// XML escape special characters
static string XmlEscape(const string &input) {
	string result;
	result.reserve(input.size());
	for (char c : input) {
		switch (c) {
		case '&':
			result += "&amp;";
			break;
		case '<':
			result += "&lt;";
			break;
		case '>':
			result += "&gt;";
			break;
		case '"':
			result += "&quot;";
			break;
		case '\'':
			result += "&apos;";
			break;
		default:
			result += c;
		}
	}
	return result;
}

// Bind function
static unique_ptr<FunctionData> AnnotationCopyBind(ClientContext &context, CopyFunctionBindInput &input,
                                                   const vector<string> &names, const vector<LogicalType> &sql_types) {
	auto result = make_uniq<AnnotationCopyBindData>();

	// Validate column count (2-4 columns)
	if (sql_types.size() < 2 || sql_types.size() > 4) {
		throw BinderException(
		    "google_pse_annotation format requires 2-4 columns:\n"
		    "  (url_pattern VARCHAR, action VARCHAR [, comment VARCHAR] [, score DOUBLE])");
	}

	// First column: url_pattern (VARCHAR)
	if (sql_types[COL_URL_PATTERN].id() != LogicalTypeId::VARCHAR) {
		throw BinderException("First column (url_pattern) must be VARCHAR");
	}
	result->url_pattern_idx = COL_URL_PATTERN;

	// Second column: action (VARCHAR - 'include' or 'exclude')
	if (sql_types[COL_ACTION].id() != LogicalTypeId::VARCHAR) {
		throw BinderException("Second column (action) must be VARCHAR ('include' or 'exclude')");
	}
	result->action_idx = COL_ACTION;

	// Third column (optional): comment (VARCHAR)
	if (sql_types.size() >= 3) {
		if (sql_types[COL_COMMENT].id() != LogicalTypeId::VARCHAR) {
			throw BinderException("Third column (comment) must be VARCHAR");
		}
		result->comment_idx = COL_COMMENT;
		result->has_comment = true;
	}

	// Fourth column (optional): score (DOUBLE, range -1.0 to 1.0)
	if (sql_types.size() == 4) {
		auto score_type = sql_types[COL_SCORE].id();
		if (score_type != LogicalTypeId::DOUBLE && score_type != LogicalTypeId::FLOAT &&
		    score_type != LogicalTypeId::DECIMAL && score_type != LogicalTypeId::INTEGER) {
			throw BinderException("Fourth column (score) must be numeric (DOUBLE recommended, range -1.0 to 1.0)");
		}
		result->score_idx = COL_SCORE;
		result->has_score = true;
	}

	return std::move(result);
}

// Initialize global state
static unique_ptr<GlobalFunctionData> AnnotationCopyInitializeGlobal(ClientContext &context, FunctionData &bind_data,
                                                                     const string &file_path) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto flags = FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW;
	auto handle = fs.OpenFile(file_path, flags);

	auto result = make_uniq<AnnotationCopyGlobalState>();
	result->handle = std::move(handle);

	// Write XML header
	string header = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Annotations>\n";
	result->handle->Write((void *)header.c_str(), header.size());
	result->bytes_written = header.size();

	return std::move(result);
}

// Initialize local state
static unique_ptr<LocalFunctionData> AnnotationCopyInitializeLocal(ExecutionContext &context, FunctionData &bind_data) {
	return make_uniq<AnnotationCopyLocalState>();
}

// Sink function - write each row
static void AnnotationCopySink(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                               LocalFunctionData &lstate, DataChunk &input) {
	auto &bdata = bind_data.Cast<AnnotationCopyBindData>();
	auto &state = gstate.Cast<AnnotationCopyGlobalState>();
	lock_guard<mutex> glock(state.lock);

	// Get column data
	UnifiedVectorFormat url_data, action_data, comment_data, score_data;
	input.data[bdata.url_pattern_idx].ToUnifiedFormat(input.size(), url_data);
	input.data[bdata.action_idx].ToUnifiedFormat(input.size(), action_data);

	auto urls = UnifiedVectorFormat::GetData<string_t>(url_data);
	auto actions = UnifiedVectorFormat::GetData<string_t>(action_data);

	const string_t *comments = nullptr;
	if (bdata.has_comment) {
		input.data[bdata.comment_idx].ToUnifiedFormat(input.size(), comment_data);
		comments = UnifiedVectorFormat::GetData<string_t>(comment_data);
	}

	// Score column needs special handling - cast to double
	Vector score_double(LogicalType::DOUBLE);
	if (bdata.has_score) {
		VectorOperations::Cast(context.client, input.data[bdata.score_idx], score_double, input.size());
		score_double.ToUnifiedFormat(input.size(), score_data);
	}

	for (idx_t row_idx = 0; row_idx < input.size(); row_idx++) {
		// Check annotation count limit
		if (state.annotation_count >= MAX_ANNOTATIONS) {
			throw InvalidInputException(
			    "Google PSE annotation limit exceeded: maximum %d annotations allowed", MAX_ANNOTATIONS);
		}

		auto url_idx = url_data.sel->get_index(row_idx);
		auto action_idx = action_data.sel->get_index(row_idx);

		// Skip null rows
		if (!url_data.validity.RowIsValid(url_idx) || !action_data.validity.RowIsValid(action_idx)) {
			continue;
		}

		string url_pattern = urls[url_idx].GetString();
		string action = StringUtil::Lower(actions[action_idx].GetString());

		// Validate action
		if (action != "include" && action != "exclude") {
			throw InvalidInputException(
			    "Invalid action '%s'. Must be 'include' or 'exclude'", action);
		}

		string label_name = (action == "include") ? "_include_" : "_exclude_";

		// Get score if present
		string score_attr = "";
		if (bdata.has_score) {
			auto score_idx_sel = score_data.sel->get_index(row_idx);
			if (score_data.validity.RowIsValid(score_idx_sel)) {
				auto scores = UnifiedVectorFormat::GetData<double>(score_data);
				double score = scores[score_idx_sel];
				// Validate score range
				if (score < -1.0 || score > 1.0) {
					throw InvalidInputException(
					    "Invalid score %.2f. Must be between -1.0 and 1.0", score);
				}
				// Format score with 1 decimal place
				char score_buf[32];
				snprintf(score_buf, sizeof(score_buf), "%.1f", score);
				score_attr = " score=\"" + string(score_buf) + "\"";
			}
		}

		// Build annotation XML
		string xml = "  <Annotation about=\"" + XmlEscape(url_pattern) + "\"" + score_attr + ">\n";
		xml += "    <Label name=\"" + label_name + "\"/>\n";

		// Add comment if present
		if (bdata.has_comment) {
			auto comment_idx_sel = comment_data.sel->get_index(row_idx);
			if (comment_data.validity.RowIsValid(comment_idx_sel)) {
				string comment = comments[comment_idx_sel].GetString();
				if (!comment.empty()) {
					xml += "    <Comment>" + XmlEscape(comment) + "</Comment>\n";
				}
			}
		}

		xml += "  </Annotation>\n";

		// Check file size limit before writing
		if (state.bytes_written + xml.size() + 15 > MAX_FILE_SIZE_BYTES) { // 15 for closing tag
			throw InvalidInputException(
			    "Google PSE annotation file size limit exceeded: maximum %d bytes allowed", MAX_FILE_SIZE_BYTES);
		}

		state.handle->Write((void *)xml.c_str(), xml.size());
		state.bytes_written += xml.size();
		state.annotation_count++;
	}
}

// Combine function
static void AnnotationCopyCombine(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                                  LocalFunctionData &lstate) {
	// Nothing to combine
}

// Finalize function
static void AnnotationCopyFinalize(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate) {
	auto &state = gstate.Cast<AnnotationCopyGlobalState>();
	lock_guard<mutex> glock(state.lock);

	// Write closing tag
	string footer = "</Annotations>\n";
	state.handle->Write((void *)footer.c_str(), footer.size());

	state.handle->Close();
}

// Register the copy function
void RegisterAnnotationCopyFunction(ExtensionLoader &loader) {
	CopyFunction func("google_pse_annotation");
	func.copy_to_bind = AnnotationCopyBind;
	func.copy_to_initialize_local = AnnotationCopyInitializeLocal;
	func.copy_to_initialize_global = AnnotationCopyInitializeGlobal;
	func.copy_to_sink = AnnotationCopySink;
	func.copy_to_combine = AnnotationCopyCombine;
	func.copy_to_finalize = AnnotationCopyFinalize;
	func.extension = "xml";

	loader.RegisterFunction(func);
}

} // namespace duckdb
