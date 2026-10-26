#pragma once
#include <algorithm>
#include <cctype>
#include <string>
#include <set>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace oceanbase { namespace sql {

int ObSelectResolver::resolve_file_copy(const ParseNode &node, ObSelectIntoItem &item)
{
  using namespace common;
  int ret = OB_SUCCESS;
  bool enabled = false;
  if (OB_FAIL(session_info_->get_sys_variable(share::SYS_VAR_ENABLE_FILE_SQL, enabled))) {
  } else if (!enabled) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "file SQL is disabled");
  } else if (node.num_child_ != 3 || nullptr == node.children_[0]) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(resolve_into_const_node(node.children_[0], item.outfile_name_))) {
  }
  if (OB_FAIL(ret)) return ret;
  bool header = false;
  std::string delimiter = ",", quote = "\"", nullstr;
  std::set<std::string> seen;
  const ParseNode *options = node.children_[1];
  for (int64_t i = 0; nullptr != options && OB_SUCC(ret) && i < options->num_child_; ++i) {
    const ParseNode *option = options->children_[i];
    if (nullptr == option || option->num_child_ != 2 || nullptr == option->children_[0]) return OB_INVALID_ARGUMENT;
    const ParseNode *key_node = option->children_[0];
    const ParseNode *value = option->children_[1];
    std::string key(key_node->str_value_, key_node->str_len_);
    std::transform(key.begin(), key.end(), key.begin(), [](unsigned char c) { return std::tolower(c); });
    if (!seen.insert(key).second) return OB_INVALID_ARGUMENT;
    std::string text;
    if (nullptr != value && nullptr != value->str_value_) text.assign(value->str_value_, value->str_len_);
    if (key == "header") {
      header = nullptr == value || (value->type_ == T_BOOL && value->value_ != 0);
      if (nullptr != value && value->type_ != T_BOOL) return OB_INVALID_ARGUMENT;
    } else if (key == "format") {
      ObString format(text.size(), text.data());
      if (format.case_compare("csv") != 0) return OB_NOT_SUPPORTED;
    } else if (nullptr == value || value->type_ != T_VARCHAR) {
      return OB_INVALID_ARGUMENT;
    } else if (key == "delimiter" || key == "delim" || key == "sep") {
      if (text.size() != 1 || text[0] == '\n' || text[0] == '\r' || text[0] == '\0') return OB_INVALID_ARGUMENT;
      delimiter = text;
    } else if (key == "quote" || key == "escape") {
      if (text != "\"") return OB_NOT_SUPPORTED;
      quote = text;
    } else if (key == "nullstr") {
      nullstr = text;
    } else {
      return OB_NOT_SUPPORTED;
    }
  }
  rapidjson::StringBuffer buffer;
  if (delimiter == quote || nullstr.find_first_of(delimiter + "\"\r\n") != std::string::npos
      || nullstr.find('\0') != std::string::npos) return OB_INVALID_ARGUMENT;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  writer.StartObject();
  writer.Key("file_sql_copy"); writer.Bool(true);
  writer.Key("header"); writer.Bool(header);
  writer.Key("delimiter"); writer.String(delimiter.data(), delimiter.size());
  writer.Key("nullstr"); writer.String(nullstr.data(), nullstr.size());
  writer.Key("names"); writer.StartArray();
  for (int64_t i = 0; i < get_select_stmt()->get_select_item_size(); ++i) {
    const SelectItem &select_item = get_select_stmt()->get_select_item(i);
    const ObString &name = select_item.alias_name_.empty() ? select_item.expr_name_ : select_item.alias_name_;
    writer.String(name.ptr(), name.length());
  }
  writer.EndArray(); writer.EndObject();
  item.is_single_ = true;
  if (OB_FAIL(ob_write_string(*allocator_, ObString(buffer.GetSize(), buffer.GetString()), item.external_properties_))) {
  }
  const_cast<ObGlobalHint &>(params_.query_ctx_->get_global_hint()).merge_plan_cache_hint(OB_USE_PLAN_CACHE_NONE);
  return ret;
}

} }
