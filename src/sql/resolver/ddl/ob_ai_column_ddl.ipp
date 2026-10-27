/* Copyright (c) 2026 OceanBase. Licensed under the Apache License, Version 2.0. */
#pragma once
#include "share/ai_service/ai_column_spec.h"
#include "sql/printer/ob_raw_expr_printer.h"
#include "query/vector/ob_vector_index_util.h"

namespace oceanbase { namespace sql {

int ObDDLResolver::resolve_ai_column_definition(share::schema::ObColumnSchemaV2 &column,
                                               const ParseNode &node)
{
  using namespace common;
  share::AiColumnSpec spec;
  if (column.is_ai_column() || node.num_child_ != 6 || nullptr == node.children_[4]) return OB_INVALID_ARGUMENT;
  const ParseNode &expression = *node.children_[4];
  spec.expression.assign(expression.str_value_, expression.str_len_);
  spec.input_expression = "pending";
  spec.model = "pending";
  const ParseNode *options = node.children_[5];
  for (int64_t i = 0; nullptr != options && i < options->num_child_; ++i) {
    const ParseNode *option = options->children_[i];
    if (nullptr == option || option->type_ != T_OP_EQ || option->num_child_ != 2) return OB_INVALID_ARGUMENT;
    const ParseNode *key = option->children_[0];
    const ParseNode *value = option->children_[1];
    if (nullptr != key && key->type_ == T_COLUMN_REF && key->num_child_ == 3) key = key->children_[2];
    if (nullptr == key || nullptr == value) return OB_INVALID_ARGUMENT;
    ObString name(key->str_len_, key->str_value_);
    if (name.case_compare("sync_mode") == 0) {
      if (value->type_ != T_VARCHAR || ObString(value->str_len_, value->str_value_).case_compare("async") != 0) return OB_NOT_SUPPORTED;
    } else if (name.case_compare("max_retries") == 0) {
      if (value->type_ != T_INT || value->value_ < 0 || value->value_ > 100) return OB_INVALID_ARGUMENT;
      spec.max_retries = value->value_;
    } else {
      return OB_NOT_SUPPORTED;
    }
  }
  const std::string encoded = spec.serialize();
  const int ret = column.set_ai_column_definition(ObString(encoded.size(), encoded.data()));
  if (ret == OB_SUCCESS) {
    column.add_column_flag(AI_COLUMN_FLAG);
    column.set_nullable(true);
  }
  return ret;
}

int ObDDLResolver::build_ai_column_hidden_columns(const share::schema::ObTableSchema &table,
    const share::schema::ObColumnSchemaV2 &column,
    common::ObIArray<share::schema::ObColumnSchemaV2> &hidden)
{
  using namespace common;
  using namespace share::schema;
  int ret = OB_SUCCESS;
  try {
    const ObString &encoded = column.get_ai_column_definition();
    share::AiColumnSpec spec = share::AiColumnSpec::deserialize({encoded.ptr(), static_cast<size_t>(encoded.length())});
    ObRawExpr *expression = nullptr;
    ObSEArray<ObQualifiedName, 8> names;
    if (OB_FAIL(ObRawExprUtils::build_generated_column_expr(
        ObString(spec.expression.size(), spec.expression.data()), *params_.expr_factory_, *session_info_,
        expression, names, &table, nullptr, schema_checker_))) return ret;
    if (nullptr == expression || (expression->get_expr_type() != T_FUN_SYS_AI_COMPLETE
        && expression->get_expr_type() != T_FUN_SYS_AI_EMBED) || expression->get_param_count() < 2
        || expression->get_param_count() > 3) return OB_NOT_SUPPORTED;
    spec.embedding = expression->get_expr_type() == T_FUN_SYS_AI_EMBED;
    ObRawExpr *model = expression->get_param_expr(0);
    if (nullptr == model || !model->is_const_expr()) return OB_INVALID_ARGUMENT;
    const ObObj &model_value = static_cast<ObConstRawExpr *>(model)->get_value();
    if (!ob_is_string_type(model_value.get_type()) || model_value.get_string().empty()) return OB_INVALID_ARGUMENT;
    spec.model.assign(model_value.get_string().ptr(), model_value.get_string().length());
    for (int64_t i = 0; i < names.count(); ++i) {
      const ObString &name = names.at(i).col_name_;
      const ObColumnSchemaV2 *dependency = table.get_column_schema(name);
      if (nullptr == dependency) return OB_ERR_BAD_FIELD_ERROR;
      if (dependency->is_ai_column() || dependency->is_hidden()) {
        return OB_NOT_SUPPORTED;
      }
      spec.dependencies.emplace_back(name.ptr(), name.length());
    }
    std::vector<char> buffer(64 * 1024);
    int64_t position = 0;
    ObRawExprPrinter printer(buffer.data(), buffer.size(), &position, schema_checker_->get_schema_guard(), session_info_->get_timezone_info());
    if (OB_FAIL(printer.do_print(expression->get_param_expr(1), T_NONE_SCOPE, true))) return ret;
    spec.input_expression.assign(buffer.data(), position);
    if (spec.embedding) {
      if (!column.is_collection()) return OB_INVALID_ARGUMENT;
      if (OB_FAIL(ObVectorIndexUtil::get_vector_dim_from_extend_type_info(column.get_extended_type_info(), spec.dimension))) return ret;
      if (expression->get_param_count() == 3) {
        const ObRawExpr *dimension = expression->get_param_expr(2);
        if (!dimension->is_const_expr()) return OB_INVALID_ARGUMENT;
        const ObObj &value = static_cast<const ObConstRawExpr *>(dimension)->get_value();
        if (!ob_is_integer_type(value.get_type()) || value.get_int() != spec.dimension) {
          LOG_USER_ERROR(OB_ERR_INVALID_VECTOR_DIM, static_cast<unsigned>(spec.dimension),
              static_cast<unsigned>(ob_is_integer_type(value.get_type()) ? value.get_int() : 0));
          return OB_ERR_INVALID_VECTOR_DIM;
        }
      }
    } else if (column.is_json()) {
      spec.system_prompt = "Return exactly one valid JSON value. Do not include Markdown or explanatory text.";
    } else if (ob_is_integer_type(column.get_data_type())) {
      spec.system_prompt = "Return exactly one base-10 integer. Do not include Markdown or explanatory text.";
    } else if (ob_is_numeric_type(column.get_data_type())) {
      spec.system_prompt = "Return exactly one finite decimal number. Do not include Markdown or explanatory text.";
    } else if (!column.is_string_type() && !ob_is_text_tc(column.get_data_type())) {
      return OB_NOT_SUPPORTED;
    }
    // Persist the typed system prompt in the executable normalized expression.
    if (!spec.embedding && !spec.system_prompt.empty()) {
      auto hex = [](const std::string &text) {
        const char *digits = "0123456789abcdef";
        std::string out;
        for (unsigned char c : text) { out += digits[c >> 4]; out += digits[c & 15]; }
        return "CONVERT(X'" + out + "' USING utf8mb4)";
      };
      spec.expression = "AI_COMPLETE(" + hex(spec.model) + "," + spec.input_expression
          + ",JSON_OBJECT('system_prompt'," + hex(spec.system_prompt) + ")) /* " + spec.system_prompt + " */";
    }
    const std::string saved = spec.serialize();
    if (saved.size() > 65535) return OB_SIZE_OVERFLOW;
    const std::string name(column.get_column_name_str().ptr(), column.get_column_name_str().length());
    for (int i = 0; OB_SUCC(ret) && i < 3; ++i) {
      ObColumnSchemaV2 control;
      const char *suffix = i == 0 ? "state" : i == 1 ? "request" : "spec";
      const std::string hidden_name = share::AiColumnSpec::hidden_name(name, suffix);
      if (nullptr != table.get_column_schema(ObString(hidden_name.size(), hidden_name.data()))) return OB_ERR_COLUMN_DUPLICATE;
      control.set_table_id(table.get_table_id());
      control.set_data_type(i == 0 ? ObTinyIntType : ObVarcharType);
      control.set_data_length(i == 0 ? 1 : i == 1 ? 64 : 65535);
      control.set_nullable(i != 0);
      control.set_is_hidden(true);
      control.set_charset_type(CHARSET_UTF8MB4);
      control.set_collation_type(CS_TYPE_UTF8MB4_BIN);
      ObObj value;
      if (i == 0) value.set_tinyint(static_cast<int8_t>(share::AiColumnState::Pending));
      else if (i == 2) { value.set_varchar(ObString(saved.size(), saved.data())); value.set_collation_type(CS_TYPE_UTF8MB4_BIN); }
      if (OB_FAIL(control.set_column_name(ObString(hidden_name.size(), hidden_name.data())))) {
      } else if (OB_FAIL(control.set_orig_default_value(value))) {
      } else if (OB_FAIL(control.set_cur_default_value(value, false))) {
      } else if (OB_FAIL(hidden.push_back(control))) {
      }
    }
  } catch (const std::exception &error) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, error.what());
  }
  return ret;
}

} }
