/* Copyright (c) 2025 OceanBase. Licensed under the Apache License, Version 2.0. */
#pragma once
#define SEEKDB_FILE_PARQUET 1
#include "sql/engine/basic/file_scan_reader.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/ob_sql_utils.h"

namespace oceanbase { namespace sql {

int ObDMLResolver::resolve_file_table_item(const ParseNode &node, TableItem *&table)
{
  using namespace common;
  int ret = OB_SUCCESS;
  bool enabled = false;
  if (OB_ISNULL(session_info_) || node.num_child_ != 5
      || OB_ISNULL(node.children_[0]) || OB_ISNULL(node.children_[1])) {
    return OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(session_info_->get_sys_variable(share::SYS_VAR_ENABLE_FILE_SQL, enabled))) {
    return ret;
  } else if (!enabled) {
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "file SQL is disabled; set enable_file_sql=ON");
    return OB_NOT_SUPPORTED;
  }
  // File-dependent column metadata cannot share a parameterized plan across
  // paths or survive a changed file. Rebind the schema for each statement.
  if (nullptr != params_.query_ctx_) {
    const_cast<ObGlobalHint &>(params_.query_ctx_->get_global_hint()).merge_plan_cache_hint(OB_USE_PLAN_CACHE_NONE);
  }
  const ParseNode &arguments = *node.children_[1];
  if (arguments.num_child_ < 1) return OB_ERR_PARSER_SYNTAX;
  FileScanOptions options;
  const int64_t kind = node.children_[0]->value_;
  options.format = kind == 1 ? "csv" : kind == 2 ? "json" : kind == 3 ? "parquet" : "glob";
  auto constant = [&](const ParseNode &value_node, ObObj &value) {
    ObRawExpr *expression = nullptr;
    bool got = false;
    int result = resolve_sql_expr(value_node, expression);
    if (OB_SUCCESS == result && nullptr != expression) result = expression->deduce_type(session_info_);
    if (OB_SUCCESS == result) {
      result = ObSQLUtils::calc_const_or_calculable_expr(session_info_->get_cur_exec_ctx(),
          expression, value, got, *allocator_);
    }
    return OB_SUCCESS == result && !got ? OB_INVALID_ARGUMENT : result;
  };
  ObObj path;
  ObString root;
  if (OB_FAIL(constant(*arguments.children_[0], path))) {
  } else if (path.is_null() || !ob_is_string_type(path.get_type())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(session_info_->get_secure_file_priv(root))) {
  } else {
    options.path.assign(path.get_string().ptr(), path.get_string().length());
    options.root.assign(root.ptr(), root.length());
  }
  for (int64_t i = 1; OB_SUCC(ret) && i < arguments.num_child_; ++i) {
    const ParseNode *option = arguments.children_[i];
    if (nullptr == option || option->type_ != T_OP_EQ || option->num_child_ != 2
        || nullptr == option->children_[0] || nullptr == option->children_[1]) {
      ret = OB_ERR_PARSER_SYNTAX;
      break;
    }
    const ParseNode *name = option->children_[0];
    if (name->type_ == T_COLUMN_REF && name->num_child_ == 3) name = name->children_[2];
    if (nullptr == name || nullptr == name->str_value_) {
      ret = OB_INVALID_ARGUMENT;
      break;
    }
    ObString key(name->str_len_, name->str_value_);
    ObObj value;
    if (OB_FAIL(constant(*option->children_[1], value))) {
    } else if (value.is_null()) {
      ret = OB_INVALID_ARGUMENT;
    } else if (key.case_compare("sample_size") == 0) {
      if (!ob_is_integer_type(value.get_type()) || value.get_int() != -1) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "file SQL requires full schema inference (sample_size=-1)");
      }
    } else if (key.case_compare("header") == 0) {
      if (!ob_is_integer_type(value.get_type()) || (value.get_int() != 0 && value.get_int() != 1)) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        options.header = value.get_int() != 0;
      }
    } else if (!ob_is_string_type(value.get_type())) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      const ObString text = value.get_string();
      if (key.case_compare("nullstr") == 0 || key.case_compare("null_string") == 0) {
        options.nullstr.assign(text.ptr(), text.length());
      } else if (key.case_compare("format") == 0) {
        if (kind != 2 || (text.case_compare("newline_delimited") != 0 && text.case_compare("ndjson") != 0)) {
          ret = OB_INVALID_ARGUMENT;
        }
      } else if (text.length() != 1) {
        ret = OB_INVALID_ARGUMENT;
      } else if (key.case_compare("delim") == 0 || key.case_compare("delimiter") == 0 || key.case_compare("sep") == 0) {
        options.delimiter = text.ptr()[0];
      } else if (key.case_compare("quote") == 0) {
        options.quote = text.ptr()[0];
      } else if (key.case_compare("escape") == 0) {
        options.escape = text.ptr()[0];
      } else {
        ret = OB_INVALID_ARGUMENT;
      }
    }
  }
  if (OB_FAIL(ret)) return ret;
  try {
    FileScanReader reader(options);
    reader.infer_schema();
    if (reader.columns().empty()) return OB_INVALID_DATA;
    options.fingerprint = reader.fingerprint();
    std::string encoded = FileScanReader::serialize_options(options);
    ObString encoded_copy;
    ObString alias(kind == 1 ? "read_csv" : kind == 2 ? "read_json" : kind == 3 ? "read_parquet" : "glob");
    if (nullptr != node.children_[2]) alias.assign_ptr(node.children_[2]->str_value_, node.children_[2]->str_len_);
    ObString alias_copy;
    ObConstRawExpr *value = nullptr;
    if (OB_FAIL(ob_write_string(*allocator_, alias, alias_copy))) {
    } else if (OB_FAIL(ob_write_string(*allocator_, ObString(encoded.size(), encoded.data()), encoded_copy))) {
    } else if (OB_FAIL(ObRawExprUtils::build_const_string_expr(*params_.expr_factory_,
        ObVarcharType, encoded_copy, CS_TYPE_UTF8MB4_BIN, value))) {
    } else if (OB_FAIL(create_unnest_table_item(table, alias_copy))) {
    } else {
      table->json_table_def_->table_type_ = OB_FILE_TABLE_TYPE;
      // Stored in the already serialized root metadata; identifies file plans
      // without changing existing JSON_TABLE/UNNEST wire formats.
      table->json_table_def_->all_cols_.at(0)->truncate_ = -2026;
      ret = table->json_table_def_->doc_exprs_.push_back(value);
    }
    for (size_t i = 0; OB_SUCC(ret) && i < reader.columns().size(); ++i) {
      const FileScanColumn &column = reader.columns()[i];
      ObDmlJtColDef *definition = OB_NEWx(ObDmlJtColDef, allocator_);
      if (OB_ISNULL(definition)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        break;
      }
      definition->table_id_ = table->table_id_;
      auto &info = definition->col_base_info_;
      info.col_type_ = COL_TYPE_VALUE;
      info.parent_id_ = 0;
      info.id_ = i + 1;
      info.output_column_idx_ = OB_APP_MIN_COLUMN_ID + i;
      ObDataType type;
      ObObjType object_type = ObVarcharType;
      switch (column.type) {
        case FileValueType::Bigint: object_type = ObIntType; break;
        case FileValueType::Double: object_type = ObDoubleType; break;
        case FileValueType::Boolean: object_type = ObTinyIntType; break;
        case FileValueType::Date: object_type = ObDateType; break;
        case FileValueType::Datetime: object_type = ObDateTimeType; break;
        default: break;
      }
      type.set_obj_type(object_type);
      type.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY[object_type]);
      if (object_type == ObVarcharType) type.set_length(FileScanReader::MAX_RECORD_BYTES);
      type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
      type.set_collation_level(CS_LEVEL_IMPLICIT);
      info.data_type_ = type;
      info.res_type_ = object_type;
      ColumnItem *item = nullptr;
      if (OB_FAIL(ob_write_string(*allocator_, ObString(column.name.size(), column.name.data()), info.col_name_))) {
      } else if (OB_FAIL(json_table_infos_.push_back(definition))) {
      } else if (OB_FAIL(generate_json_table_output_column_item(table, type, info.col_name_, OB_APP_MIN_COLUMN_ID + i, item))) {
      } else {
        item->col_idx_ = i + 1;
        if (!column.nullable) item->expr_->set_result_flag(NOT_NULL_FLAG);
        ret = table->json_table_def_->all_cols_.push_back(&info);
      }
    }
  } catch (const std::exception &error) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, error.what());
  }
  return ret;
}

} } // namespace oceanbase::sql
