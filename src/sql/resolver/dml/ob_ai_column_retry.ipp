/* Copyright (c) 2026 OceanBase. Licensed under the Apache License, Version 2.0. */
#pragma once
#include "share/ai_service/ai_column_spec.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include <functional>

namespace oceanbase { namespace sql {

int ObUpdateResolver::resolve_ai_column_retry(const ParseNode &tree,
    common::ObIArray<ObTableAssignment> &assignments)
{
  using namespace common;
  using namespace share::schema;
  int ret = OB_SUCCESS;
  const ParseNode *marker = tree.num_child_ > WHEN ? tree.children_[WHEN] : nullptr;
  if (nullptr != marker && marker->type_ == T_EMPTY && marker->value_ == 2028) {
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "combining AI COLUMN RETRY with another ALTER action");
    return OB_NOT_SUPPORTED;
  }
  if (nullptr == marker || marker->type_ != T_EMPTY || marker->value_ != 2027) return OB_SUCCESS;
  if (assignments.count() != 1 || marker->num_child_ != 1 || nullptr == marker->children_[0]) return OB_NOT_SUPPORTED;
  const TableItem *table_item = get_stmt()->get_table_item_by_id(assignments.at(0).table_id_);
  if (nullptr == table_item || !table_item->is_basic_table()) return OB_NOT_SUPPORTED;
  const ObTableSchema *table = nullptr;
  if (OB_FAIL(schema_checker_->get_table_schema(table_item->ref_id_, table))) return ret;
  if (nullptr == table) return OB_SCHEMA_ERROR;
  const ParseNode &name_node = *marker->children_[0];
  ObString name(name_node.str_len_, name_node.str_value_);
  const auto *column = table->get_column_schema(name);
  if (nullptr == column || !column->is_ai_column()) {
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "retrying a column that is not an AI column");
    return OB_NOT_SUPPORTED;
  }
  for (int64_t i = 0; i < assignments.at(0).assignments_.count(); ++i) {
    assignments.at(0).assignments_.at(i).is_implicit_ = true;
  }
  const std::string hidden_name = share::AiColumnSpec::hidden_name(column->get_column_name(), "state");
  const auto *state = table->get_column_schema(ObString(hidden_name.size(), hidden_name.data()));
  if (nullptr == state) return OB_SCHEMA_ERROR;
  ObSEArray<ObColumnRefRawExpr *, 1> state_expressions;
  if (OB_FAIL(add_column_to_stmt(*table_item, *state, state_expressions))) return ret;
  if (state_expressions.count() != 1) return OB_ERR_UNEXPECTED;
  ObConstRawExpr *force = nullptr;
  ObAssignment assignment;
  assignment.column_expr_ = state_expressions.at(0);
  assignment.is_implicit_ = true;
  if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*params_.expr_factory_, ObIntType, -1, force))) {
  } else {
    assignment.expr_ = force;
    ret = ObRawExprUtils::build_column_conv_expr(*params_.expr_factory_, *allocator_,
        *assignment.column_expr_, assignment.expr_, session_info_);
  }
  if (OB_FAIL(ret)) return ret;
  // The executor replaces this datum with PENDING for each row. Give it its
  // own per-row storage so that the next row still evaluates the -1 sentinel.
  if (OB_FAIL(ObRawExprUtils::build_wrapper_inner_expr(*params_.expr_factory_,
      *session_info_, assignment.expr_, assignment.expr_))) return ret;
  if (OB_FAIL(assignments.at(0).assignments_.push_back(assignment))) return ret;
  // STATUS is a pseudo column only inside the retry predicate. A quoted
  // `status` remains an ordinary business column.
  std::string quoted = "`";
  for (char c : hidden_name) { if (c == '`') quoted += '`'; quoted += c; }
  quoted += '`';
  const std::string expression = "CASE " + quoted + " WHEN 0 THEN 'PENDING' WHEN 1 THEN 'READY' "
      "WHEN 2 THEN 'FAILED' WHEN 3 THEN 'RETRY' ELSE 'PENDING' END";
  std::function<int(ParseNode *&, int)> replace = [&](ParseNode *&node, int depth) {
    if (nullptr == node) return OB_SUCCESS;
    if (depth > 128) return OB_SIZE_OVERFLOW;
    if (node->type_ == T_COLUMN_REF && node->num_child_ == 3 && nullptr == node->children_[0]
        && nullptr == node->children_[1] && nullptr != node->children_[2]) {
      const ParseNode &name = *node->children_[2];
      if (!name.is_input_quoted_ && ObString(name.str_len_, name.str_value_).case_compare("status") == 0) {
        const ParseNode *replacement = nullptr;
        const int result = ObRawExprUtils::parse_expr_node_from_str(
            ObString(expression.size(), expression.data()), session_info_->get_charsets4parser(),
            *allocator_, replacement, session_info_->get_sql_mode());
        if (result == OB_SUCCESS) node = const_cast<ParseNode *>(replacement);
        return result;
      }
    }
    for (int64_t i = 0; i < node->num_child_; ++i) {
      const int result = replace(node->children_[i], depth + 1);
      if (result != OB_SUCCESS) return result;
    }
    return OB_SUCCESS;
  };
  return replace(tree.children_[WHERE], 0);
}

} }
