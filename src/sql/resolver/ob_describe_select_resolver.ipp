#pragma once

namespace oceanbase { namespace sql {

int ObResolver::resolve_describe_select(const ParseNode &parse_tree, ObStmt *&stmt)
{
  using namespace common;
  int ret = OB_SUCCESS;
  ObStmt *source = nullptr;
  if (parse_tree.num_child_ != 5 || nullptr == parse_tree.children_[4]) {
    return OB_ERR_UNEXPECTED;
  }
  if (OB_FAIL(select_stmt_resolver_func<ObSelectResolver>(params_, *parse_tree.children_[4], source))) {
    return ret;
  }
  if (nullptr == source || !source->is_select_stmt()) return OB_ERR_UNEXPECTED;
  const ObSelectStmt &select = *static_cast<ObSelectStmt *>(source);
  ObSqlString sql;
  for (int64_t i = 0; OB_SUCC(ret) && i < select.get_select_item_size(); ++i) {
    const SelectItem &item = select.get_select_item(i);
    if (nullptr == item.expr_) return OB_ERR_UNEXPECTED;
    const ObObjType type = item.expr_->get_result_type().get_type();
    const char *type_name = ob_obj_type_str(type);
    switch (type) {
      case ObIntType: type_name = "BIGINT"; break;
      case ObTinyIntType: type_name = "BOOLEAN"; break;
      case ObDoubleType: type_name = "DOUBLE"; break;
      case ObVarcharType: type_name = "VARCHAR"; break;
      case ObDateType: type_name = "DATE"; break;
      case ObDateTimeType: type_name = "DATETIME"; break;
      default: break;
    }
    // Hex literals preserve arbitrary column names under every SQL escape mode.
    if (i > 0) ret = sql.append(" UNION ALL ");
    if (OB_SUCC(ret)) ret = sql.append("SELECT CONVERT(X'");
    const ObString &name = item.alias_name_.empty() ? item.expr_name_ : item.alias_name_;
    for (int64_t j = 0; OB_SUCC(ret) && j < name.length(); ++j) {
      ret = sql.append_fmt("%02x", static_cast<unsigned char>(name.ptr()[j]));
    }
    if (OB_SUCC(ret)) {
      ret = sql.append_fmt("' USING utf8mb4) AS column_name, '%s' AS column_type, '%s' AS `null`, "
          "NULL AS `key`, NULL AS `default`, NULL AS extra", type_name,
          item.expr_->get_result_type().has_result_flag(NOT_NULL_FLAG) ? "NO" : "YES");
    }
  }
  ParseResult result;
  ObParser parser(*params_.allocator_, params_.session_info_->get_sql_mode());
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(parser.parse(sql.string(), result))) {
  } else if (nullptr == result.result_tree_ || result.result_tree_->num_child_ != 1
             || nullptr == result.result_tree_->children_[0]) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ret = select_stmt_resolver_func<ObSelectResolver>(params_, *result.result_tree_->children_[0], stmt);
  }
  return ret;
}

} } // namespace oceanbase::sql
