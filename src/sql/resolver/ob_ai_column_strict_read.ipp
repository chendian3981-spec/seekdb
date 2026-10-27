/* Copyright (c) 2026 OceanBase. Licensed under the Apache License, Version 2.0. */
#pragma once
#include "share/ai_service/ai_column_spec.h"
#include "query/session/ob_inner_sql_connection_access.h"
#include "sql/session/ob_inner_sql_connection.h"
#include "common/mysqlclient/ob_mysql_result.h"
#include "share/ob_server_struct.h"
#include "sql/resolver/dml/ob_update_stmt.h"
#include "sql/resolver/dml/ob_stmt_expr_visitor.h"
#include <set>
#include <memory>

namespace oceanbase { namespace sql {

int ObResolver::check_ai_column_strict_read(ObDMLStmt &statement)
{
  using namespace common;
  using namespace share::schema;
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = params_.session_info_;
  bool strict = false;
  if (nullptr == session || session->is_ai_column_internal()) return OB_SUCCESS;
  if (OB_FAIL(session->get_sys_variable(share::SYS_VAR_AI_COLUMN_STRICT_READ, strict))) return ret;
  if (!strict) return OB_SUCCESS;
  auto identifier = [](const ObString &name) {
    std::string quoted = "`";
    for (int64_t i = 0; i < name.length(); ++i) { if (name.ptr()[i] == '`') quoted += '`'; quoted += name.ptr()[i]; }
    return quoted + '`';
  };
  struct Check { uint64_t table, column; std::string sql; };
  std::vector<Check> checks;
  std::set<std::pair<uint64_t, uint64_t>> visited;
  std::vector<ObDMLStmt *> statements{&statement};
  std::set<ObDMLStmt *> seen_statements{&statement};
  for (size_t i = 0; i < statements.size(); ++i) {
    ObDMLStmt &current = *statements[i];
    ObSEArray<ObRawExpr *, 16> roots, referenced;
    ObStmtExprGetter getter;
    getter.set_relation_scope();
    getter.remove_scope(SCOPE_DMLINFOS);
    getter.remove_scope(SCOPE_DML_COLUMN);
    getter.remove_scope(SCOPE_DML_VALUE);
    getter.remove_scope(SCOPE_DML_CONSTRAINT);
    if (OB_FAIL(current.get_relation_exprs(roots, getter))) return ret;
    if (current.is_update_stmt()) {
      const auto &infos = static_cast<ObUpdateStmt &>(current).get_update_table_info();
      for (int64_t k = 0; k < infos.count(); ++k) {
        const auto *info = infos.at(k);
        if (nullptr == info) continue;
        for (const auto &assignment : info->assignments_) {
          if (!assignment.is_implicit_ && OB_FAIL(roots.push_back(assignment.expr_))) return ret;
        }
      }
    }
    if (OB_FAIL(ObRawExprUtils::extract_column_exprs(roots, referenced))) return ret;
    std::set<const ObRawExpr *> actual_references;
    for (auto *expr : referenced) actual_references.insert(expr);
    for (int64_t j = 0; j < current.get_column_size(); ++j) {
      const ColumnItem *item = current.get_column_item(j);
      if (nullptr == item || nullptr == item->expr_ || actual_references.count(item->expr_) == 0) continue;
      const TableItem *table_item = current.get_table_item_by_id(item->table_id_);
      if (nullptr == table_item || !table_item->is_basic_table()) continue;
      const ObTableSchema *table = nullptr;
      if (OB_FAIL(params_.schema_checker_->get_table_schema(table_item->ref_id_, table))) return ret;
      if (nullptr == table) return OB_SCHEMA_ERROR;
      const auto *column = table->get_column_schema(item->column_id_);
      if (nullptr == column || !column->is_ai_column()) continue;
      if (!visited.emplace(table->get_table_id(), column->get_column_id()).second) continue;
      const ObDatabaseSchema *database = nullptr;
      if (OB_FAIL(params_.schema_checker_->get_schema_guard()->get_database_schema(table->get_database_id(), database))) return ret;
      if (nullptr == database) return OB_SCHEMA_ERROR;
      const std::string state = share::AiColumnSpec::hidden_name(column->get_column_name(), "state");
      checks.push_back({table->get_table_id(), column->get_column_id(), "SELECT 1 AS incomplete FROM "
          + identifier(database->get_database_name_str()) + "." + identifier(table->get_table_name_str())
          + " WHERE " + identifier(ObString(state.size(), state.data())) + "<>1 LIMIT 1"});
    }
    ObSEArray<ObSelectStmt *, 4> children;
    if (OB_FAIL(current.get_child_stmts(children))) return ret;
    for (int64_t j = 0; j < children.count(); ++j) {
      if (nullptr != children.at(j) && seen_statements.insert(children.at(j)).second) statements.push_back(children.at(j));
    }
  }
  if (checks.empty()) return OB_SUCCESS;
  // Incompleteness is mutable data. Recheck it on each execution instead of
  // caching a successful compile-time check across subsequent invalidations.
  const_cast<ObGlobalHint &>(params_.query_ctx_->get_global_hint()).merge_plan_cache_hint(OB_USE_PLAN_CACHE_NONE);
  // The outer statement has not started its transaction during resolution.
  // Without an existing user transaction, use an independent read connection;
  // a nested session at this point has no parent transaction to attach to.
  if (!session->is_in_transaction()) {
    if (nullptr == GCTX.sql_proxy_) return OB_NOT_INIT;
    for (const auto &check : checks) {
      ObMySQLProxy::MySQLResult response;
      if (OB_FAIL(GCTX.sql_proxy_->read(response, check.sql.c_str()))) break;
      auto *result = response.get_result();
      if (nullptr == result) { ret = OB_ERR_UNEXPECTED; break; }
      const int next = result->next();
      if (next == OB_SUCCESS) {
        ret = OB_AI_COLUMN_NOT_READY;
        LOG_USER_ERROR(OB_AI_COLUMN_NOT_READY, check.table, check.column);
      } else if (next != OB_ITER_END) ret = next;
      const int close_ret = result->close();
      if (ret == OB_SUCCESS) ret = close_ret;
      if (ret != OB_SUCCESS) break;
    }
    return ret;
  }
  common::sqlclient::ObISQLConnectionGuard connection_guard;
  common::sqlclient::ObISQLConnection *base = session->get_inner_conn();
  if (nullptr == base) {
    if (OB_FAIL(query::ObInnerSQLConnectionAccess::create_connection_with_external_session(session, connection_guard))) return ret;
    base = connection_guard.get_ptr();
  }
  ObIInnerSQLConnection *connection = as_inner_sql_connection(base);
  if (nullptr == connection) return OB_ERR_UNEXPECTED;
  ObSQLSessionInfo::StmtSavedValue saved_session;
  // Resolution runs before a parent execution plan exists. Preserve the
  // session without entering a nested execution frame (the optimizer's own
  // external-session reads likewise run at this level).
  if (OB_FAIL(session->save_session(saved_session))) return ret;
  session->set_ai_column_internal(true);
  auto restore = [&](int *) {
    session->set_ai_column_internal(false);
    const int end_ret = session->restore_session(saved_session);
    if (ret == OB_SUCCESS) ret = end_ret;
  };
  std::unique_ptr<int, decltype(restore)> restore_guard(&ret, restore);
  for (const auto &check : checks) {
    ObMySQLProxy::MySQLResult response;
    if (OB_FAIL(connection->execute_read(check.sql.c_str(), response))) break;
    auto *result = response.get_result();
    if (nullptr == result) { ret = OB_ERR_UNEXPECTED; break; }
    const int next = result->next();
    if (next == OB_SUCCESS) {
      ret = OB_AI_COLUMN_NOT_READY;
      LOG_USER_ERROR(OB_AI_COLUMN_NOT_READY, check.table, check.column);
    } else if (next != OB_ITER_END) ret = next;
    const int close_ret = result->close();
    if (ret == OB_SUCCESS) ret = close_ret;
    if (ret != OB_SUCCESS) break;
  }
  restore_guard.reset();
  return ret;
}

} }
