/* Copyright (c) 2026 OceanBase. Licensed under the Apache License, Version 2.0. */
#pragma once
#include "share/ai_service/ai_column_spec.h"
#include "sql/engine/dml/ob_table_modify_op.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "common/mysqlclient/ob_mysql_result.h"
#include <set>
#include <map>
#include <memory>

namespace oceanbase { namespace sql {

static std::string ai_column_sql_string(const std::string &text)
{
  const char *digits = "0123456789abcdef";
  std::string hex;
  for (unsigned char c : text) { hex += digits[c >> 4]; hex += digits[c & 15]; }
  return "CONVERT(X'" + hex + "' USING utf8mb4)";
}

static std::string ai_column_sql_identifier(const std::string &name)
{
  std::string result = "`";
  for (char c : name) { if (c == '`') result += '`'; result += c; }
  return result + '`';
}

// Use the same nested SQL connection as foreign keys and triggers. Its writes
// participate in the caller's transaction and statement savepoint.
static int prepare_ai_column_dml(const ObDMLBaseCtDef &definition, ObTableModifyOp &op,
                                  bool deleting, bool updating)
{
  using namespace common;
  using namespace share::schema;
  using share::AiColumnSpec;
  int ret = OB_SUCCESS;
  ObExecContext &context = op.get_exec_ctx();
  ObSQLSessionInfo *session = context.get_my_session();
  if (!definition.is_primary_index_ || nullptr == session || session->is_ai_column_internal()) return OB_SUCCESS;
  const ObTableSchema *table = nullptr;
  if (nullptr == context.get_sql_ctx() || nullptr == context.get_sql_ctx()->schema_guard_) return OB_ERR_UNEXPECTED;
  if (OB_FAIL(context.get_sql_ctx()->schema_guard_->get_table_schema(definition.das_base_ctdef_.index_tid_, table))) return ret;
  if (nullptr == table) return OB_SCHEMA_ERROR;
  std::vector<const ObColumnSchemaV2 *> columns;
  for (int64_t i = 0; i < table->get_column_count(); ++i) {
    const auto *column = table->get_column_schema_by_idx(i);
    if (nullptr != column && column->is_ai_column()) columns.push_back(column);
  }
  if (columns.empty()) return OB_SUCCESS;
  try {
    auto position = [&](uint64_t id) -> int64_t {
      for (int64_t i = 0; i < definition.column_ids_.count(); ++i) if (definition.column_ids_.at(i) == id) return i;
      return -1;
    };
    ObEvalCtx &eval = op.get_eval_ctx();
    auto datum = [&](uint64_t id, bool old, ObDatum *&value) {
      const int64_t index = position(id);
      const auto &row = old ? definition.old_row_ : definition.new_row_;
      if (index < 0 || index >= row.count()) return OB_SCHEMA_ERROR;
      return row.at(index)->eval(eval, value);
    };
    auto literal = [&](uint64_t id, bool old, std::string &sql) {
      int result = OB_SUCCESS;
      const int64_t index = position(id);
      const auto &row = old ? definition.old_row_ : definition.new_row_;
      ObDatum *value = nullptr;
      if (index < 0 || index >= row.count()) return OB_SCHEMA_ERROR;
      ObExpr *expression = row.at(index);
      if (OB_SUCCESS != (result = expression->eval(eval, value))) return result;
      if (value->is_null()) { sql = "NULL"; return OB_SUCCESS; }
      if (ob_is_string_type(expression->obj_meta_.get_type())) {
        ObEvalCtx::TempAllocGuard temporary(eval);
        ObString text;
        if (OB_SUCCESS == (result = ObTextStringHelper::read_real_string_data(context,
            temporary.get_allocator(), *value, expression->datum_meta_, expression->obj_meta_.has_lob_header(), text))) {
          sql = ai_column_sql_string({text.ptr(), static_cast<size_t>(text.length())});
        }
      } else {
        ObObj object;
        std::vector<char> buffer(16 * 1024 * 1024);
        int64_t length = 0;
        if (OB_SUCCESS != (result = value->to_obj(object, expression->obj_meta_))) {
        } else if (OB_SUCCESS != (result = object.print_sql_literal(buffer.data(), buffer.size(), length))) {
        } else {
          sql.assign(buffer.data(), length);
        }
      }
      return result;
    };
    auto changed = [&](uint64_t id, bool &different) {
      std::string before, after;
      int result = literal(id, true, before);
      if (result == OB_SUCCESS) result = literal(id, false, after);
      if (result == OB_SUCCESS) different = before != after;
      return result;
    };
    auto predicate = [&](bool old, std::string &sql) {
      int result = OB_SUCCESS;
      const auto &rowkey = table->get_rowkey_info();
      for (int64_t i = 0; result == OB_SUCCESS && i < rowkey.get_size(); ++i) {
        const uint64_t id = rowkey.get_column(i)->column_id_;
        const auto *column = table->get_column_schema(id);
        if (nullptr == column) return OB_SCHEMA_ERROR;
        std::string value;
        if (OB_SUCCESS == (result = literal(id, old, value))) {
          if (i) sql += " AND ";
          sql += ai_column_sql_identifier(column->get_column_name()) + " = " + value;
        }
      }
      return result;
    };
    std::string old_predicate, new_predicate;
    if ((deleting || updating) && OB_FAIL(predicate(true, old_predicate))) return ret;
    if (!deleting && OB_FAIL(predicate(false, new_predicate))) return ret;
    struct Pending {
      const ObColumnSchemaV2 *column;
      const ObColumnSchemaV2 *state;
      const ObColumnSchemaV2 *request;
      AiColumnSpec spec;
      std::string source;
    };
    std::vector<Pending> pending;
    for (const auto *column : columns) {
      const std::string name(column->get_column_name());
      auto control = [&](const char *suffix) {
        const std::string hidden = AiColumnSpec::hidden_name(name, suffix);
        return table->get_column_schema(ObString(hidden.size(), hidden.data()));
      };
      const auto *state = control("state");
      const auto *request = control("request");
      const auto *specification = control("spec");
      if (nullptr == state || nullptr == request || nullptr == specification) return OB_SCHEMA_ERROR;
      const ObString &saved = specification->get_cur_default_value().get_string();
      AiColumnSpec spec = AiColumnSpec::deserialize({saved.ptr(), static_cast<size_t>(saved.length())});
      bool invalidated = !updating || old_predicate != new_predicate;
      if (updating && !deleting) {
        ObDatum *state_value = nullptr;
        if (OB_FAIL(datum(state->get_column_id(), false, state_value))) return ret;
        invalidated = invalidated || (!state_value->is_null() && state_value->get_int() == -1);
      }
      for (const auto &dependency : spec.dependencies) {
        if (!updating || invalidated) break;
        const auto *source = table->get_column_schema(ObString(dependency.size(), dependency.data()));
        bool different = false;
        if (nullptr == source) return OB_SCHEMA_ERROR;
        if (OB_FAIL(changed(source->get_column_id(), different))) return ret;
        invalidated = invalidated || different;
      }
      if (!deleting && !invalidated) continue;
      Pending entry{column, state, request, std::move(spec), {}};
      if (!deleting) {
        std::set<std::string> included;
        for (const auto &dependency : entry.spec.dependencies) {
          if (!included.insert(dependency).second) continue;
          const auto *source = table->get_column_schema(ObString(dependency.size(), dependency.data()));
          std::string value;
          if (nullptr == source) return OB_SCHEMA_ERROR;
          if (OB_FAIL(literal(source->get_column_id(), false, value))) return ret;
          if (!entry.source.empty()) entry.source += ',';
          entry.source += value + " AS " + ai_column_sql_identifier(dependency);
        }
        if (entry.source.empty()) entry.source = "1 AS __dummy";
      }
      pending.push_back(std::move(entry));
    }
    if (pending.empty()) return OB_SUCCESS;
    if (OB_FAIL(op.open_inner_conn())) return ret;
    auto nested = [&](auto action) {
      // Internal task writes are cascading DML, just like foreign-key actions;
      // the executor uses this marker to retain the root transaction context.
      const bool was_cascading = context.get_das_ctx().is_fk_cascading_;
      context.get_das_ctx().is_fk_cascading_ = true;
      int result = op.begin_nested_session(false);
      if (result != OB_SUCCESS) {
        context.get_das_ctx().is_fk_cascading_ = was_cascading;
        return result;
      }
      const bool was_internal = session->is_ai_column_internal();
      session->set_ai_column_internal(true);
      auto restore = [&](int *) {
        session->set_ai_column_internal(was_internal);
        const int end_ret = op.end_nested_session();
        context.get_das_ctx().is_fk_cascading_ = was_cascading;
        if (result == OB_SUCCESS) result = end_ret;
      };
      std::unique_ptr<int, decltype(restore)> guard(&result, restore);
      result = action();
      guard.reset();
      return result;
    };
    auto write = [&](const std::string &sql) {
      return nested([&]() { return op.execute_write(sql.c_str()); });
    };
    auto read = [&](const std::string &sql, const char *name, std::string &value) {
      return nested([&]() {
      int result = OB_SUCCESS;
      ObMySQLProxy::MySQLResult response;
      if (OB_SUCCESS != (result = op.execute_read(sql.c_str(), response))) {
      } else if (nullptr == response.get_result()) {
        result = OB_ERR_UNEXPECTED;
      } else {
        ObString text;
        if (OB_SUCCESS != (result = response.get_result()->next())) {
        } else if (OB_SUCCESS != (result = response.get_result()->get_varchar(name, text))) {
        } else {
          value.assign(text.ptr(), text.length());
        }
        const int close_ret = response.get_result()->close();
        if (result == OB_SUCCESS) result = close_ret;
      }
      return result;
      });
    };
    std::string request_id;
    if (!deleting) ret = read("SELECT REPLACE(UUID(),'-','') AS id", "id", request_id);
    for (const auto &entry : pending) {
      if (ret != OB_SUCCESS) break;
      const std::string key = "table_id=" + std::to_string(table->get_table_id())
          + " AND column_id=" + std::to_string(entry.column->get_column_id());
      if (deleting || updating) {
        ret = write("DELETE FROM oceanbase.__all_ai_column_task WHERE " + key
            + " AND rowkey_hash=MD5(" + ai_column_sql_string(old_predicate) + ")");
      }
      if (ret != OB_SUCCESS || deleting) continue;
      if (!updating || old_predicate != new_predicate) {
        ret = write("DELETE FROM oceanbase.__all_ai_column_task WHERE " + key
            + " AND rowkey_hash=MD5(" + ai_column_sql_string(new_predicate) + ")");
        if (ret != OB_SUCCESS) break;
      }
      std::string payload;
      ret = read("SELECT CAST(JSON_OBJECT('input'," + entry.spec.input_expression
          + ") AS CHAR) AS payload FROM (SELECT " + entry.source + ") __ai_input", "payload", payload);
      if (ret != OB_SUCCESS) break;
      const std::string sql = "INSERT INTO oceanbase.__all_ai_column_task(table_id,column_id,rowkey_hash,"
          "request_id,rowkey_predicate,request_payload,state,retry_count,eligible_ts,schema_version) VALUES("
          + std::to_string(table->get_table_id()) + "," + std::to_string(entry.column->get_column_id())
          + ",MD5(" + ai_column_sql_string(new_predicate) + ")," + ai_column_sql_string(request_id)
          + "," + ai_column_sql_string(new_predicate) + "," + ai_column_sql_string(payload)
          + ",0,0,NOW(6)," + std::to_string(table->get_schema_version())
          + ")";
      ret = write(sql);
      if (ret != OB_SUCCESS) LOG_WARN("AI COLUMN task insert failed", K(ret));
      if (ret != OB_SUCCESS) break;
      // Allocate request bytes in the outer execution arena; DAS may buffer the
      // row after this nested statement and this stack frame have returned.
      ObString stored;
      if (OB_FAIL(ob_write_string(context.get_allocator(), ObString(request_id.size(), request_id.data()), stored))) break;
      for (const auto &control : {std::make_pair(entry.column, 0), std::make_pair(entry.state, 1), std::make_pair(entry.request, 2)}) {
        const int64_t index = position(control.first->get_column_id());
        if (index < 0 || index >= definition.new_row_.count()) { ret = OB_SCHEMA_ERROR; break; }
        ObExpr *expression = definition.new_row_.at(index);
        ObDatum &value = expression->locate_datum_for_write(eval);
        if (control.second == 0) value.set_null();
        else if (control.second == 1) value.set_int(static_cast<int64_t>(share::AiColumnState::Pending));
        else value.set_string(stored);
        expression->get_eval_info(eval).evaluated_ = true;
        op.clear_dml_evaluated_flag(expression->parent_cnt_, expression->parents_);
      }
    }
  } catch (const std::exception &error) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, error.what());
  }
  return ret;
}

} }
