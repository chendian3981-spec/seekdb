/* Copyright (c) 2026 OceanBase. Licensed under the Apache License, Version 2.0. */
#pragma once
#include "share/ai_service/ai_column_spec.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"
#include "common/mysqlclient/ob_mysql_transaction.h"
#include "common/mysqlclient/ob_mysql_result.h"
#include "observer/ob_inner_sql_connection.h"
#include <charconv>
#include <cmath>
#include <cerrno>
#include <cstdlib>

namespace oceanbase { namespace omt {

namespace {
std::string ai_worker_string(const std::string &value)
{
  const char *digits = "0123456789abcdef";
  std::string hex;
  for (unsigned char c : value) { hex += digits[c >> 4]; hex += digits[c & 15]; }
  return "CONVERT(X'" + hex + "' USING utf8mb4)";
}
std::string ai_worker_identifier(const std::string &name)
{
  std::string result = "`";
  for (char c : name) { if (c == '`') result += '`'; result += c; }
  return result + '`';
}
struct AiWorkerTask
{
  uint64_t column_id = 0;
  int64_t retry_count = 0;
  share::AiColumnSpec specification;
  std::string payload;
  std::string result;
  std::string column;
  std::string state;
  std::string request;
  common::ObObjType type = common::ObNullType;
};
}

void ObAiService::run1()
{
  lib::set_thread_name("AiColumnWorker");
  while (!has_set_stop()) {
    if (nullptr != GCTX.sql_proxy_ && nullptr != GCTX.schema_service_
        && !GCTX.in_bootstrap_ && GCTX.start_service_time_ > 0) {
      int ret = OB_SUCCESS;
      try {
        ret = process_ai_column_group();
      } catch (const std::exception &error) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("AI COLUMN worker exception", "error", error.what());
      }
      if (ret != OB_SUCCESS && ret != OB_ITER_END && ret != OB_TABLE_NOT_EXIST
          && ret != OB_SCHEMA_EAGAIN && ret != OB_NOT_INIT) {
        LOG_WARN("AI COLUMN worker iteration failed", K(ret));
      }
      if (ret == OB_SUCCESS) continue;
    }
    // Short bounded idle interval also bounds service shutdown latency.
    ob_usleep(100000);
  }
}

int ObAiService::process_ai_column_group()
{
  using namespace common;
  using namespace share;
  using namespace share::schema;
  int ret = OB_SUCCESS;
  uint64_t table_id = 0;
  std::string request_id, hash, predicate;
  {
    ObMySQLProxy::MySQLResult response;
    if (OB_FAIL(GCTX.sql_proxy_->read(response,
        "SELECT table_id,request_id,rowkey_hash,rowkey_predicate FROM oceanbase.__all_ai_column_task "
        "WHERE eligible_ts<=NOW(6) ORDER BY eligible_ts,table_id,column_id LIMIT 1"))) return ret;
    auto *result = response.get_result();
    if (nullptr == result) return OB_ERR_UNEXPECTED;
    ObString request, rowkey, where;
    int64_t raw_table_id = 0;
    if (OB_FAIL(result->next())) {
    } else if (OB_FAIL(result->get_int("table_id", raw_table_id))) {
    } else if (OB_FAIL(result->get_varchar("request_id", request))) {
    } else if (OB_FAIL(result->get_varchar("rowkey_hash", rowkey))) {
    } else if (OB_FAIL(result->get_varchar("rowkey_predicate", where))) {
    } else {
      table_id = static_cast<uint64_t>(raw_table_id);
      request_id.assign(request.ptr(), request.length());
      hash.assign(rowkey.ptr(), rowkey.length());
      predicate.assign(where.ptr(), where.length());
    }
    const int close_ret = result->close();
    if (ret == OB_SUCCESS) ret = close_ret;
  }
  if (ret != OB_SUCCESS) return ret;
  const std::string group = "table_id=" + std::to_string(table_id)
      + " AND request_id=" + ai_worker_string(request_id) + " AND rowkey_hash=" + ai_worker_string(hash);
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table = nullptr;
  const ObDatabaseSchema *database = nullptr;
  if (OB_FAIL(GCTX.schema_service_->get_runtime_schema_guard(schema_guard))) return ret;
  if (OB_FAIL(schema_guard.get_table_schema(table_id, table))) return ret;
  if (nullptr == table || table->is_in_recyclebin()) {
    int64_t affected = 0;
    return GCTX.sql_proxy_->write(("DELETE FROM oceanbase.__all_ai_column_task WHERE " + group).c_str(), affected);
  }
  if (OB_FAIL(schema_guard.get_database_schema(table->get_database_id(), database))) return ret;
  if (nullptr == database) return OB_SCHEMA_ERROR;
  const std::string target = ai_worker_identifier(database->get_database_name()) + "." + ai_worker_identifier(table->get_table_name());
  std::vector<AiWorkerTask> tasks;
  {
    ObMySQLProxy::MySQLResult response;
    const std::string sql = "SELECT task.column_id,task.retry_count,CAST(task.request_payload AS CHAR) AS request_payload,"
        "CAST(metadata.specification AS CHAR) AS specification "
        "FROM oceanbase.__all_ai_column_task task JOIN oceanbase.__all_ai_column metadata "
        "ON metadata.table_id=task.table_id AND metadata.column_id=task.column_id WHERE task." + group
        + " ORDER BY task.column_id";
    if (OB_FAIL(GCTX.sql_proxy_->read(response, sql.c_str()))) return ret;
    auto *result = response.get_result();
    if (nullptr == result) return OB_ERR_UNEXPECTED;
    while (OB_SUCC(ret)) {
      AiWorkerTask task;
      int64_t raw_column_id = 0;
      ObString payload, spec;
      if (OB_FAIL(result->next())) {
      } else if (OB_FAIL(result->get_int("column_id", raw_column_id))) {
      } else if (OB_FAIL(result->get_int("retry_count", task.retry_count))) {
      } else if (OB_FAIL(result->get_varchar("request_payload", payload))) {
      } else if (OB_FAIL(result->get_varchar("specification", spec))) {
      } else {
        try {
          task.column_id = static_cast<uint64_t>(raw_column_id);
          task.payload.assign(payload.ptr(), payload.length());
          task.specification = AiColumnSpec::deserialize({spec.ptr(), static_cast<size_t>(spec.length())});
          const auto *column = table->get_column_schema(task.column_id);
          if (nullptr == column || !column->is_ai_column()) { ret = OB_SCHEMA_EAGAIN; break; }
          task.column = column->get_column_name();
          task.type = column->get_data_type();
          const auto state_name = AiColumnSpec::hidden_name(task.column, "state");
          const auto request_name = AiColumnSpec::hidden_name(task.column, "request");
          if (nullptr == table->get_column_schema(ObString(state_name.size(), state_name.data()))
              || nullptr == table->get_column_schema(ObString(request_name.size(), request_name.data()))) {
            ret = OB_SCHEMA_EAGAIN; break;
          }
          task.state = state_name; task.request = request_name;
          tasks.push_back(std::move(task));
        } catch (const std::exception &) { ret = OB_INVALID_ARGUMENT; }
      }
    }
    if (ret == OB_ITER_END) ret = OB_SUCCESS;
    const int close_ret = result->close();
    if (ret == OB_SUCCESS) ret = close_ret;
  }
  if (ret != OB_SUCCESS) return ret;
  if (tasks.empty()) {
    int64_t affected = 0;
    return GCTX.sql_proxy_->write(("DELETE FROM oceanbase.__all_ai_column_task WHERE " + group).c_str(), affected);
  }
  // The source transaction has committed before its tasks are visible here.
  // HTTP work takes place outside the result-publication transaction.
  int request_error = OB_SUCCESS;
  bool retry = false;
  int64_t retry_count = 0;
  for (auto &task : tasks) {
    ObArenaAllocator allocator;
    const auto &spec = task.specification;
    rapidjson::Document payload;
    payload.Parse(task.payload.data(), task.payload.size());
    ObString input;
    if (payload.HasParseError() || !payload.IsObject() || !payload.HasMember("input")) {
      request_error = OB_INVALID_ARGUMENT;
    } else if (payload["input"].IsString()) {
      input.assign_ptr(payload["input"].GetString(), payload["input"].GetStringLength());
    } else if (payload["input"].IsObject()) {
      rapidjson::StringBuffer buffer;
      rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
      payload["input"].Accept(writer);
      ObIJsonBase *base = nullptr;
      if (OB_SUCCESS != (request_error = ObJsonBaseFactory::get_json_base(
          &allocator, ObString(buffer.GetSize(), buffer.GetString()), ObJsonInType::JSON_TREE,
          ObJsonInType::JSON_TREE, base))) {
      } else if (nullptr == base || base->json_type() != ObJsonNodeType::J_OBJECT) {
        request_error = OB_INVALID_ARGUMENT;
      } else {
        request_error = ObAIFuncPromptObjectUtils::replace_all_str_args_in_template(
            allocator, static_cast<ObJsonObject *>(base), input);
      }
    } else {
      request_error = OB_INVALID_ARGUMENT;
    }
    ObAIFuncExprInfo *info = nullptr;
    ObAiModelEndpointInfo endpoint;
    ObString output;
    if (request_error != OB_SUCCESS) {
    } else if (OB_SUCCESS != (request_error = ObAIFuncUtils::get_ai_func_info(
        allocator, ObString(spec.model.size(), spec.model.data()), schema_guard, info))) {
    } else if (OB_SUCCESS != (request_error = share::ObAiServiceExecutor::read_ai_endpoint_by_ai_model_name(
        allocator, ObString(spec.model.size(), spec.model.data()), endpoint))) {
    } else if (nullptr == info) {
      request_error = OB_ERR_UNEXPECTED;
    } else {
      ObAIFuncModel model(allocator, *info, endpoint);
      if (spec.embedding) {
        ObJsonObject config(&allocator);
        ObJsonInt dimension(spec.dimension);
        if (OB_SUCCESS == (request_error = config.add("dimensions", &dimension))) {
          request_error = model.call_dense_embedding(input, &config, output);
        }
      } else {
        request_error = model.call_completion(input, nullptr, output,
            ObString(spec.system_prompt.size(), spec.system_prompt.data()));
      }
      if (request_error == OB_SUCCESS) {
        task.result.assign(output.ptr(), output.length());
        if (task.type == ObJsonType) {
          rapidjson::Document value;
          value.Parse(task.result.data(), task.result.size());
          if (value.HasParseError()) request_error = OB_INVALID_DATA;
        } else if (ob_is_integer_type(task.type)) {
          int64_t value = 0;
          const auto conversion = std::from_chars(task.result.data(), task.result.data() + task.result.size(), value);
          if (conversion.ec != std::errc() || conversion.ptr != task.result.data() + task.result.size()) request_error = OB_INVALID_DATA;
        } else if (ob_is_numeric_type(task.type)) {
          char *end = nullptr;
          errno = 0;
          const double value = std::strtod(task.result.c_str(), &end);
          if (end == task.result.c_str() || end != task.result.c_str() + task.result.size()
              || errno == ERANGE || !std::isfinite(value)
              || (task.type == ObFloatType && std::abs(value) > std::numeric_limits<float>::max())) {
            request_error = OB_INVALID_DATA;
          }
        }
      }
    }
    retry_count = std::max(retry_count, task.retry_count + 1);
    if (request_error != OB_SUCCESS) {
      retry = task.retry_count < spec.max_retries;
      break;
    }
  }
  ObMySQLTransaction transaction;
  if (OB_FAIL(transaction.start(GCTX.sql_proxy_))) return ret;
  auto *connection = static_cast<observer::ObInnerSQLConnection *>(transaction.get_connection());
  if (nullptr == connection) { transaction.end(false); return OB_ERR_UNEXPECTED; }
  const bool previous_internal = connection->get_session().is_ai_column_internal();
  connection->get_session().set_ai_column_internal(true);
  bool current = false;
  {
    std::string sql = "SELECT 1 AS current_request FROM " + target + " WHERE " + predicate;
    for (const auto &task : tasks) sql += " AND " + ai_worker_identifier(task.request) + "=" + ai_worker_string(request_id);
    sql += " FOR UPDATE";
    ObMySQLProxy::MySQLResult response;
    if (OB_FAIL(transaction.read(response, sql.c_str()))) {
    } else if (nullptr == response.get_result()) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      const int next = response.get_result()->next();
      current = next == OB_SUCCESS;
      if (next != OB_SUCCESS && next != OB_ITER_END) ret = next;
      const int close_ret = response.get_result()->close();
      if (ret == OB_SUCCESS) ret = close_ret;
    }
  }
  int64_t affected = 0;
  if (OB_SUCC(ret) && current) {
    std::string sql = "UPDATE " + target + " SET ";
    for (size_t i = 0; i < tasks.size(); ++i) {
      const auto &task = tasks[i];
      if (i) sql += ',';
      const int64_t state = request_error == OB_SUCCESS ? static_cast<int64_t>(AiColumnState::Ready)
          : retry ? static_cast<int64_t>(AiColumnState::Retry) : static_cast<int64_t>(AiColumnState::Failed);
      sql += ai_worker_identifier(task.state) + '=' + std::to_string(state) + ','
          + ai_worker_identifier(task.column) + '=';
      if (request_error != OB_SUCCESS) sql += "NULL";
      else if (task.type == ObJsonType) sql += "CAST(" + ai_worker_string(task.result) + " AS JSON)";
      else sql += ai_worker_string(task.result);
    }
    sql += " WHERE " + predicate;
    if (OB_FAIL(transaction.write(sql.c_str(), affected))) {
      LOG_WARN("AI COLUMN result publication failed", K(ret), K(table_id));
    }
  }
  if (OB_SUCC(ret)) {
    std::string sql;
    if (current && request_error != OB_SUCCESS && retry) {
      const int64_t delay = INT64_C(1) << std::min<int64_t>(retry_count - 1, 5);
      sql = "UPDATE oceanbase.__all_ai_column_task SET state=3,retry_count=" + std::to_string(retry_count)
          + ",eligible_ts=DATE_ADD(NOW(6),INTERVAL " + std::to_string(delay) + " SECOND),last_error="
          + ai_worker_string("AI request failed: " + std::to_string(request_error)) + " WHERE " + group;
    } else {
      sql = "DELETE FROM oceanbase.__all_ai_column_task WHERE " + group;
    }
    ret = transaction.write(sql.c_str(), affected);
  }
  connection->get_session().set_ai_column_internal(previous_internal);
  const int end_ret = transaction.end(ret == OB_SUCCESS);
  if (ret == OB_SUCCESS) ret = end_ret;
  if (ret != OB_SUCCESS) {
    // A transient SQL publication failure must not monopolize the queue or
    // immediately issue another HTTP request for the same row.
    int64_t ignored = 0;
    const std::string defer = "UPDATE oceanbase.__all_ai_column_task "
        "SET eligible_ts=DATE_ADD(NOW(6),INTERVAL 1 SECOND) WHERE " + group;
    const int defer_ret = GCTX.sql_proxy_->write(defer.c_str(), ignored);
    if (defer_ret != OB_SUCCESS) LOG_WARN("could not defer AI COLUMN publication retry", K(defer_ret));
  }
  return ret;
}

} }
