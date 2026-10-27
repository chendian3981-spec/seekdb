/* Copyright (c) 2026 OceanBase. Licensed under the Apache License, Version 2.0. */
#pragma once
#include "share/ai_service/ai_column_spec.h"

namespace oceanbase { namespace share { namespace schema {

// Called with the DDL transaction which persists the physical columns.
static int persist_ai_column_metadata(common::ObISQLClient &client, const ObTableSchema &table)
{
  using namespace common;
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table.get_column_count(); ++i) {
    const ObColumnSchemaV2 *column = table.get_column_schema_by_idx(i);
    if (nullptr == column || !column->is_ai_column()) continue;
    const ObString &name = column->get_column_name_str();
    const std::string text(name.ptr(), name.length());
    auto control = [&](const char *suffix) {
      const std::string hidden = AiColumnSpec::hidden_name(text, suffix);
      return table.get_column_schema(ObString(hidden.size(), hidden.data()));
    };
    const auto *state = control("state");
    const auto *request = control("request");
    const auto *specification = control("spec");
    if (nullptr == state || nullptr == request || nullptr == specification
        || !specification->get_cur_default_value().is_varchar()) return OB_SCHEMA_ERROR;
    const ObString &saved = specification->get_cur_default_value().get_string();
    try {
      const AiColumnSpec spec = AiColumnSpec::deserialize({saved.ptr(), static_cast<size_t>(saved.length())});
      ObDMLSqlSplicer dml;
      ObSqlString sql;
      if (OB_FAIL(dml.add_pk_column("table_id", table.get_table_id()))) {
      } else if (OB_FAIL(dml.add_pk_column("column_id", column->get_column_id()))) {
      } else if (OB_FAIL(dml.add_column("column_name", ObHexEscapeSqlStr(name)))) {
      } else if (OB_FAIL(dml.add_column("state_column_id", state->get_column_id()))) {
      } else if (OB_FAIL(dml.add_column("request_column_id", request->get_column_id()))) {
      } else if (OB_FAIL(dml.add_column("result_spec_column_id", specification->get_column_id()))) {
      } else if (OB_FAIL(dml.add_column("normalized_expr", ObHexEscapeSqlStr(ObString(spec.expression.size(), spec.expression.data()))))) {
      } else if (OB_FAIL(dml.add_column("specification", ObHexEscapeSqlStr(saved)))) {
      } else if (OB_FAIL(dml.add_column("max_retries", spec.max_retries))) {
      } else if (OB_FAIL(dml.add_column("schema_version", table.get_schema_version()))) {
      } else if (OB_FAIL(dml.splice_insert_update_sql("oceanbase.__all_ai_column", sql))) {
      } else {
        int64_t affected = 0;
        ret = client.write(sql.ptr(), affected);
      }
    } catch (const std::exception &error) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid persisted AI COLUMN specification", K(ret), "error", error.what());
    }
  }
  return ret;
}

} } }
