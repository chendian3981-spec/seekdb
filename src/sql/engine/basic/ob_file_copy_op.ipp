#pragma once
#include "sql/engine/basic/file_csv_writer.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include <rapidjson/document.h>

namespace oceanbase { namespace sql {

bool ObSelectIntoOp::is_file_copy() const
{
  const common::ObString &options = MY_SPEC.external_properties_.str_;
  static constexpr char prefix[] = "{\"file_sql_copy\":true,";
  return options.length() >= sizeof(prefix) - 1
      && 0 == std::memcmp(options.ptr(), prefix, sizeof(prefix) - 1);
}

int ObSelectIntoOp::run_file_copy(bool batch)
{
  using namespace common;
  int ret = OB_SUCCESS;
  bool enabled = false;
  ObSQLSessionInfo *session = ctx_.get_my_session();
  if (nullptr == session || nullptr == child_) return OB_ERR_UNEXPECTED;
  if (OB_FAIL(session->get_sys_variable(share::SYS_VAR_ENABLE_FILE_SQL, enabled))) return ret;
  if (!enabled) return OB_NOT_SUPPORTED;
  ObString root;
  if (OB_FAIL(session->get_secure_file_priv(root))) return ret;
  const ObString &encoded = MY_SPEC.external_properties_.str_;
  rapidjson::Document options;
  options.Parse(encoded.ptr(), encoded.length());
  if (options.HasParseError() || !options.IsObject() || !options.HasMember("delimiter")
      || !options["delimiter"].IsString() || options["delimiter"].GetStringLength() != 1
      || !options.HasMember("nullstr") || !options["nullstr"].IsString()
      || !options.HasMember("header") || !options["header"].IsBool()
      || !options.HasMember("names") || !options["names"].IsArray()) return OB_INVALID_ARGUMENT;
  try {
    const ObString path = MY_SPEC.outfile_name_.get_string();
    FileCsvWriter writer(std::string(path.ptr(), path.length()), std::string(root.ptr(), root.length()),
        options["delimiter"].GetString()[0], std::string(options["nullstr"].GetString(), options["nullstr"].GetStringLength()));
    std::vector<std::optional<std::string>> row;
    if (options["header"].GetBool()) {
      for (const auto &name : options["names"].GetArray()) {
        if (!name.IsString()) return OB_INVALID_ARGUMENT;
        row.emplace_back(std::string(name.GetString(), name.GetStringLength()));
      }
      writer.row(row);
    }
    // The buffer is reused per cell; memory stays bounded independently of the
    // number of rows exported by the native query pipeline.
    std::vector<char> text(16 * 1024 * 1024);
    auto emit_row = [&]() {
      int result = OB_SUCCESS;
      ObEvalCtx::TempAllocGuard alloc_guard(eval_ctx_);
      row.clear();
      for (int64_t i = 0; result == OB_SUCCESS && i < MY_SPEC.select_exprs_.count(); ++i) {
        ObExpr *expression = MY_SPEC.select_exprs_.at(i);
        ObDatum *datum = nullptr;
        ObObj value;
        if (OB_SUCCESS != (result = expression->eval(eval_ctx_, datum))) {
        } else if (nullptr == datum) {
          result = OB_ERR_UNEXPECTED;
        } else if (datum->is_null()) {
          row.emplace_back(std::nullopt);
        } else if (ob_is_string_type(expression->obj_meta_.get_type())) {
          ObString string;
          if (OB_SUCCESS == (result = ObTextStringHelper::read_real_string_data(
                  ctx_, alloc_guard.get_allocator(), *datum, expression->datum_meta_,
                  expression->obj_meta_.has_lob_header(), string))) {
            row.emplace_back(std::string(string.ptr(), string.length()));
          }
        } else if (OB_SUCCESS == (result = datum->to_obj(value, expression->obj_meta_))) {
          int64_t position = 0;
          if (OB_SUCCESS == (result = value.print_plain_str_literal(text.data(), text.size(), position))) {
            row.emplace_back(std::string(text.data(), position));
          }
        }
      }
      if (result == OB_SUCCESS) writer.row(row);
      return result;
    };
    int64_t count = 0;
    if (batch) {
      bool end = false;
      while (OB_SUCC(ret) && !end) {
        clear_evaluated_flag();
        const ObBatchRows *rows = nullptr;
        if (OB_FAIL(child_->get_next_batch(std::max<int64_t>(1, MY_SPEC.max_batch_size_), rows))) {
        } else if (nullptr == rows) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
          guard.set_batch_size(rows->size_);
          for (int64_t i = 0; OB_SUCC(ret) && i < rows->size_; ++i) {
            if (rows->skip_->contain(i)) continue;
            guard.set_batch_idx(i);
            ret = emit_row();
            if (OB_SUCC(ret)) ++count;
          }
          end = rows->end_;
        }
      }
    } else {
      while (OB_SUCC(ret)) {
        clear_evaluated_flag();
        if (OB_FAIL(child_->get_next_row())) {
        } else {
          ret = emit_row();
          if (OB_SUCC(ret)) ++count;
        }
      }
      if (ret == OB_ITER_END) ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret)) {
      writer.commit();
      ctx_.get_physical_plan_ctx()->set_affected_rows(count);
    }
  } catch (const std::exception &error) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, error.what());
  }
  return ret;
}

} }
