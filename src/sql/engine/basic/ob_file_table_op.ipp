/* Copyright (c) 2025 OceanBase. Licensed under the Apache License, Version 2.0. */
#pragma once

namespace oceanbase { namespace sql {

int ObJsonTableOp::open_file_scan()
{
  using namespace common;
  int ret = OB_SUCCESS;
  bool enabled = false;
  ObSQLSessionInfo *session = get_exec_ctx().get_my_session();
  ObDatum *config = nullptr;
  if (nullptr == session || MY_SPEC.value_exprs_.count() != 1) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(session->get_sys_variable(share::SYS_VAR_ENABLE_FILE_SQL, enabled))) {
  } else if (!enabled) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "file SQL is disabled");
  } else if (OB_FAIL(MY_SPEC.value_exprs_.at(0)->eval(eval_ctx_, config))) {
  } else if (nullptr == config || config->is_null()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    try {
      ObString text = config->get_string();
      FileScanOptions options = FileScanReader::deserialize_options({text.ptr(), static_cast<size_t>(text.length())});
      // A cached plan must respect the current privilege setting as well.
      ObString root;
      if (OB_FAIL(session->get_secure_file_priv(root))) return ret;
      options.root.assign(root.ptr(), root.length());
      delete file_reader_;
      file_reader_ = nullptr;
      file_reader_ = new FileScanReader(options);
      file_reader_->open();
      std::vector<FileScanColumn> schema;
      for (int64_t i = 1; i < MY_SPEC.cols_def_.count(); ++i) {
        const ObJtColInfo &info = *MY_SPEC.cols_def_.at(i);
        FileScanColumn column;
        column.name.assign(info.col_name_.ptr(), info.col_name_.length());
        switch (info.data_type_.get_obj_type()) {
          case ObIntType: column.type = FileValueType::Bigint; break;
          case ObDoubleType: column.type = FileValueType::Double; break;
          case ObTinyIntType: column.type = FileValueType::Boolean; break;
          case ObDateType: column.type = FileValueType::Date; break;
          case ObDateTimeType: column.type = FileValueType::Datetime; break;
          default: column.type = FileValueType::Varchar; break;
        }
        schema.push_back(std::move(column));
      }
      file_reader_->use_schema(std::move(schema));
      std::vector<size_t> projected;
      for (int64_t i = 1; i < MY_SPEC.cols_def_.count(); ++i) {
        if (MY_SPEC.cols_def_.at(i)->output_column_idx_ >= 0) projected.push_back(i - 1);
      }
      file_reader_->project(projected);
    } catch (const std::exception &error) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, error.what());
      delete file_reader_;
      file_reader_ = nullptr;
    }
  }
  return ret;
}

int ObJsonTableOp::next_file_row()
{
  using namespace common;
  int ret = OB_SUCCESS;
  if (nullptr == file_reader_) return OB_ERR_UNEXPECTED;
  try {
    // String datums reference the reader's current record until it advances.
    if (!file_reader_->next()) return OB_ITER_END;
    const auto &cells = file_reader_->current();
    clear_evaluated_flag();
    for (int64_t i = 1; OB_SUCC(ret) && i < MY_SPEC.cols_def_.count(); ++i) {
      const ObJtColInfo &info = *MY_SPEC.cols_def_.at(i);
      if (info.output_column_idx_ < 0) continue;
      if (info.output_column_idx_ >= MY_SPEC.column_exprs_.count()
          || i - 1 >= cells.size()) return OB_ERR_UNEXPECTED;
      ObExpr *expression = MY_SPEC.column_exprs_.at(info.output_column_idx_);
      ObDatum &datum = expression->locate_datum_for_write(eval_ctx_);
      const auto &cell = cells[i - 1];
      ObObj value;
      if (cell.type == FileValueType::Null) {
        datum.set_null();
      } else {
        const ObString text(cell.text.size(), cell.text.data());
        const bool boolean = cell.text == "true" || cell.text == "TRUE";
        switch (info.data_type_.get_obj_type()) {
          case ObIntType: {
            int64_t number = 0;
            if (cell.type == FileValueType::Boolean) number = boolean;
            else if (!FileScanReader::parse_integer(cell.text, number)) ret = OB_INVALID_DATA;
            value.set_int(number);
            break;
          }
          case ObDoubleType: {
            double number = 0;
            if (cell.type == FileValueType::Boolean) number = boolean;
            else if (!FileScanReader::parse_double(cell.text, number)) ret = OB_INVALID_DATA;
            value.set_double(number);
            break;
          }
          case ObTinyIntType: value.set_tinyint(boolean ? 1 : 0); break;
          case ObDateType: {
            int32_t date = 0;
            ret = ObTimeConverter::str_to_date(text, date);
            value.set_date(date);
            break;
          }
          case ObDateTimeType: {
            int64_t date = 0;
            ObTimeConvertCtx conversion(nullptr, false);
            ret = ObTimeConverter::str_to_datetime(text, conversion, date);
            value.set_datetime(date);
            break;
          }
          default:
            value.set_varchar(text);
            value.set_collation_type(CS_TYPE_UTF8MB4_BIN);
            break;
        }
        if (OB_SUCC(ret)) ret = datum.from_obj(value, expression->obj_datum_map_);
      }
      if (OB_SUCC(ret)) expression->set_evaluated_projected(eval_ctx_);
    }
  } catch (const std::exception &error) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, error.what());
  }
  return ret;
}

} } // namespace oceanbase::sql
