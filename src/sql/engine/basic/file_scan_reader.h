/*
 * Copyright (c) 2025 OceanBase.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <algorithm>
#include <charconv>
#include <cmath>
#include <cstdio>
#include <filesystem>
#include <iomanip>
#include <limits>
#include <map>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#ifdef SEEKDB_FILE_PARQUET
#include "local_bridge.h"
#endif
#ifndef _WIN32
#include <fcntl.h>
#include <glob.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace oceanbase { namespace sql {

enum class FileValueType { Null, Bigint, Double, Boolean, Varchar, Date, Datetime };

struct FileScanOptions
{
  std::string format = "csv";
  std::string path;
  std::string root;
  bool header = true;
  char delimiter = ',';
  char quote = '"';
  char escape = '"';
  std::string nullstr;
  std::string fingerprint;
};

struct FileScanCell
{
  std::string text;
  FileValueType type = FileValueType::Null;
};

struct FileScanColumn
{
  std::string name;
  FileValueType type = FileValueType::Null;
  bool nullable = false;
};

// A schema pass retains only column metadata. Execution reopens the same file
// descriptor at offset zero and keeps at most one bounded record in memory.
class FileScanReader
{
public:
  static constexpr size_t MAX_COLUMNS = 4096;
  static constexpr size_t MAX_RECORD_BYTES = 16 * 1024 * 1024;
  explicit FileScanReader(FileScanOptions options) : options_(std::move(options)) {}
  ~FileScanReader() { close(); }
  FileScanReader(const FileScanReader &) = delete;
  FileScanReader &operator=(const FileScanReader &) = delete;

  const std::vector<FileScanColumn> &columns() const { return columns_; }
  const FileScanOptions &options() const { return options_; }
  uint64_t row_number() const { return row_; }
  const std::vector<FileScanCell> &current() const { return current_; }
  bool next() { return next(current_); }
  std::string fingerprint() const
  {
    if (options_.format == "glob") return {};
#ifndef _WIN32
    return std::to_string(identity_.st_dev) + ":" + std::to_string(identity_.st_ino)
        + ":" + std::to_string(identity_.st_size) + ":" + std::to_string(identity_.st_mtim.tv_sec)
        + ":" + std::to_string(identity_.st_mtim.tv_nsec) + ":" + std::to_string(identity_.st_ctim.tv_sec)
        + ":" + std::to_string(identity_.st_ctim.tv_nsec);
#else
    return std::to_string(size_) + ":" + std::to_string(modified_.time_since_epoch().count());
#endif
  }
  void use_schema(std::vector<FileScanColumn> columns)
  {
    columns_ = std::move(columns);
    keys_.clear();
    for (size_t i = 0; i < columns_.size(); ++i) keys_[columns_[i].name] = i;
  }
  void project(const std::vector<size_t> &indices)
  {
#ifdef SEEKDB_FILE_PARQUET
    if (options_.format == "parquet") {
      if (!parquet_) fail("Parquet reader is not open");
      parquet_.value()->parquet_project(rust::Slice<const size_t>(indices.data(), indices.size()));
      projected_ = indices;
    }
#else
    (void)indices;
#endif
  }

  static std::string serialize_options(const FileScanOptions &options)
  {
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    writer.StartObject();
    auto string = [&](const char *key, const std::string &value) {
      writer.Key(key); writer.String(value.data(), value.size());
    };
    string("path", options.path); string("root", options.root); string("format", options.format);
    string("nullstr", options.nullstr); string("fingerprint", options.fingerprint);
    writer.Key("header"); writer.Bool(options.header);
    writer.Key("delimiter"); writer.Uint(static_cast<unsigned char>(options.delimiter));
    writer.Key("quote"); writer.Uint(static_cast<unsigned char>(options.quote));
    writer.Key("escape"); writer.Uint(static_cast<unsigned char>(options.escape));
    writer.EndObject();
    return {buffer.GetString(), buffer.GetSize()};
  }

  static FileScanOptions deserialize_options(const std::string &text)
  {
    rapidjson::Document doc;
    doc.Parse(text.data(), text.size());
    if (doc.HasParseError() || !doc.IsObject()) throw std::runtime_error("invalid file scan options");
    FileScanOptions result;
    auto string = [&](const char *key, std::string &value) {
      if (!doc.HasMember(key) || !doc[key].IsString()) throw std::runtime_error("invalid file scan string option");
      value.assign(doc[key].GetString(), doc[key].GetStringLength());
    };
    string("path", result.path); string("root", result.root); string("format", result.format);
    string("nullstr", result.nullstr); string("fingerprint", result.fingerprint);
    if (!doc.HasMember("header") || !doc["header"].IsBool()) throw std::runtime_error("invalid file scan header option");
    result.header = doc["header"].GetBool();
    auto character = [&](const char *key, char &value) {
      if (!doc.HasMember(key) || !doc[key].IsUint() || doc[key].GetUint() > 127) {
        throw std::runtime_error("invalid file scan character option");
      }
      value = static_cast<char>(doc[key].GetUint());
    };
    character("delimiter", result.delimiter); character("quote", result.quote); character("escape", result.escape);
    return result;
  }

  void open()
  {
    close();
    if (options_.root.empty() || options_.root == "NULL") fail("secure_file_priv is required");
    if (options_.format == "glob") {
      discover_files();
      return;
    }
    if (options_.delimiter == options_.quote || options_.delimiter == '\n'
        || options_.delimiter == '\r' || options_.delimiter == 0 || options_.quote == 0) {
      fail("invalid CSV delimiter or quote");
    }
    const auto root = std::filesystem::canonical(options_.root);
    if (!std::filesystem::is_directory(root)) fail("secure_file_priv must name a directory");
    const auto path = std::filesystem::canonical(options_.path);
    auto relative = path.lexically_relative(root);
    if (relative.empty() || relative.is_absolute() || *relative.begin() == "..") {
      fail("path is outside secure_file_priv");
    }
#ifndef _WIN32
    // Traverse the resolved path from / with no symlink following. Checking a
    // pathname and then using fopen alone permits a directory-symlink race.
    int directory = ::open("/", O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (directory < 0) fail("cannot open filesystem root");
    const auto parent = path.parent_path().relative_path();
    for (const auto &component : parent) {
      int child = ::openat(directory, component.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
      ::close(directory);
      if (child < 0) fail("cannot open directory without following symlinks");
      directory = child;
    }
    int descriptor = ::openat(directory, path.filename().c_str(), O_RDONLY | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC);
    ::close(directory);
    if (descriptor < 0) fail("cannot open input file");
    if (::fstat(descriptor, &identity_) != 0 || !S_ISREG(identity_.st_mode)) {
      ::close(descriptor);
      fail("input must be a regular file");
    }
    file_ = ::fdopen(descriptor, "rb");
    if (!file_) {
      ::close(descriptor);
      fail("cannot create file stream");
    }
#else
    if (!std::filesystem::is_regular_file(path)) fail("input must be a regular file");
    modified_ = std::filesystem::last_write_time(path);
    size_ = std::filesystem::file_size(path);
    file_ = std::fopen(path.string().c_str(), "rb");
    if (!file_) fail("cannot open input file");
#endif
    canonical_path_ = path.string();
    if (!options_.fingerprint.empty() && options_.fingerprint != fingerprint()) {
      close();
      fail("input file changed since query planning");
    }
    if (options_.format == "parquet") {
#ifdef SEEKDB_FILE_PARQUET
      parquet_.emplace(::oceanbase::local::parquet_open(::fileno(file_)));
      read_parquet_schema();
      return;
#else
      fail("Parquet reader is unavailable in this build");
#endif
    }
    rewind();
  }

  void close()
  {
#ifdef SEEKDB_FILE_PARQUET
    parquet_.reset();
#endif
    if (file_) std::fclose(file_);
    file_ = nullptr;
  }

  void check_unchanged() const
  {
    if (options_.format == "glob") return;
    if (!file_) fail("file is not open");
#ifndef _WIN32
    struct stat now{};
    struct stat path_now{};
    if (::fstat(::fileno(file_), &now) != 0 || ::stat(canonical_path_.c_str(), &path_now) != 0
        || now.st_dev != identity_.st_dev || now.st_ino != identity_.st_ino
        || path_now.st_dev != identity_.st_dev || path_now.st_ino != identity_.st_ino
        || now.st_size != identity_.st_size || now.st_mtim.tv_sec != identity_.st_mtim.tv_sec
        || now.st_mtim.tv_nsec != identity_.st_mtim.tv_nsec
        || now.st_ctim.tv_sec != identity_.st_ctim.tv_sec || now.st_ctim.tv_nsec != identity_.st_ctim.tv_nsec) {
      fail("input file changed during query");
    }
#else
    if (std::filesystem::last_write_time(canonical_path_) != modified_
        || std::filesystem::file_size(canonical_path_) != size_) fail("input file changed during query");
#endif
  }

  void rewind()
  {
    if (options_.format == "glob") { glob_position_ = 0; return; }
    check_unchanged();
    if (options_.format == "parquet") {
#ifdef SEEKDB_FILE_PARQUET
      parquet_.value()->parquet_rewind();
      return;
#else
      fail("Parquet reader is unavailable in this build");
#endif
    }
    std::rewind(file_);
    row_ = 0;
    line_ = 1;
    int first = std::fgetc(file_);
    if (first == 0xef) {
      if (std::fgetc(file_) != 0xbb || std::fgetc(file_) != 0xbf) fail("invalid UTF-8 BOM");
    } else if (first != EOF) {
      std::ungetc(first, file_);
    }
    if (options_.format == "csv" && options_.header) {
      std::vector<FileScanCell> header;
      csv_record(header);
    }
  }

  void infer_schema()
  {
    columns_.clear();
    keys_.clear();
    if (options_.format == "glob") {
      open();
      columns_.push_back({"file", FileValueType::Varchar, false});
      return;
    }
    if (options_.format == "parquet") {
      open();
      return;
    }
    if (!file_) open();
    check_unchanged();
    std::rewind(file_);
    row_ = 0;
    line_ = 1;
    skip_bom();
    if (options_.format == "csv") {
      std::vector<FileScanCell> first;
      if (csv_record(first)) {
        for (size_t i = 0; i < first.size(); ++i) {
          std::string name = options_.header ? first[i].text : "column" + std::to_string(i + 1);
          if (name.size() > 1024) fail("column name exceeds 1024 bytes");
          if (name.empty()) name = "column" + std::to_string(i + 1);
          const std::string base = name;
          unsigned suffix = 1;
          while (keys_.count(fold(name))) name = base + "__" + std::to_string(++suffix);
          keys_[fold(name)] = columns_.size();
          columns_.push_back({name});
        }
        if (!options_.header) merge_row(first);
      }
      std::vector<FileScanCell> cells;
      while (csv_record(cells)) merge_row(cells);
    } else if (options_.format == "json" || options_.format == "ndjson") {
      std::vector<FileScanCell> cells;
      while (json_record(cells, true)) merge_row(cells);
    } else {
      fail("unsupported file format");
    }
    for (auto &column : columns_) {
      if (column.type == FileValueType::Null) column.type = FileValueType::Varchar;
    }
    check_unchanged();
    rewind();
  }

  bool next(std::vector<FileScanCell> &cells)
  {
    if (options_.format == "glob") {
      if (glob_position_ == glob_files_.size()) return false;
      cells.assign(1, {glob_files_[glob_position_++], FileValueType::Varchar});
      return true;
    }
    check_unchanged();
    if (options_.format == "parquet") {
#ifdef SEEKDB_FILE_PARQUET
      return read_parquet_record(cells);
#else
      fail("Parquet reader is unavailable in this build");
#endif
    }
    const bool found = options_.format == "csv" ? csv_record(cells) : json_record(cells, false);
    if (found && cells.size() != columns_.size()) fail("record column count differs from schema");
    return found;
  }

  static bool parse_integer(const std::string &text, int64_t &result)
  {
    if (text.empty()) return false;
    const char *begin = text.data();
    if (*begin == '+') ++begin;
    auto parsed = std::from_chars(begin, text.data() + text.size(), result);
    return parsed.ec == std::errc() && parsed.ptr == text.data() + text.size();
  }

  static bool parse_double(const std::string &text, double &result)
  {
    if (text.empty()) return false;
    const char *begin = text.data();
    if (*begin == '+') ++begin;
    auto parsed = std::from_chars(begin, text.data() + text.size(), result);
    return parsed.ec == std::errc() && parsed.ptr == text.data() + text.size() && std::isfinite(result);
  }

  static FileValueType classify(const std::string &text)
  {
    if (text == "true" || text == "false" || text == "TRUE" || text == "FALSE") return FileValueType::Boolean;
    int64_t integer = 0;
    if (parse_integer(text, integer)) return FileValueType::Bigint;
    size_t digit_start = !text.empty() && (text[0] == '-' || text[0] == '+') ? 1 : 0;
    if (digit_start < text.size()
        && std::all_of(text.begin() + digit_start, text.end(), [](char c) { return c >= '0' && c <= '9'; })) {
      return FileValueType::Varchar; // Preserve integers beyond signed BIGINT.
    }
    double real = 0;
    if (parse_double(text, real)) return FileValueType::Double;
    if (valid_date(text)) return text.size() == 10 ? FileValueType::Date : FileValueType::Datetime;
    return FileValueType::Varchar;
  }

private:
#ifdef SEEKDB_FILE_PARQUET
  void read_parquet_schema()
  {
    const auto encoded = parquet_.value()->parquet_schema();
    rapidjson::Document schema;
    schema.Parse(encoded.data(), encoded.size());
    if (schema.HasParseError() || !schema.IsArray() || schema.Size() > MAX_COLUMNS) fail("invalid Parquet schema");
    columns_.clear();
    projected_.clear();
    for (const auto &entry : schema.GetArray()) {
      if (!entry.IsObject() || !entry.HasMember("name") || !entry["name"].IsString()
          || !entry.HasMember("type") || !entry["type"].IsString()
          || !entry.HasMember("nullable") || !entry["nullable"].IsBool()) fail("invalid Parquet column");
      FileScanColumn column;
      column.name.assign(entry["name"].GetString(), entry["name"].GetStringLength());
      const std::string type = entry["type"].GetString();
      column.type = type == "bigint" ? FileValueType::Bigint : type == "double" ? FileValueType::Double
          : type == "boolean" ? FileValueType::Boolean : type == "date" ? FileValueType::Date
          : type == "datetime" ? FileValueType::Datetime : FileValueType::Varchar;
      column.nullable = entry["nullable"].GetBool();
      projected_.push_back(columns_.size());
      columns_.push_back(std::move(column));
    }
  }
  bool read_parquet_record(std::vector<FileScanCell> &cells)
  {
    const auto encoded = parquet_.value()->parquet_next();
    if (encoded.empty()) return false;
    if (encoded.size() > MAX_RECORD_BYTES) fail("Parquet record exceeds 16 MiB");
    rapidjson::Document values;
    values.Parse(encoded.data(), encoded.size());
    if (values.HasParseError() || !values.IsArray() || values.Size() != projected_.size()) fail("invalid Parquet record");
    cells.assign(columns_.size(), {});
    for (size_t i = 0; i < projected_.size(); ++i) {
      const auto &value = values[i];
      FileScanCell &cell = cells[projected_[i]];
      if (value.IsNull()) continue;
      cell.type = columns_[projected_[i]].type;
      if (value.IsString()) cell.text.assign(value.GetString(), value.GetStringLength());
      else if (value.IsBool()) cell.text = value.GetBool() ? "true" : "false";
      else if (value.IsInt64()) cell.text = std::to_string(value.GetInt64());
      else if (value.IsNumber() && std::isfinite(value.GetDouble())) {
        std::ostringstream out;
        out << std::setprecision(std::numeric_limits<double>::max_digits10) << value.GetDouble();
        cell.text = out.str();
      } else fail("invalid Parquet scalar");
    }
    ++row_;
    return true;
  }
#endif
  void discover_files()
  {
    glob_files_.clear();
    glob_position_ = 0;
    const auto root = std::filesystem::canonical(options_.root);
    if (!std::filesystem::is_directory(root)) fail("secure_file_priv must name a directory");
#ifndef _WIN32
    glob_t matches{};
    const int result = ::glob(options_.path.c_str(), GLOB_ERR, nullptr, &matches);
    if (result != 0 && result != GLOB_NOMATCH) {
      ::globfree(&matches);
      fail("file discovery failed");
    }
    try {
      if (matches.gl_pathc > 4096) fail("glob exceeds 4096-file limit");
      for (size_t i = 0; i < matches.gl_pathc; ++i) {
        const auto path = std::filesystem::canonical(matches.gl_pathv[i]);
        const auto relative = path.lexically_relative(root);
        if (relative.empty() || relative.is_absolute() || *relative.begin() == "..") {
          fail("glob match is outside secure_file_priv");
        }
        if (std::filesystem::is_regular_file(path)) glob_files_.push_back(path.string());
      }
    } catch (...) {
      ::globfree(&matches);
      throw;
    }
    ::globfree(&matches);
#else
    fail("glob is not supported on this platform");
#endif
  }
  [[noreturn]] void fail(const char *message) const
  {
    throw std::runtime_error(options_.path + ": line " + std::to_string(line_) + ": " + message);
  }
  static std::string fold(std::string name)
  {
    for (char &c : name) if (c >= 'A' && c <= 'Z') c += 'a' - 'A';
    return name;
  }
  static bool valid_utf8(const std::string &text)
  {
    for (size_t i = 0; i < text.size();) {
      uint32_t lead = static_cast<unsigned char>(text[i++]);
      if (lead < 0x80) continue;
      int remaining = lead >= 0xc2 && lead <= 0xdf ? 1 : lead >= 0xe0 && lead <= 0xef ? 2 : lead >= 0xf0 && lead <= 0xf4 ? 3 : -1;
      if (remaining < 0 || i + remaining > text.size()) return false;
      uint32_t codepoint = lead & ((1u << (6 - remaining)) - 1);
      const int length = remaining + 1;
      while (remaining--) {
        unsigned char next = text[i++];
        if ((next & 0xc0) != 0x80) return false;
        codepoint = (codepoint << 6) | (next & 0x3f);
      }
      if ((length == 2 && codepoint < 0x80) || (length == 3 && codepoint < 0x800)
          || (length == 4 && codepoint < 0x10000) || codepoint > 0x10ffff
          || (codepoint >= 0xd800 && codepoint <= 0xdfff)) return false;
    }
    return true;
  }
  static bool valid_date(const std::string &s)
  {
    if (s.size() != 10 && s.size() != 19 && (s.size() < 21 || s.size() > 26)) return false;
    auto number = [&s](size_t start, size_t length) {
      int result = 0;
      for (size_t i = start; i < start + length; ++i) {
        if (s[i] < '0' || s[i] > '9') return -1;
        result = result * 10 + s[i] - '0';
      }
      return result;
    };
    int year = number(0, 4), month = number(5, 2), day = number(8, 2);
    if (s[4] != '-' || s[7] != '-' || year < 1 || month < 1 || month > 12 || day < 1) return false;
    constexpr int days[] = {31,28,31,30,31,30,31,31,30,31,30,31};
    int limit = days[month - 1] + (month == 2 && year % 4 == 0 && (year % 100 != 0 || year % 400 == 0));
    if (day > limit) return false;
    if (s.size() == 10) return true;
    if ((s[10] != 'T' && s[10] != ' ') || s[13] != ':' || s[16] != ':') return false;
    int hour = number(11, 2), minute = number(14, 2), second = number(17, 2);
    return hour >= 0 && hour < 24 && minute >= 0 && minute < 60 && second >= 0 && second < 60
        && (s.size() == 19 || (s[19] == '.' && number(20, s.size() - 20) >= 0));
  }
  static FileValueType widen(FileValueType a, FileValueType b)
  {
    if (a == FileValueType::Null) return b;
    if (b == FileValueType::Null || a == b) return a;
    if ((a == FileValueType::Date && b == FileValueType::Datetime)
        || (b == FileValueType::Date && a == FileValueType::Datetime)) return FileValueType::Datetime;
    auto numeric = [](FileValueType t) {
      return t == FileValueType::Boolean || t == FileValueType::Bigint || t == FileValueType::Double;
    };
    if (numeric(a) && numeric(b)) {
      return a == FileValueType::Double || b == FileValueType::Double ? FileValueType::Double : FileValueType::Bigint;
    }
    return FileValueType::Varchar;
  }
  void merge_row(const std::vector<FileScanCell> &cells)
  {
    if (cells.size() != columns_.size()) fail("record column count differs from header");
    for (size_t i = 0; i < cells.size(); ++i) {
      columns_[i].type = widen(columns_[i].type, cells[i].type);
      columns_[i].nullable |= cells[i].type == FileValueType::Null;
    }
  }
  void skip_bom()
  {
    int first = std::fgetc(file_);
    if (first == 0xef) {
      if (std::fgetc(file_) != 0xbb || std::fgetc(file_) != 0xbf) fail("invalid UTF-8 BOM");
    } else if (first != EOF) std::ungetc(first, file_);
  }
  void append(std::string &field, int c, size_t &bytes)
  {
    if (++bytes > MAX_RECORD_BYTES) fail("record exceeds 16 MiB limit");
    if (c == 0) fail("NUL byte in input");
    field.push_back(static_cast<char>(c));
  }
  bool csv_record(std::vector<FileScanCell> &cells)
  {
    cells.clear();
    std::string field;
    bool quoted = false, inside = false, closed = false, any = false;
    size_t bytes = 0;
    auto finish = [&]() {
      if (cells.size() >= MAX_COLUMNS) fail("too many columns");
      if (!valid_utf8(field)) fail("invalid UTF-8 field");
      const bool null = !quoted && field == options_.nullstr;
      cells.push_back({field, null ? FileValueType::Null : classify(field)});
      field.clear();
      quoted = inside = closed = false;
    };
    for (;;) {
      int c = std::fgetc(file_);
      if (c == EOF) {
        if (std::ferror(file_)) fail("file read failed");
        if (inside) fail("unterminated quoted field");
        if (!any) return false;
        finish();
        ++row_;
        return true;
      }
      any = true;
      if (inside) {
        if (c == options_.escape && options_.escape != options_.quote) {
          c = std::fgetc(file_);
          if (c == EOF) fail("unterminated field escape");
        } else if (c == options_.quote) {
          int next = std::fgetc(file_);
          if (next == options_.quote && options_.escape == options_.quote) {
            append(field, c, bytes);
          } else {
            if (next != EOF) std::ungetc(next, file_);
            inside = false;
            closed = true;
          }
          continue;
        }
        if (c == '\r') {
          int next = std::fgetc(file_);
          if (next != '\n' && next != EOF) std::ungetc(next, file_);
          c = '\n';
        }
        if (c == '\n') ++line_;
        append(field, c, bytes);
      } else if (c == options_.delimiter) {
        finish();
      } else if (c == '\n' || c == '\r') {
        if (c == '\r') {
          int next = std::fgetc(file_);
          if (next != '\n' && next != EOF) std::ungetc(next, file_);
        }
        ++line_;
        finish();
        ++row_;
        return true;
      } else if (closed) {
        fail("unexpected character after closing quote");
      } else if (c == options_.quote) {
        if (!field.empty()) fail("quote inside unquoted field");
        inside = quoted = true;
      } else {
        append(field, c, bytes);
      }
    }
  }
  bool json_record(std::vector<FileScanCell> &cells, bool inference)
  {
    std::string line;
    for (;;) {
      line.clear();
      int c = EOF;
      size_t bytes = 0;
      while ((c = std::fgetc(file_)) != EOF && c != '\n') append(line, c, bytes);
      if (std::ferror(file_)) fail("file read failed");
      if (c == EOF && line.empty()) return false;
      ++line_;
      if (line.find_first_not_of(" \t\r") != std::string::npos) break;
    }
    rapidjson::Document document;
    document.Parse<rapidjson::kParseValidateEncodingFlag>(line.data(), line.size());
    if (document.HasParseError() || !document.IsObject()) fail("expected one flat JSON object per line");
    std::map<std::string, FileScanCell> values;
    for (auto it = document.MemberBegin(); it != document.MemberEnd(); ++it) {
      const std::string key(it->name.GetString(), it->name.GetStringLength());
      if (key.size() > 1024 || key.find('\0') != std::string::npos) fail("invalid JSON field name");
      if (values.count(key)) fail("duplicate JSON object field");
      const auto &value = it->value;
      FileScanCell cell;
      if (value.IsNull()) {
      } else if (value.IsString()) {
        cell.text.assign(value.GetString(), value.GetStringLength());
        cell.type = valid_date(cell.text) ? (cell.text.size() == 10 ? FileValueType::Date : FileValueType::Datetime) : FileValueType::Varchar;
      } else if (value.IsBool()) {
        cell.text = value.GetBool() ? "true" : "false";
        cell.type = FileValueType::Boolean;
      } else if (value.IsInt64()) {
        cell.text = std::to_string(value.GetInt64());
        cell.type = FileValueType::Bigint;
      } else if (value.IsUint64()) {
        cell.text = std::to_string(value.GetUint64());
        cell.type = FileValueType::Varchar;
      } else if (value.IsDouble() && std::isfinite(value.GetDouble())) {
        std::ostringstream out;
        out << std::setprecision(std::numeric_limits<double>::max_digits10) << value.GetDouble();
        cell.text = out.str();
        cell.type = FileValueType::Double;
      } else {
        fail("nested JSON objects, arrays and nonfinite numbers are unsupported");
      }
      values.emplace(key, std::move(cell));
      if (inference && !keys_.count(key)) {
        if (columns_.size() >= MAX_COLUMNS) fail("too many columns");
        keys_[key] = columns_.size();
        columns_.push_back({key, FileValueType::Null, row_ != 0});
      } else if (!inference && !keys_.count(key)) {
        fail("file schema changed");
      }
    }
    cells.assign(columns_.size(), {});
    for (const auto &entry : values) cells[keys_.at(entry.first)] = entry.second;
    ++row_;
    return true;
  }

  FileScanOptions options_;
  std::string canonical_path_;
  FILE *file_ = nullptr;
  uint64_t row_ = 0;
  uint64_t line_ = 1;
  std::vector<FileScanColumn> columns_;
  std::vector<FileScanCell> current_;
#ifdef SEEKDB_FILE_PARQUET
  std::optional<rust::Box<::oceanbase::local::ParquetScan>> parquet_;
  std::vector<size_t> projected_;
#endif
  std::vector<std::string> glob_files_;
  size_t glob_position_ = 0;
  std::map<std::string, size_t> keys_;
#ifndef _WIN32
  struct stat identity_{};
#else
  std::filesystem::file_time_type modified_{};
  uintmax_t size_ = 0;
#endif
};

} } // namespace oceanbase::sql
