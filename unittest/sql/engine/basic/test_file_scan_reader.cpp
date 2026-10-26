/* Copyright (c) 2025 OceanBase. Licensed under the Apache License, Version 2.0. */
#include <gtest/gtest.h>
#include <fstream>
#include "sql/engine/basic/file_scan_reader.h"
#include "sql/engine/basic/file_csv_writer.h"

using namespace oceanbase::sql;

TEST(FileCsvWriterTest, AtomicCsvRoundTripAndNoOverwrite)
{
  const auto directory = std::filesystem::temp_directory_path() / ("seekdb_copy_" + std::to_string(::getpid()));
  std::filesystem::create_directory(directory);
  const auto path = directory / "output.csv";
  {
    FileCsvWriter writer(path.string(), directory.string(), ',', "NULL");
    writer.row({"id", "text", "empty", "absent"});
    writer.row({"1", "hello,\n\"世界\"", "", std::nullopt});
    EXPECT_FALSE(std::filesystem::exists(path));
    writer.commit();
  }
  FileScanOptions options;
  options.path = path.string(); options.root = directory.string(); options.nullstr = "NULL";
  FileScanReader reader(options);
  reader.infer_schema();
  ASSERT_TRUE(reader.next());
  EXPECT_EQ("hello,\n\"世界\"", reader.current()[1].text);
  EXPECT_EQ(FileValueType::Varchar, reader.current()[2].type);
  EXPECT_EQ(FileValueType::Null, reader.current()[3].type);
  {
    FileCsvWriter writer(path.string(), directory.string(), ',', "");
    writer.row({"replacement"});
    EXPECT_THROW(writer.commit(), std::runtime_error);
  }
  EXPECT_THROW(FileCsvWriter((directory.parent_path() / "escaped.csv").string(), directory.string(), ',', ""), std::runtime_error);
  for (const auto &entry : std::filesystem::directory_iterator(directory)) EXPECT_EQ(path, entry.path());
  std::filesystem::remove(path);
  std::filesystem::remove(directory);
}

TEST(FileCsvWriterTest, FailedExportRemovesTemporaryFile)
{
  const auto directory = std::filesystem::temp_directory_path() / ("seekdb_copy_abort_" + std::to_string(::getpid()));
  std::filesystem::create_directory(directory);
  {
    FileCsvWriter writer((directory / "output.csv").string(), directory.string(), ',', "");
    writer.row({"unfinished"});
  }
  EXPECT_TRUE(std::filesystem::is_empty(directory));
  std::filesystem::remove(directory);
}

class FileScanReaderTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    directory = std::filesystem::temp_directory_path() / ("seekdb_file_scan_" + std::to_string(::getpid()));
    std::filesystem::create_directory(directory);
  }
  void TearDown() override
  {
    for (const auto &path : created) std::filesystem::remove(path);
    std::filesystem::remove(directory);
  }
  FileScanOptions data(const char *name, const std::string &content, const char *format = "csv")
  {
    auto path = directory / name;
    std::ofstream(path, std::ios::binary) << content;
    created.push_back(path);
    FileScanOptions options;
    options.path = path.string();
    options.root = directory.string();
    options.format = format;
    return options;
  }
  std::filesystem::path directory;
  std::vector<std::filesystem::path> created;
};

TEST_F(FileScanReaderTest, CsvSchemaMultilineEscapesNullAndRescan)
{
  auto options = data("records.csv", "id,id,score,active,day,note\r\n1,10,1.5,true,2024-02-29,\"hello,\r\nworld\"\r\n2,20,2,false,,\"say \"\"hi\"\"\"\r\n");
  FileScanReader reader(options);
  reader.infer_schema();
  ASSERT_EQ(6, reader.columns().size());
  EXPECT_EQ("id__2", reader.columns()[1].name);
  EXPECT_EQ(FileValueType::Bigint, reader.columns()[0].type);
  EXPECT_EQ(FileValueType::Double, reader.columns()[2].type);
  EXPECT_EQ(FileValueType::Boolean, reader.columns()[3].type);
  EXPECT_EQ(FileValueType::Date, reader.columns()[4].type);
  EXPECT_TRUE(reader.columns()[4].nullable);
  std::vector<FileScanCell> row;
  ASSERT_TRUE(reader.next(row));
  EXPECT_EQ("hello,\nworld", row[5].text);
  ASSERT_TRUE(reader.next(row));
  EXPECT_EQ(FileValueType::Null, row[4].type);
  EXPECT_EQ("say \"hi\"", row[5].text);
  EXPECT_FALSE(reader.next(row));
  reader.rewind();
  ASSERT_TRUE(reader.next(row));
  EXPECT_EQ("1", row[0].text);
}

TEST_F(FileScanReaderTest, JsonSchemaUnionAndMissingValues)
{
  auto options = data("records.jsonl", "{\"id\":1,\"score\":1.5}\n{\"id\":2,\"name\":\"你好\"}\n{\"id\":3,\"score\":4,\"name\":null}\n", "json");
  FileScanReader reader(options);
  reader.infer_schema();
  ASSERT_EQ(3, reader.columns().size());
  EXPECT_EQ("id", reader.columns()[0].name);
  EXPECT_EQ("score", reader.columns()[1].name);
  EXPECT_EQ("name", reader.columns()[2].name);
  EXPECT_TRUE(reader.columns()[1].nullable);
  EXPECT_TRUE(reader.columns()[2].nullable);
  EXPECT_EQ(FileValueType::Double, reader.columns()[1].type);
  std::vector<FileScanCell> row;
  ASSERT_TRUE(reader.next(row));
  EXPECT_EQ(FileValueType::Null, row[2].type);
  ASSERT_TRUE(reader.next(row));
  EXPECT_EQ("你好", row[2].text);
  EXPECT_EQ(FileValueType::Null, row[1].type);
}

TEST_F(FileScanReaderTest, RejectMalformedAndNestedInput)
{
  for (const char *content : {"a,b\n1\n", "a\n\"unterminated", "a\n\"closed\"tail\n"}) {
    FileScanReader reader(data("bad.csv", content));
    EXPECT_THROW(reader.infer_schema(), std::runtime_error);
  }
  for (const char *content : {"{\"a\":{}}\n", "{\"a\":[]}\n", "{broken}\n"}) {
    FileScanReader reader(data("bad.jsonl", content, "json"));
    EXPECT_THROW(reader.infer_schema(), std::runtime_error);
  }
}

TEST_F(FileScanReaderTest, RestrictPathsAndDetectReplacement)
{
  auto options = data("changing.csv", "id\n1\n");
  FileScanReader reader(options);
  reader.infer_schema();
  std::ofstream(options.path, std::ios::app) << "2\n";
  std::vector<FileScanCell> row;
  EXPECT_THROW(reader.next(row), std::runtime_error);
  options.path = "/etc/passwd";
  FileScanReader outside(options);
  EXPECT_THROW(outside.open(), std::runtime_error);
  options = data("symlink.csv", "id\n1\n");
  auto link = directory / "escape.csv";
  std::filesystem::create_symlink("/etc/passwd", link);
  created.push_back(link);
  options.path = link.string();
  FileScanReader symlink(options);
  EXPECT_THROW(symlink.open(), std::runtime_error);
}

TEST_F(FileScanReaderTest, NumericBoundsAndCalendarValidation)
{
  EXPECT_EQ(FileValueType::Bigint, FileScanReader::classify("9223372036854775807"));
  EXPECT_EQ(FileValueType::Bigint, FileScanReader::classify("-9223372036854775808"));
  EXPECT_EQ(FileValueType::Varchar, FileScanReader::classify("9223372036854775808"));
  EXPECT_EQ(FileValueType::Varchar, FileScanReader::classify("1e9999"));
  EXPECT_EQ(FileValueType::Varchar, FileScanReader::classify("2023-02-29"));
  EXPECT_EQ(FileValueType::Date, FileScanReader::classify("2024-02-29"));
  EXPECT_EQ(FileValueType::Datetime, FileScanReader::classify("2024-02-29T12:13:14.123456"));
  EXPECT_EQ(FileValueType::Varchar, FileScanReader::classify("2024-02-29T25:13:14"));
}

TEST_F(FileScanReaderTest, GlobAndSerializedSchemaFingerprint)
{
  auto options = data("one.csv", "id\n1\n");
  data("two.csv", "id\n2\n");
  FileScanReader source(options);
  source.infer_schema();
  options.fingerprint = source.fingerprint();
  auto restored = FileScanReader::deserialize_options(FileScanReader::serialize_options(options));
  EXPECT_EQ(options.path, restored.path);
  EXPECT_EQ(options.fingerprint, restored.fingerprint);
  FileScanReader planned(restored);
  planned.open();
  planned.use_schema(source.columns());
  ASSERT_TRUE(planned.next());
  EXPECT_EQ("1", planned.current()[0].text);
  std::ofstream(options.path, std::ios::app) << "3\n";
  FileScanReader stale(restored);
  EXPECT_THROW(stale.open(), std::runtime_error);
  options.format = "glob";
  options.path = (directory / "*.csv").string();
  options.fingerprint.clear();
  FileScanReader files(options);
  files.infer_schema();
  ASSERT_EQ(1, files.columns().size());
  EXPECT_EQ("file", files.columns()[0].name);
  EXPECT_TRUE(files.next());
  EXPECT_TRUE(files.next());
  EXPECT_FALSE(files.next());
  files.rewind();
  EXPECT_TRUE(files.next());
}

TEST_F(FileScanReaderTest, RejectInvalidUtf8AndKeepQuotedEmptyString)
{
  FileScanReader invalid(data("invalid.csv", std::string("text\n") + char(0xc0) + char(0x80) + "\n"));
  EXPECT_THROW(invalid.infer_schema(), std::runtime_error);
  FileScanReader reader(data("empty.csv", "a,b\n\"\",\n"));
  reader.infer_schema();
  ASSERT_TRUE(reader.next());
  EXPECT_EQ(FileValueType::Varchar, reader.current()[0].type);
  EXPECT_EQ(FileValueType::Null, reader.current()[1].type);
}
