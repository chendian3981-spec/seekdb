/* Copyright (c) 2026 OceanBase. Licensed under the Apache License, Version 2.0. */
#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include <stdexcept>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace oceanbase { namespace share {

enum class AiColumnState : int64_t { Pending = 0, Ready = 1, Failed = 2, Retry = 3 };

struct AiColumnSpec
{
  std::string expression;
  std::string input_expression;
  std::string model;
  std::string system_prompt;
  std::vector<std::string> dependencies;
  int64_t max_retries = 3;
  int64_t dimension = 0;
  bool embedding = false;

  static std::string hidden_name(const std::string &column, const char *suffix)
  {
    return "__ai_column_" + column + "_" + suffix;
  }
  std::string serialize() const
  {
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    writer.StartObject();
    writer.Key("version"); writer.Int(1);
    auto string = [&](const char *key, const std::string &value) {
      writer.Key(key); writer.String(value.data(), value.size());
    };
    string("expression", expression); string("input_expression", input_expression);
    string("model", model); string("system_prompt", system_prompt);
    writer.Key("max_retries"); writer.Int64(max_retries);
    writer.Key("dimension"); writer.Int64(dimension);
    writer.Key("embedding"); writer.Bool(embedding);
    writer.Key("dependencies"); writer.StartArray();
    for (const auto &name : dependencies) writer.String(name.data(), name.size());
    writer.EndArray(); writer.EndObject();
    return {buffer.GetString(), buffer.GetSize()};
  }
  static AiColumnSpec deserialize(const std::string &encoded)
  {
    rapidjson::Document doc;
    doc.Parse(encoded.data(), encoded.size());
    if (doc.HasParseError() || !doc.IsObject() || !doc.HasMember("version")
        || !doc["version"].IsInt() || doc["version"].GetInt() != 1) {
      throw std::runtime_error("invalid AI COLUMN specification version");
    }
    AiColumnSpec result;
    auto string = [&](const char *key, std::string &value) {
      if (!doc.HasMember(key) || !doc[key].IsString()) throw std::runtime_error("invalid AI COLUMN string field");
      value.assign(doc[key].GetString(), doc[key].GetStringLength());
    };
    string("expression", result.expression); string("input_expression", result.input_expression);
    string("model", result.model); string("system_prompt", result.system_prompt);
    if (!doc.HasMember("max_retries") || !doc["max_retries"].IsInt64()
        || doc["max_retries"].GetInt64() < 0 || doc["max_retries"].GetInt64() > 100
        || !doc.HasMember("dimension") || !doc["dimension"].IsInt64()
        || doc["dimension"].GetInt64() < 0 || !doc.HasMember("embedding") || !doc["embedding"].IsBool()
        || !doc.HasMember("dependencies") || !doc["dependencies"].IsArray()) {
      throw std::runtime_error("invalid AI COLUMN specification fields");
    }
    result.max_retries = doc["max_retries"].GetInt64();
    result.dimension = doc["dimension"].GetInt64();
    result.embedding = doc["embedding"].GetBool();
    for (const auto &name : doc["dependencies"].GetArray()) {
      if (!name.IsString()) throw std::runtime_error("invalid AI COLUMN dependency");
      result.dependencies.emplace_back(name.GetString(), name.GetStringLength());
    }
    if (result.model.empty() || result.expression.empty() || result.input_expression.empty()
        || (result.embedding && result.dimension == 0)) throw std::runtime_error("incomplete AI COLUMN specification");
    return result;
  }
};

} }
