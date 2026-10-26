/* Copyright (c) 2025 OceanBase. Licensed under the Apache License, Version 2.0. */
#pragma once
#include <cerrno>
#include <atomic>
#include <cstdio>
#include <filesystem>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>
#ifndef _WIN32
#include <fcntl.h>
#include <unistd.h>
#endif

namespace oceanbase { namespace sql {

// Publish a complete file without replacing an existing destination. All path
// traversal uses directory descriptors so concurrent symlink changes cannot
// redirect writes outside secure_file_priv.
class FileCsvWriter
{
public:
  FileCsvWriter(const std::string &path, const std::string &root, char delimiter,
                const std::string &nullstr)
      : delimiter_(delimiter), nullstr_(nullstr)
  {
#ifndef _WIN32
    namespace fs = std::filesystem;
    if (root.empty() || root == "NULL" || path.find('\0') != std::string::npos) {
      throw std::runtime_error("COPY requires a permitted local output path");
    }
    const fs::path output = fs::absolute(path).lexically_normal();
    const fs::path allowed = fs::canonical(root);
    const fs::path parent = fs::canonical(output.parent_path());
    const fs::path relative = parent.lexically_relative(allowed);
    if (relative.empty() || *relative.begin() == ".." || !fs::is_directory(allowed)) {
      throw std::runtime_error("COPY output is outside secure_file_priv");
    }
    name_ = output.filename().string();
    if (name_.empty() || name_ == "." || name_ == "..") throw std::runtime_error("invalid COPY filename");
    int directory = ::open("/", O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (directory < 0) throw std::runtime_error("cannot open COPY output directory");
    for (const auto &component : parent.relative_path()) {
      const int next = ::openat(directory, component.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
      ::close(directory);
      directory = next;
      if (directory < 0) throw std::runtime_error("COPY directory changed or is inaccessible");
    }
    directory_ = directory;
    for (unsigned attempt = 0; attempt < 100; ++attempt) {
      temporary_ = ".seekdb-copy-" + std::to_string(::getpid()) + "-" + std::to_string(next_id_++);
      const int fd = ::openat(directory_, temporary_.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0600);
      if (fd >= 0) {
        file_ = ::fdopen(fd, "wb");
        if (nullptr == file_) {
          ::close(fd);
          ::unlinkat(directory_, temporary_.c_str(), 0);
        }
        break;
      }
      if (errno != EEXIST) break;
    }
    if (nullptr == file_) {
      ::close(directory_); directory_ = -1;
      throw std::runtime_error("cannot create COPY output file");
    }
#else
    throw std::runtime_error("COPY local file SQL requires POSIX file descriptors");
#endif
  }

  ~FileCsvWriter()
  {
    if (nullptr != file_) std::fclose(file_);
#ifndef _WIN32
    if (directory_ >= 0) {
      if (!temporary_.empty()) ::unlinkat(directory_, temporary_.c_str(), 0);
      ::close(directory_);
    }
#endif
  }
  FileCsvWriter(const FileCsvWriter &) = delete;
  FileCsvWriter &operator=(const FileCsvWriter &) = delete;

  void row(const std::vector<std::optional<std::string>> &cells)
  {
    std::string record;
    for (size_t i = 0; i < cells.size(); ++i) {
      if (i) record += delimiter_;
      if (!cells[i]) {
        record += nullstr_;
      } else {
        record += '"';
        for (char c : *cells[i]) {
          if (c == '"') record += '"';
          record += c;
        }
        record += '"';
      }
      if (record.size() > 16 * 1024 * 1024) throw std::runtime_error("COPY record exceeds 16 MiB");
    }
    record += '\n';
    if (std::fwrite(record.data(), 1, record.size(), file_) != record.size()) {
      throw std::runtime_error("failed to write COPY output");
    }
  }

  void commit()
  {
#ifndef _WIN32
    if (std::fflush(file_) != 0 || ::fsync(::fileno(file_)) != 0) throw std::runtime_error("failed to flush COPY output");
    FILE *file = file_; file_ = nullptr;
    if (std::fclose(file) != 0) throw std::runtime_error("failed to close COPY output");
    if (::linkat(directory_, temporary_.c_str(), directory_, name_.c_str(), 0) != 0) {
      throw std::runtime_error("COPY destination exists or cannot be published");
    }
    ::unlinkat(directory_, temporary_.c_str(), 0);
    temporary_.clear();
#endif
  }
private:
  inline static std::atomic<unsigned long long> next_id_{0};
  FILE *file_ = nullptr;
  int directory_ = -1;
  std::string name_;
  std::string temporary_;
  char delimiter_;
  std::string nullstr_;
};

} }
