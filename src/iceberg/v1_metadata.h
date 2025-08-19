/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

/// \file iceberg/v1_metadata.h

#include <cstdint>

#include "iceberg/manifest_entry.h"
#include "iceberg/manifest_list.h"

namespace iceberg {

/// \brief v1 metadata wrapper.
///
/// Wrapper for v1 manifest list and manifest entry.
class V1MetaData {
 public:
  /// \brief Wraps a ManifestFile to conform to the v1 schema.
  struct ManifestFileWrapper : public ManifestFile {
    ManifestFileWrapper() = default;

    ManifestFileWrapper& Wrap(const ManifestFile& file) {
      static_cast<ManifestFile&>(*this) = file;
      return *this;
    }
  };

  /// \brief Wraps a DataFile to conform to the v1 schema.
  struct DataFileWrapper : public DataFile {
    int64_t block_size_in_bytes = 64 * 1024 * 1024;

    DataFileWrapper() = default;

    DataFileWrapper& Wrap(const DataFile& file) {
      static_cast<DataFile&>(*this) = file;
      return *this;
    }
  };

  /// \brief Wraps a ManifestEntry to conform to the v1 schema.
  struct ManifestEntryWrapper {
    ManifestStatus status;
    int64_t snapshot_id;
    DataFileWrapper data_file;

    ManifestEntryWrapper() = default;

    ManifestEntryWrapper& Wrap(const ManifestEntry& entry) {
      this->status = entry.status;
      this->snapshot_id = entry.snapshot_id.value_or(0);
      if (entry.data_file) {
        this->data_file.Wrap(*entry.data_file);
      }
      return *this;
    }

    explicit operator ManifestEntry() const {
      ManifestEntry entry;
      entry.status = this->status;
      entry.snapshot_id = this->snapshot_id;

      entry.data_file = std::make_shared<DataFile>(this->data_file);
      entry.sequence_number = 0;
      entry.file_sequence_number = 0;

      return entry;
    }
  };

  static ManifestFileWrapper manifestFileWrapper() { return {}; }

  static ManifestEntryWrapper manifestEntryWrapper() { return {}; }

  static DataFileWrapper dataFileWrapper() { return {}; }
};

}  // namespace iceberg
