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

#include <iostream>

#include "iceberg/avro/avro_reader.h"
#include "iceberg/file_reader.h"

int main() {
  iceberg::avro::AvroReader::Register();
  auto open_result = iceberg::ReaderFactoryRegistry::Open(
      iceberg::FileFormatType::kAvro, {.path = "non-existing-file.avro"});
  if (!open_result.has_value()) {
    std::cerr << "Failed to open avro file" << std::endl;
    return 1;
  }
  return 0;
}
