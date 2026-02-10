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
package org.apache.iceberg.flink.source;

import java.util.List;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.encryption.InputFilesDecryptor;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.io.InputFile;

@Internal
class FlinkDeleteFilter extends DeleteFilter<RowData> {
  private final RowType requiredRowType;
  private final RowDataWrapper asStructLike;
  private final InputFilesDecryptor inputFilesDecryptor;

  FlinkDeleteFilter(
      FileScanTask task,
      Schema tableSchema,
      Schema requestedSchema,
      InputFilesDecryptor inputFilesDecryptor) {
    super(task.file().location(), task.deletes(), tableSchema, requestedSchema);
    this.requiredRowType = FlinkSchemaUtil.convert(requiredSchema());
    this.asStructLike = new RowDataWrapper(requiredRowType, requiredSchema().asStruct());
    this.inputFilesDecryptor = inputFilesDecryptor;
  }

  FlinkDeleteFilter(
      String filePath,
      List<DeleteFile> deletes,
      Schema tableSchema,
      Schema requestedSchema,
      InputFilesDecryptor inputFilesDecryptor) {
    super(filePath, deletes, tableSchema, requestedSchema);
    this.requiredRowType = FlinkSchemaUtil.convert(requiredSchema());
    this.asStructLike = new RowDataWrapper(requiredRowType, requiredSchema().asStruct());
    this.inputFilesDecryptor = inputFilesDecryptor;
  }

  public RowType requiredRowType() {
    return requiredRowType;
  }

  @Override
  protected StructLike asStructLike(RowData row) {
    return asStructLike.wrap(row);
  }

  @Override
  protected InputFile getInputFile(String location) {
    return inputFilesDecryptor.getInputFile(location);
  }
}
