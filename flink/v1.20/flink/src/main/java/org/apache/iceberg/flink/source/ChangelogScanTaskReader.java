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

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.AddedRowsScanTask;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DeletedDataFileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.encryption.InputFilesDecryptor;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.data.FlinkOrcReader;
import org.apache.iceberg.flink.data.FlinkParquetReaders;
import org.apache.iceberg.flink.data.FlinkPlannedAvroReader;
import org.apache.iceberg.flink.data.RowDataProjection;
import org.apache.iceberg.flink.data.RowDataUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.PartitionUtil;

@Internal
public class ChangelogScanTaskReader implements Serializable {

  private final Schema tableSchema;
  private final Schema projectedSchema;
  private final String nameMapping;
  private final boolean caseSensitive;

  public ChangelogScanTaskReader(
      Schema tableSchema, Schema projectedSchema, String nameMapping, boolean caseSensitive) {
    this.tableSchema = tableSchema;
    this.projectedSchema = projectedSchema;
    this.nameMapping = nameMapping;
    this.caseSensitive = caseSensitive;
  }

  public CloseableIterator<RowData> open(
      ChangelogScanTask task, InputFilesDecryptor inputFilesDecryptor) {
    if (task instanceof AddedRowsScanTask) {
      return openAddedRows((AddedRowsScanTask) task, inputFilesDecryptor);
    } else if (task instanceof DeletedDataFileScanTask) {
      return openDeletedDataFile((DeletedDataFileScanTask) task, inputFilesDecryptor);
    } else {
      throw new UnsupportedOperationException(
          "Unsupported changelog task type: " + task.getClass().getCanonicalName());
    }
  }

  private CloseableIterator<RowData> openAddedRows(
      AddedRowsScanTask task, InputFilesDecryptor inputFilesDecryptor) {
    CloseableIterable<RowData> iterable = readAndFilterTask(task, task.deletes(), inputFilesDecryptor);
    return CloseableIterable.transform(iterable, row -> {
      row.setRowKind(RowKind.INSERT);
      return row;
    }).iterator();
  }

  private CloseableIterator<RowData> openDeletedDataFile(
      DeletedDataFileScanTask task, InputFilesDecryptor inputFilesDecryptor) {
    CloseableIterable<RowData> iterable =
        readAndFilterTask(task, task.existingDeletes(), inputFilesDecryptor);
    return CloseableIterable.transform(iterable, row -> {
      row.setRowKind(RowKind.DELETE);
      return row;
    }).iterator();
  }

  private CloseableIterable<RowData> readAndFilterTask(
      ContentScanTask<DataFile> task, List<DeleteFile> deletes, InputFilesDecryptor inputFilesDecryptor) {
    Schema partitionSchema = TypeUtil.select(projectedSchema, task.spec().identitySourceIds());

    Map<Integer, ?> idToConstant =
        partitionSchema.columns().isEmpty()
            ? ImmutableMap.of()
            : PartitionUtil.constantsMap(task, RowDataUtil::convertConstant);

    FlinkDeleteFilter deleteFilter =
        new FlinkDeleteFilter(
            task.file().location(), deletes, tableSchema, projectedSchema, inputFilesDecryptor);

    CloseableIterable<RowData> iterable =
        deleteFilter.filter(
            newIterable(task, deleteFilter.requiredSchema(), idToConstant, inputFilesDecryptor));

    if (!projectedSchema.sameSchema(deleteFilter.requiredSchema())) {
      RowDataProjection rowDataProjection =
          RowDataProjection.create(
              deleteFilter.requiredRowType(),
              deleteFilter.requiredSchema().asStruct(),
              projectedSchema.asStruct());
      iterable = CloseableIterable.transform(iterable, rowDataProjection::wrap);
    }

    return iterable;
  }

  private CloseableIterable<RowData> newIterable(
      ContentScanTask<DataFile> task,
      Schema schema,
      Map<Integer, ?> idToConstant,
      InputFilesDecryptor inputFilesDecryptor) {
    switch (task.file().format()) {
      case PARQUET:
        return newParquetIterable(task, schema, idToConstant, inputFilesDecryptor);
      case AVRO:
        return newAvroIterable(task, schema, idToConstant, inputFilesDecryptor);
      case ORC:
        return newOrcIterable(task, schema, idToConstant, inputFilesDecryptor);
      default:
        throw new UnsupportedOperationException(
            "Cannot read unknown format: " + task.file().format());
    }
  }

  private CloseableIterable<RowData> newAvroIterable(
      ContentScanTask<DataFile> task,
      Schema schema,
      Map<Integer, ?> idToConstant,
      InputFilesDecryptor inputFilesDecryptor) {
    Avro.ReadBuilder builder =
        Avro.read(inputFilesDecryptor.getInputFile(task.file().location()))
            .reuseContainers()
            .project(schema)
            .split(task.start(), task.length())
            .createReaderFunc(readSchema -> FlinkPlannedAvroReader.create(schema, idToConstant));

    if (nameMapping != null) {
      builder.withNameMapping(NameMappingParser.fromJson(nameMapping));
    }

    return builder.build();
  }

  private CloseableIterable<RowData> newParquetIterable(
      ContentScanTask<DataFile> task,
      Schema schema,
      Map<Integer, ?> idToConstant,
      InputFilesDecryptor inputFilesDecryptor) {
    Parquet.ReadBuilder builder =
        Parquet.read(inputFilesDecryptor.getInputFile(task.file().location()))
            .split(task.start(), task.length())
            .project(schema)
            .createReaderFunc(
                fileSchema -> FlinkParquetReaders.buildReader(schema, fileSchema, idToConstant))
            .filter(task.residual())
            .caseSensitive(caseSensitive)
            .reuseContainers();

    if (nameMapping != null) {
      builder.withNameMapping(NameMappingParser.fromJson(nameMapping));
    }

    return builder.build();
  }

  private CloseableIterable<RowData> newOrcIterable(
      ContentScanTask<DataFile> task,
      Schema schema,
      Map<Integer, ?> idToConstant,
      InputFilesDecryptor inputFilesDecryptor) {
    Schema readSchemaWithoutConstantAndMetadataFields =
        TypeUtil.selectNot(
            schema, Sets.union(idToConstant.keySet(), MetadataColumns.metadataFieldIds()));

    ORC.ReadBuilder builder =
        ORC.read(inputFilesDecryptor.getInputFile(task.file().location()))
            .project(readSchemaWithoutConstantAndMetadataFields)
            .split(task.start(), task.length())
            .createReaderFunc(
                readOrcSchema -> new FlinkOrcReader(schema, readOrcSchema, idToConstant))
            .filter(task.residual())
            .caseSensitive(caseSensitive);

    if (nameMapping != null) {
      builder.withNameMapping(NameMappingParser.fromJson(nameMapping));
    }

    return builder.build();
  }
}
