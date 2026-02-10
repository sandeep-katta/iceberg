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
package org.apache.iceberg;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionParser;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.util.JsonUtil;

public class ChangelogScanTaskParser {
  private static final String TASK_TYPE = "task-type";
  private static final String TASK_TYPE_ADDED_ROWS = "added-rows";
  private static final String TASK_TYPE_DELETED_DATA_FILE = "deleted-data-file";

  private static final String SCHEMA = "schema";
  private static final String SPEC = "spec";
  private static final String DATA_FILE = "data-file";
  private static final String START = "start";
  private static final String LENGTH = "length";
  private static final String DELETE_FILES = "delete-files";
  private static final String EXISTING_DELETE_FILES = "existing-delete-files";
  private static final String RESIDUAL = "residual-filter";
  private static final String CHANGE_ORDINAL = "change-ordinal";
  private static final String COMMIT_SNAPSHOT_ID = "commit-snapshot-id";

  private ChangelogScanTaskParser() {}

  public static String toJson(ChangelogScanTask task) {
    Preconditions.checkArgument(task != null, "Invalid changelog scan task: null");
    return JsonUtil.generate(generator -> toJson(task, generator), false);
  }

  public static ChangelogScanTask fromJson(String json, boolean caseSensitive) {
    Preconditions.checkArgument(json != null, "Invalid JSON string for changelog scan task: null");
    return JsonUtil.parse(json, node -> fromJson(node, caseSensitive));
  }

  static void toJson(ChangelogScanTask task, JsonGenerator generator) throws IOException {
    Preconditions.checkArgument(task != null, "Invalid changelog scan task: null");
    Preconditions.checkArgument(generator != null, "Invalid JSON generator: null");

    generator.writeStartObject();

    if (task instanceof AddedRowsScanTask) {
      generator.writeStringField(TASK_TYPE, TASK_TYPE_ADDED_ROWS);
      addedRowsToJson((AddedRowsScanTask) task, generator);
    } else if (task instanceof DeletedDataFileScanTask) {
      generator.writeStringField(TASK_TYPE, TASK_TYPE_DELETED_DATA_FILE);
      deletedDataFileToJson((DeletedDataFileScanTask) task, generator);
    } else {
      throw new UnsupportedOperationException(
          "Unsupported changelog task type: " + task.getClass().getCanonicalName());
    }

    generator.writeEndObject();
  }

  static ChangelogScanTask fromJson(JsonNode jsonNode, boolean caseSensitive) {
    Preconditions.checkArgument(
        jsonNode != null, "Invalid JSON node for changelog scan task: null");
    Preconditions.checkArgument(
        jsonNode.isObject(),
        "Invalid JSON node for changelog scan task: non-object (%s)",
        jsonNode);

    String taskType = JsonUtil.getString(TASK_TYPE, jsonNode);
    switch (taskType) {
      case TASK_TYPE_ADDED_ROWS:
        return addedRowsFromJson(jsonNode, caseSensitive);
      case TASK_TYPE_DELETED_DATA_FILE:
        return deletedDataFileFromJson(jsonNode, caseSensitive);
      default:
        throw new UnsupportedOperationException("Unsupported changelog task type: " + taskType);
    }
  }

  private static void addedRowsToJson(AddedRowsScanTask task, JsonGenerator generator)
      throws IOException {
    writeCommonFields(task, generator);

    if (task.deletes() != null && !task.deletes().isEmpty()) {
      generator.writeArrayFieldStart(DELETE_FILES);
      for (DeleteFile deleteFile : task.deletes()) {
        ContentFileParser.toJson(deleteFile, task.spec(), generator);
      }
      generator.writeEndArray();
    }
  }

  private static void deletedDataFileToJson(
      DeletedDataFileScanTask task, JsonGenerator generator) throws IOException {
    writeCommonFields(task, generator);

    if (task.existingDeletes() != null && !task.existingDeletes().isEmpty()) {
      generator.writeArrayFieldStart(EXISTING_DELETE_FILES);
      for (DeleteFile deleteFile : task.existingDeletes()) {
        ContentFileParser.toJson(deleteFile, task.spec(), generator);
      }
      generator.writeEndArray();
    }
  }

  private static <F extends ContentFile<F>> void writeCommonFields(
      ContentScanTask<F> task, JsonGenerator generator) throws IOException {
    ChangelogScanTask changelogTask = (ChangelogScanTask) task;

    generator.writeNumberField(CHANGE_ORDINAL, changelogTask.changeOrdinal());
    generator.writeNumberField(COMMIT_SNAPSHOT_ID, changelogTask.commitSnapshotId());

    generator.writeFieldName(SCHEMA);
    // schema() is protected on BaseContentScanTask, accessible from same package.
    // When tasks are split via planTasks(), the result is a SplitScanTask which does not
    // extend BaseContentScanTask, so we access schema through the parent task.
    Schema schema;
    if (task instanceof BaseContentScanTask) {
      schema = ((BaseContentScanTask<?, ?>) task).schema();
    } else if (task instanceof BaseChangelogContentScanTask.SplitScanTask) {
      ContentScanTask<?> parent =
          ((BaseChangelogContentScanTask.SplitScanTask<?, ?, ?>) task).parentTask();
      schema = ((BaseContentScanTask<?, ?>) parent).schema();
    } else {
      throw new UnsupportedOperationException(
          "Cannot extract schema from task type: " + task.getClass().getCanonicalName());
    }
    SchemaParser.toJson(schema, generator);

    generator.writeFieldName(SPEC);
    PartitionSpec spec = task.spec();
    PartitionSpecParser.toJson(spec, generator);

    generator.writeFieldName(DATA_FILE);
    ContentFileParser.toJson(task.file(), spec, generator);

    generator.writeNumberField(START, task.start());
    generator.writeNumberField(LENGTH, task.length());

    if (task.residual() != null) {
      generator.writeFieldName(RESIDUAL);
      ExpressionParser.toJson(task.residual(), generator);
    }
  }

  private static AddedRowsScanTask addedRowsFromJson(JsonNode jsonNode, boolean caseSensitive) {
    int changeOrdinal = JsonUtil.getInt(CHANGE_ORDINAL, jsonNode);
    long commitSnapshotId = JsonUtil.getLong(COMMIT_SNAPSHOT_ID, jsonNode);

    Schema schema = SchemaParser.fromJson(JsonUtil.get(SCHEMA, jsonNode));
    String schemaString = SchemaParser.toJson(schema);

    PartitionSpec spec = PartitionSpecParser.fromJson(schema, JsonUtil.get(SPEC, jsonNode));
    String specString = PartitionSpecParser.toJson(spec);

    DataFile dataFile = (DataFile) ContentFileParser.fromJson(jsonNode.get(DATA_FILE), spec);

    DeleteFile[] deleteFiles = parseDeleteFiles(jsonNode, DELETE_FILES, spec);

    Expression filter = Expressions.alwaysTrue();
    if (jsonNode.has(RESIDUAL)) {
      filter = ExpressionParser.fromJson(jsonNode.get(RESIDUAL));
    }

    ResidualEvaluator residualEvaluator = ResidualEvaluator.of(spec, filter, caseSensitive);
    return new BaseAddedRowsScanTask(
        changeOrdinal, commitSnapshotId, dataFile, deleteFiles, schemaString, specString,
        residualEvaluator);
  }

  private static DeletedDataFileScanTask deletedDataFileFromJson(
      JsonNode jsonNode, boolean caseSensitive) {
    int changeOrdinal = JsonUtil.getInt(CHANGE_ORDINAL, jsonNode);
    long commitSnapshotId = JsonUtil.getLong(COMMIT_SNAPSHOT_ID, jsonNode);

    Schema schema = SchemaParser.fromJson(JsonUtil.get(SCHEMA, jsonNode));
    String schemaString = SchemaParser.toJson(schema);

    PartitionSpec spec = PartitionSpecParser.fromJson(schema, JsonUtil.get(SPEC, jsonNode));
    String specString = PartitionSpecParser.toJson(spec);

    DataFile dataFile = (DataFile) ContentFileParser.fromJson(jsonNode.get(DATA_FILE), spec);

    DeleteFile[] deleteFiles = parseDeleteFiles(jsonNode, EXISTING_DELETE_FILES, spec);

    Expression filter = Expressions.alwaysTrue();
    if (jsonNode.has(RESIDUAL)) {
      filter = ExpressionParser.fromJson(jsonNode.get(RESIDUAL));
    }

    ResidualEvaluator residualEvaluator = ResidualEvaluator.of(spec, filter, caseSensitive);
    return new BaseDeletedDataFileScanTask(
        changeOrdinal, commitSnapshotId, dataFile, deleteFiles, schemaString, specString,
        residualEvaluator);
  }

  private static DeleteFile[] parseDeleteFiles(
      JsonNode jsonNode, String fieldName, PartitionSpec spec) {
    if (!jsonNode.has(fieldName)) {
      return new DeleteFile[0];
    }

    JsonNode deletesArray = jsonNode.get(fieldName);
    Preconditions.checkArgument(
        deletesArray.isArray(),
        "Invalid JSON node for delete files: non-array (%s)",
        deletesArray);

    ImmutableList.Builder<DeleteFile> builder = ImmutableList.builder();
    for (JsonNode deleteFileNode : deletesArray) {
      DeleteFile deleteFile = (DeleteFile) ContentFileParser.fromJson(deleteFileNode, spec);
      builder.add(deleteFile);
    }

    return builder.build().toArray(new DeleteFile[0]);
  }
}
