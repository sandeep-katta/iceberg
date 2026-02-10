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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.junit.jupiter.api.Test;

public class TestChangelogScanTaskParser {

  private static final Schema SCHEMA = TestBase.SCHEMA;
  private static final PartitionSpec SPEC = TestBase.SPEC;
  private static final String SCHEMA_STRING = SchemaParser.toJson(SCHEMA);
  private static final String SPEC_STRING = PartitionSpecParser.toJson(SPEC);
  private static final ResidualEvaluator RESIDUAL =
      ResidualEvaluator.of(SPEC, Expressions.alwaysTrue(), false);

  @Test
  public void testAddedRowsScanTaskRoundTrip() {
    DeleteFile[] deletes = new DeleteFile[] {TestBase.FILE_A_DELETES, TestBase.FILE_A2_DELETES};
    BaseAddedRowsScanTask task =
        new BaseAddedRowsScanTask(
            0, 12345L, TestBase.FILE_A, deletes, SCHEMA_STRING, SPEC_STRING, RESIDUAL);

    String json = ChangelogScanTaskParser.toJson(task);
    ChangelogScanTask deserialized = ChangelogScanTaskParser.fromJson(json, false);

    assertThat(deserialized).isInstanceOf(AddedRowsScanTask.class);
    AddedRowsScanTask result = (AddedRowsScanTask) deserialized;
    assertThat(result.changeOrdinal()).isEqualTo(0);
    assertThat(result.commitSnapshotId()).isEqualTo(12345L);
    assertThat(result.file().location()).isEqualTo(TestBase.FILE_A.location());
    assertThat(result.deletes()).hasSize(2);
    assertThat(result.deletes().get(0).location())
        .isEqualTo(TestBase.FILE_A_DELETES.location());
    assertThat(result.deletes().get(1).location())
        .isEqualTo(TestBase.FILE_A2_DELETES.location());
  }

  @Test
  public void testAddedRowsScanTaskNoDeletes() {
    BaseAddedRowsScanTask task =
        new BaseAddedRowsScanTask(
            1, 67890L, TestBase.FILE_B, null, SCHEMA_STRING, SPEC_STRING, RESIDUAL);

    String json = ChangelogScanTaskParser.toJson(task);
    ChangelogScanTask deserialized = ChangelogScanTaskParser.fromJson(json, false);

    assertThat(deserialized).isInstanceOf(AddedRowsScanTask.class);
    AddedRowsScanTask result = (AddedRowsScanTask) deserialized;
    assertThat(result.changeOrdinal()).isEqualTo(1);
    assertThat(result.commitSnapshotId()).isEqualTo(67890L);
    assertThat(result.file().location()).isEqualTo(TestBase.FILE_B.location());
    assertThat(result.deletes()).isEmpty();
  }

  @Test
  public void testDeletedDataFileScanTaskRoundTrip() {
    DeleteFile[] existingDeletes = new DeleteFile[] {TestBase.FILE_B_DELETES};
    BaseDeletedDataFileScanTask task =
        new BaseDeletedDataFileScanTask(
            2, 99999L, TestBase.FILE_B, existingDeletes, SCHEMA_STRING, SPEC_STRING, RESIDUAL);

    String json = ChangelogScanTaskParser.toJson(task);
    ChangelogScanTask deserialized = ChangelogScanTaskParser.fromJson(json, false);

    assertThat(deserialized).isInstanceOf(DeletedDataFileScanTask.class);
    DeletedDataFileScanTask result = (DeletedDataFileScanTask) deserialized;
    assertThat(result.changeOrdinal()).isEqualTo(2);
    assertThat(result.commitSnapshotId()).isEqualTo(99999L);
    assertThat(result.file().location()).isEqualTo(TestBase.FILE_B.location());
    assertThat(result.existingDeletes()).hasSize(1);
    assertThat(result.existingDeletes().get(0).location())
        .isEqualTo(TestBase.FILE_B_DELETES.location());
  }

  @Test
  public void testDeletedDataFileScanTaskNoDeletes() {
    BaseDeletedDataFileScanTask task =
        new BaseDeletedDataFileScanTask(
            3, 11111L, TestBase.FILE_A, null, SCHEMA_STRING, SPEC_STRING, RESIDUAL);

    String json = ChangelogScanTaskParser.toJson(task);
    ChangelogScanTask deserialized = ChangelogScanTaskParser.fromJson(json, false);

    assertThat(deserialized).isInstanceOf(DeletedDataFileScanTask.class);
    DeletedDataFileScanTask result = (DeletedDataFileScanTask) deserialized;
    assertThat(result.changeOrdinal()).isEqualTo(3);
    assertThat(result.commitSnapshotId()).isEqualTo(11111L);
    assertThat(result.file().location()).isEqualTo(TestBase.FILE_A.location());
    assertThat(result.existingDeletes()).isEmpty();
  }

  @Test
  public void testSplitAddedRowsScanTaskRoundTrip() {
    // FILE_D has split offsets [0, 3, 6] and size 10, so splitting produces SplitScanTask instances
    DeleteFile[] deletes = new DeleteFile[] {TestBase.FILE_D2_DELETES};
    BaseAddedRowsScanTask parentTask =
        new BaseAddedRowsScanTask(
            0, 12345L, TestBase.FILE_D, deletes, SCHEMA_STRING, SPEC_STRING, RESIDUAL);

    // Split the task - targetSplitSize=1 forces splitting via OffsetsAwareSplitScanTaskIterator
    Iterable<AddedRowsScanTask> splits = parentTask.split(1);
    AddedRowsScanTask splitTask = splits.iterator().next();

    // Verify it's actually a SplitScanTask (not the original)
    assertThat(splitTask).isInstanceOf(BaseChangelogContentScanTask.SplitScanTask.class);

    String json = ChangelogScanTaskParser.toJson(splitTask);
    ChangelogScanTask deserialized = ChangelogScanTaskParser.fromJson(json, false);

    assertThat(deserialized).isInstanceOf(AddedRowsScanTask.class);
    AddedRowsScanTask result = (AddedRowsScanTask) deserialized;
    assertThat(result.changeOrdinal()).isEqualTo(0);
    assertThat(result.commitSnapshotId()).isEqualTo(12345L);
    assertThat(result.file().location()).isEqualTo(TestBase.FILE_D.location());
    assertThat(result.deletes()).hasSize(1);
  }

  @Test
  public void testNullTask() {
    assertThatThrownBy(() -> ChangelogScanTaskParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid changelog scan task: null");
  }

  @Test
  public void testNullJson() {
    assertThatThrownBy(() -> ChangelogScanTaskParser.fromJson((String) null, true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid JSON string for changelog scan task: null");
  }

  @Test
  public void testInvalidTaskType() {
    String jsonStr = "{\"task-type\":\"junk\"}";
    assertThatThrownBy(() -> ChangelogScanTaskParser.fromJson(jsonStr, true))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Unsupported changelog task type: junk");
  }
}
