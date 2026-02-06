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
import static org.assertj.core.api.Assumptions.assumeThat;

import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestBaseIncrementalDataScan
    extends ScanTestBase<IncrementalDataScan, FileScanTask, CombinedScanTask> {

  @Override
  protected IncrementalDataScan newScan() {
    return table.newIncrementalDataScan();
  }

  @TestTemplate
  public void testAppendSnapshots() {
    table.newFastAppend().appendFile(FILE_A).commit();
    table.newFastAppend().appendFile(FILE_B).commit();

    IncrementalDataScan scan = newScan();
    assertThat(Iterables.size(scan.planFiles())).isEqualTo(2);
  }

  @TestTemplate
  public void testOverwriteSnapshots() {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    Snapshot snap1 = table.currentSnapshot();

    table.newOverwrite().addFile(FILE_A2).deleteFile(FILE_A).commit();
    Snapshot snap2 = table.currentSnapshot();

    IncrementalDataScan scan =
        newScan().fromSnapshotExclusive(snap1.snapshotId()).toSnapshot(snap2.snapshotId());

    List<FileScanTask> tasks = Lists.newArrayList(scan.planFiles());
    assertThat(tasks).hasSize(1);
    assertThat(tasks.get(0).file().location()).isEqualTo(FILE_A2.location());
  }

  @TestTemplate
  public void testRowDeltaUpsertSnapshots() {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(2);

    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot snap1 = table.currentSnapshot();

    // RowDelta simulates an upsert: adds new data + equality deletes
    table.newRowDelta().addRows(FILE_A2).addDeletes(FILE_A2_DELETES).commit();
    Snapshot snap2 = table.currentSnapshot();
    assertThat(snap2.operation()).isEqualTo(DataOperations.OVERWRITE);

    IncrementalDataScan scan =
        newScan().fromSnapshotExclusive(snap1.snapshotId()).toSnapshot(snap2.snapshotId());

    List<FileScanTask> tasks = Lists.newArrayList(scan.planFiles());
    // Should include the data file from the OVERWRITE snapshot
    assertThat(tasks).hasSize(1);
    assertThat(tasks.get(0).file().location()).isEqualTo(FILE_A2.location());
  }

  @TestTemplate
  public void testDeleteSnapshots() {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    Snapshot snap1 = table.currentSnapshot();

    table.newDelete().deleteFile(FILE_A).commit();
    Snapshot snap2 = table.currentSnapshot();
    assertThat(snap2.operation()).isEqualTo(DataOperations.DELETE);

    IncrementalDataScan scan =
        newScan().fromSnapshotExclusive(snap1.snapshotId()).toSnapshot(snap2.snapshotId());

    // DELETE snapshot removes files but doesn't add new data files
    List<FileScanTask> tasks = Lists.newArrayList(scan.planFiles());
    assertThat(tasks).isEmpty();
  }

  @TestTemplate
  public void testReplaceSnapshotsAreExcluded() {
    table.newFastAppend().appendFile(FILE_A).commit();

    table.newFastAppend().appendFile(FILE_B).commit();
    Snapshot snap2 = table.currentSnapshot();

    // REPLACE (data file rewrite) should be excluded
    table.newRewrite().rewriteFiles(ImmutableSet.of(FILE_A), ImmutableSet.of(FILE_A2)).commit();
    Snapshot snap3 = table.currentSnapshot();
    assertThat(snap3.operation()).isEqualTo(DataOperations.REPLACE);

    IncrementalDataScan scan =
        newScan().fromSnapshotExclusive(snap2.snapshotId()).toSnapshot(snap3.snapshotId());

    // No data files should be returned since the only snapshot in range is REPLACE
    List<FileScanTask> tasks = Lists.newArrayList(scan.planFiles());
    assertThat(tasks).isEmpty();
  }

  @TestTemplate
  public void testMixedSnapshotTypes() {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(2);

    // append
    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot snap1 = table.currentSnapshot();

    // append
    table.newFastAppend().appendFile(FILE_B).commit();

    // upsert (OVERWRITE via RowDelta)
    table.newRowDelta().addRows(FILE_C).addDeletes(FILE_A2_DELETES).commit();

    Snapshot snapLast = table.currentSnapshot();

    IncrementalDataScan scan =
        newScan().fromSnapshotExclusive(snap1.snapshotId()).toSnapshot(snapLast.snapshotId());

    List<FileScanTask> tasks = Lists.newArrayList(scan.planFiles());
    // Should include FILE_B (append) and FILE_C (overwrite/upsert)
    assertThat(tasks).hasSize(2);
    assertThat(tasks)
        .extracting(t -> t.file().location())
        .containsExactlyInAnyOrder(FILE_B.location(), FILE_C.location());
  }

  @TestTemplate
  public void testFromSnapshotInclusive() {
    table.newFastAppend().appendFile(FILE_A).commit();
    long snapshotAId = table.currentSnapshot().snapshotId();
    table.newFastAppend().appendFile(FILE_B).commit();
    table.newFastAppend().appendFile(FILE_C).commit();
    long snapshotCId = table.currentSnapshot().snapshotId();

    IncrementalDataScan scan = newScan().fromSnapshotInclusive(snapshotAId);
    assertThat(scan.planFiles()).hasSize(3);

    IncrementalDataScan scanWithToSnapshot =
        newScan().fromSnapshotInclusive(snapshotAId).toSnapshot(snapshotCId);
    assertThat(scanWithToSnapshot.planFiles()).hasSize(3);
  }

  @TestTemplate
  public void testFromSnapshotExclusive() {
    table.newFastAppend().appendFile(FILE_A).commit();
    long snapshotAId = table.currentSnapshot().snapshotId();
    table.newFastAppend().appendFile(FILE_B).commit();
    long snapshotBId = table.currentSnapshot().snapshotId();
    table.newFastAppend().appendFile(FILE_C).commit();

    IncrementalDataScan scan = newScan().fromSnapshotExclusive(snapshotAId);
    assertThat(scan.planFiles()).hasSize(2);

    IncrementalDataScan scanWithToSnapshot =
        newScan().fromSnapshotExclusive(snapshotAId).toSnapshot(snapshotBId);
    assertThat(scanWithToSnapshot.planFiles()).hasSize(1);
  }

  @TestTemplate
  public void testAppendScanSkipsOverwriteButDataScanIncludes() {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(2);

    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot snap1 = table.currentSnapshot();

    // RowDelta produces OVERWRITE snapshot
    table.newRowDelta().addRows(FILE_B).addDeletes(FILE_A2_DELETES).commit();

    // Verify IncrementalAppendScan skips the overwrite
    IncrementalAppendScan appendScan =
        table
            .newIncrementalAppendScan()
            .fromSnapshotExclusive(snap1.snapshotId());
    assertThat(appendScan.planFiles()).isEmpty();

    // Verify IncrementalDataScan includes the overwrite
    IncrementalDataScan dataScan =
        newScan().fromSnapshotExclusive(snap1.snapshotId());
    List<FileScanTask> tasks = Lists.newArrayList(dataScan.planFiles());
    assertThat(tasks).hasSize(1);
    assertThat(tasks.get(0).file().location()).isEqualTo(FILE_B.location());
  }

  @TestTemplate
  public void testEmptyRange() {
    table.newFastAppend().appendFile(FILE_A).commit();
    long snapshotAId = table.currentSnapshot().snapshotId();

    // Use fromSnapshotInclusive and toSnapshot with the same snapshot to get single-snapshot scan
    IncrementalDataScan scan =
        newScan().fromSnapshotInclusive(snapshotAId).toSnapshot(snapshotAId);
    // Single snapshot should return its file
    assertThat(scan.planFiles()).hasSize(1);
  }

  @TestTemplate
  public void testDeleteFilesArePairedWithDataFiles() {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(2);

    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot snap1 = table.currentSnapshot();

    // RowDelta adds a data file and an equality delete that applies to FILE_A's partition
    table.newRowDelta().addRows(FILE_A2).addDeletes(FILE_A2_DELETES).commit();

    IncrementalDataScan scan =
        newScan().fromSnapshotExclusive(snap1.snapshotId());

    List<FileScanTask> tasks = Lists.newArrayList(scan.planFiles());
    assertThat(tasks).hasSize(1);
    // The data file from the upsert snapshot should be present
    assertThat(tasks.get(0).file().location()).isEqualTo(FILE_A2.location());
  }
}
