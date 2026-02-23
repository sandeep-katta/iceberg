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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AddedRowsScanTask;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeletedDataFileScanTask;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkReadOptions;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.ThreadPools;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestFlinkSplitPlannerChangelog {

  @TempDir protected Path temporaryFolder;

  private static final Schema SCHEMA = TestFixtures.SCHEMA;

  private HadoopCatalog catalog;
  private Table table;
  private Snapshot snap1;
  private Snapshot snap2;

  @BeforeEach
  public void setup() throws Exception {
    File warehouseFile = File.createTempFile("junit", null, temporaryFolder.toFile());
    assertThat(warehouseFile.delete()).isTrue();
    String warehouse = "file:" + warehouseFile;
    Configuration hadoopConf = new Configuration();
    catalog = new HadoopCatalog(hadoopConf, warehouse);

    table =
        catalog.createTable(
            TestFixtures.TABLE_IDENTIFIER,
            SCHEMA,
            PartitionSpec.unpartitioned(),
            null,
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "2"));

    GenericAppenderHelper appender =
        new GenericAppenderHelper(table, FileFormat.PARQUET, temporaryFolder);

    // Append two data files (snap1)
    List<Record> records1 = RandomGenericData.generate(SCHEMA, 3, 0);
    appender.appendToTable(records1);
    List<Record> records2 = RandomGenericData.generate(SCHEMA, 3, 1);
    appender.appendToTable(records2);
    snap1 = table.currentSnapshot();

    // Write a new data file (but don't commit via append)
    DataFile newFile = appender.writeFile(RandomGenericData.generate(SCHEMA, 3, 2));

    // Find an existing file to delete
    DataFile existingFile = table.newScan().planFiles().iterator().next().file();

    // Overwrite: add a new file and delete an existing file (snap2)
    table.newOverwrite().addFile(newFile).deleteFile(existingFile).commit();
    snap2 = table.currentSnapshot();
  }

  @AfterEach
  public void teardown() throws Exception {
    catalog.dropTable(TestFixtures.TABLE_IDENTIFIER);
    catalog.close();
  }

  @Test
  public void testPlanChangelogSplits() {
    ScanContext context =
        ScanContext.builder()
            .startSnapshotId(snap1.snapshotId())
            .endSnapshotId(snap2.snapshotId())
            .streamingChangelogMode(FlinkReadOptions.STREAMING_CHANGELOG_MODE_CHANGELOG)
            .project(SCHEMA)
            .build();

    List<IcebergSourceSplit> splits =
        FlinkSplitPlanner.planIcebergSourceSplits(table, context, ThreadPools.getWorkerPool());

    assertThat(splits).isNotEmpty();

    // All splits should be changelog splits
    for (IcebergSourceSplit split : splits) {
      assertThat(split.isChangelogSplit()).isTrue();
      assertThat(split.changelogTasks()).isNotEmpty();
    }

    // Collect all tasks across splits
    List<ChangelogScanTask> allTasks =
        splits.stream()
            .flatMap(split -> split.changelogTasks().stream())
            .collect(java.util.stream.Collectors.toList());

    // Should have both AddedRows and DeletedDataFile tasks
    boolean hasAdded =
        allTasks.stream().anyMatch(task -> task instanceof AddedRowsScanTask);
    boolean hasDeleted =
        allTasks.stream().anyMatch(task -> task instanceof DeletedDataFileScanTask);
    assertThat(hasAdded).isTrue();
    assertThat(hasDeleted).isTrue();
  }

  @Test
  public void testChangelogSplitsContainCorrectSnapshotId() {
    ScanContext context =
        ScanContext.builder()
            .startSnapshotId(snap1.snapshotId())
            .endSnapshotId(snap2.snapshotId())
            .streamingChangelogMode(FlinkReadOptions.STREAMING_CHANGELOG_MODE_CHANGELOG)
            .project(SCHEMA)
            .build();

    List<IcebergSourceSplit> splits =
        FlinkSplitPlanner.planIcebergSourceSplits(table, context, ThreadPools.getWorkerPool());

    for (IcebergSourceSplit split : splits) {
      for (ChangelogScanTask task : split.changelogTasks()) {
        assertThat(task.commitSnapshotId()).isEqualTo(snap2.snapshotId());
      }
    }
  }

  @Test
  public void testChangelogSplitTasksHaveFileInfo() {
    ScanContext context =
        ScanContext.builder()
            .startSnapshotId(snap1.snapshotId())
            .endSnapshotId(snap2.snapshotId())
            .streamingChangelogMode(FlinkReadOptions.STREAMING_CHANGELOG_MODE_CHANGELOG)
            .project(SCHEMA)
            .build();

    List<IcebergSourceSplit> splits =
        FlinkSplitPlanner.planIcebergSourceSplits(table, context, ThreadPools.getWorkerPool());

    for (IcebergSourceSplit split : splits) {
      for (ChangelogScanTask task : split.changelogTasks()) {
        assertThat(task).isInstanceOf(ContentScanTask.class);
        ContentScanTask<?> contentTask = (ContentScanTask<?>) task;
        assertThat(contentTask.file()).isNotNull();
        assertThat(contentTask.file().location()).isNotNull();
        assertThat(contentTask.length()).isGreaterThan(0);
      }
    }
  }

  @Test
  public void testNonChangelogModePlanReturnsRegularSplits() {
    // Upsert mode should return regular (non-changelog) splits
    ScanContext context =
        ScanContext.builder()
            .startSnapshotId(snap1.snapshotId())
            .endSnapshotId(snap2.snapshotId())
            .streamingChangelogMode(FlinkReadOptions.STREAMING_CHANGELOG_MODE_UPSERT)
            .project(SCHEMA)
            .build();

    List<IcebergSourceSplit> splits =
        FlinkSplitPlanner.planIcebergSourceSplits(table, context, ThreadPools.getWorkerPool());

    assertThat(splits).isNotEmpty();
    for (IcebergSourceSplit split : splits) {
      assertThat(split.isChangelogSplit()).isFalse();
    }
  }

  @Test
  public void testAppendOnlyChangelogScan() throws Exception {
    // Create a table with only append operations (no overwrite)
    File warehouseFile = File.createTempFile("junit", null, temporaryFolder.toFile());
    assertThat(warehouseFile.delete()).isTrue();
    String warehouse = "file:" + warehouseFile;
    HadoopCatalog appendCatalog = new HadoopCatalog(new Configuration(), warehouse);

    try {
      Table appendTable =
          appendCatalog.createTable(
              TestFixtures.TABLE_IDENTIFIER.toString().contains("t")
                  ? org.apache.iceberg.catalog.TableIdentifier.of("default", "t_append")
                  : TestFixtures.TABLE_IDENTIFIER,
              SCHEMA,
              PartitionSpec.unpartitioned(),
              null,
              ImmutableMap.of(TableProperties.FORMAT_VERSION, "2"));

      GenericAppenderHelper appender =
          new GenericAppenderHelper(appendTable, FileFormat.PARQUET, temporaryFolder);

      List<Record> records1 = RandomGenericData.generate(SCHEMA, 3, 10);
      appender.appendToTable(records1);
      Snapshot appendSnap1 = appendTable.currentSnapshot();

      List<Record> records2 = RandomGenericData.generate(SCHEMA, 3, 11);
      appender.appendToTable(records2);
      Snapshot appendSnap2 = appendTable.currentSnapshot();

      ScanContext context =
          ScanContext.builder()
              .startSnapshotId(appendSnap1.snapshotId())
              .endSnapshotId(appendSnap2.snapshotId())
              .streamingChangelogMode(FlinkReadOptions.STREAMING_CHANGELOG_MODE_CHANGELOG)
              .project(SCHEMA)
              .build();

      List<IcebergSourceSplit> splits =
          FlinkSplitPlanner.planIcebergSourceSplits(
              appendTable, context, ThreadPools.getWorkerPool());

      assertThat(splits).isNotEmpty();
      for (IcebergSourceSplit split : splits) {
        assertThat(split.isChangelogSplit()).isTrue();
        for (ChangelogScanTask task : split.changelogTasks()) {
          // Pure appends should only produce AddedRowsScanTask
          assertThat(task).isInstanceOf(AddedRowsScanTask.class);
        }
      }
    } finally {
      appendCatalog.dropTable(
          org.apache.iceberg.catalog.TableIdentifier.of("default", "t_append"));
      appendCatalog.close();
    }
  }
}
