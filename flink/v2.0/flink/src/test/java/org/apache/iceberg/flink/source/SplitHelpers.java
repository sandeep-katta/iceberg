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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.BaseFileScanTask;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.IncrementalChangelogScan;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.ThreadPools;

public class SplitHelpers {

  private SplitHelpers() {}

  /**
   * This create a list of IcebergSourceSplit from real files
   * <li>Create a new Hadoop table under the {@code temporaryFolder}
   * <li>write {@code fileCount} number of files to the new Iceberg table
   * <li>Discover the splits from the table and partition the splits by the {@code filePerSplit}
   *     limit
   * <li>Delete the Hadoop table
   *
   *     <p>Since the table and data files are deleted before this method return, caller shouldn't
   *     attempt to read the data files.
   *
   *     <p>By default, v1 Iceberg table is created. For v2 table use {@link
   *     SplitHelpers#createSplitsFromTransientHadoopTable(Path, int, int, String)}
   *
   * @param temporaryFolder Folder to place the data to
   * @param fileCount The number of files to create and add to the table
   * @param filesPerSplit The number of files used for a split
   */
  public static List<IcebergSourceSplit> createSplitsFromTransientHadoopTable(
      Path temporaryFolder, int fileCount, int filesPerSplit) throws Exception {
    return createSplitsFromTransientHadoopTable(temporaryFolder, fileCount, filesPerSplit, "1");
  }

  /**
   * This create a list of IcebergSourceSplit from real files
   * <li>Create a new Hadoop table under the {@code temporaryFolder}
   * <li>write {@code fileCount} number of files to the new Iceberg table
   * <li>Discover the splits from the table and partition the splits by the {@code filePerSplit}
   *     limit
   * <li>Delete the Hadoop table
   *
   *     <p>Since the table and data files are deleted before this method return, caller shouldn't
   *     attempt to read the data files.
   *
   * @param temporaryFolder Folder to place the data to
   * @param fileCount The number of files to create and add to the table
   * @param filesPerSplit The number of files used for a split
   * @param version The table version to create
   */
  public static List<IcebergSourceSplit> createSplitsFromTransientHadoopTable(
      Path temporaryFolder, int fileCount, int filesPerSplit, String version) throws Exception {
    final File warehouseFile = File.createTempFile("junit", null, temporaryFolder.toFile());
    assertThat(warehouseFile.delete()).isTrue();
    final String warehouse = "file:" + warehouseFile;
    Configuration hadoopConf = new Configuration();
    final HadoopCatalog catalog = new HadoopCatalog(hadoopConf, warehouse);
    ImmutableMap<String, String> properties =
        ImmutableMap.of(TableProperties.FORMAT_VERSION, version);
    try {
      final Table table =
          catalog.createTable(
              TestFixtures.TABLE_IDENTIFIER,
              TestFixtures.SCHEMA,
              PartitionSpec.unpartitioned(),
              null,
              properties);
      final GenericAppenderHelper dataAppender =
          new GenericAppenderHelper(table, FileFormat.PARQUET, temporaryFolder);
      for (int i = 0; i < fileCount; ++i) {
        List<Record> records = RandomGenericData.generate(TestFixtures.SCHEMA, 2, i);
        dataAppender.appendToTable(records);
      }

      final ScanContext scanContext = ScanContext.builder().build();
      final List<IcebergSourceSplit> splits =
          FlinkSplitPlanner.planIcebergSourceSplits(
              table, scanContext, ThreadPools.getWorkerPool());
      return splits.stream()
          .flatMap(
              split -> {
                List<List<FileScanTask>> filesList =
                    Lists.partition(Lists.newArrayList(split.task().files()), filesPerSplit);
                return filesList.stream()
                    .map(files -> new BaseCombinedScanTask(files))
                    .map(
                        combinedScanTask ->
                            IcebergSourceSplit.fromCombinedScanTask(combinedScanTask));
              })
          .collect(Collectors.toList());
    } finally {
      catalog.dropTable(TestFixtures.TABLE_IDENTIFIER);
      catalog.close();
    }
  }

  /**
   * This method will equip the {@code icebergSourceSplits} with mock delete files.
   * <li>For each split, create {@code deleteFilesPerSplit} number of delete files
   * <li>Replace the original {@code FileScanTask} with the new {@code FileScanTask} with mock
   * <li>Caller should not attempt to read the deleted files since they are created as mock, and
   *     they are not real files
   *
   * @param icebergSourceSplits The real splits to equip with mock delete files
   * @param temporaryFolder The temporary folder to create the mock delete files with
   * @param deleteFilesPerSplit The number of delete files to create for each split
   * @return The list of re-created splits with mock delete files
   * @throws IOException If there is any error creating the mock delete files
   */
  public static List<IcebergSourceSplit> equipSplitsWithMockDeleteFiles(
      List<IcebergSourceSplit> icebergSourceSplits, Path temporaryFolder, int deleteFilesPerSplit)
      throws IOException {
    List<IcebergSourceSplit> icebergSourceSplitsWithMockDeleteFiles = Lists.newArrayList();
    for (IcebergSourceSplit split : icebergSourceSplits) {
      final CombinedScanTask combinedScanTask = spy(split.task());

      final List<DeleteFile> deleteFiles = Lists.newArrayList();
      final PartitionSpec spec =
          PartitionSpec.builderFor(TestFixtures.SCHEMA).withSpecId(0).build();

      for (int i = 0; i < deleteFilesPerSplit; ++i) {
        final DeleteFile deleteFile =
            FileMetadata.deleteFileBuilder(spec)
                .withFormat(FileFormat.PARQUET)
                .withPath(File.createTempFile("junit", null, temporaryFolder.toFile()).getPath())
                .ofPositionDeletes()
                .withFileSizeInBytes(1000)
                .withRecordCount(1000)
                .build();
        deleteFiles.add(deleteFile);
      }

      List<FileScanTask> newFileScanTasks = Lists.newArrayList();
      for (FileScanTask task : combinedScanTask.tasks()) {
        String schemaString = SchemaParser.toJson(task.schema());
        String specString = PartitionSpecParser.toJson(task.spec());

        BaseFileScanTask baseFileScanTask =
            new BaseFileScanTask(
                task.file(),
                deleteFiles.toArray(new DeleteFile[] {}),
                schemaString,
                specString,
                ResidualEvaluator.unpartitioned(task.residual()));
        newFileScanTasks.add(baseFileScanTask);
      }
      doReturn(newFileScanTasks).when(combinedScanTask).tasks();
      icebergSourceSplitsWithMockDeleteFiles.add(
          IcebergSourceSplit.fromCombinedScanTask(
              combinedScanTask, split.fileOffset(), split.recordOffset()));
    }
    return icebergSourceSplitsWithMockDeleteFiles;
  }

  /**
   * Creates a list of changelog-based IcebergSourceSplit instances from a transient Hadoop table.
   *
   * <p>Steps:
   * <li>Create a v2 Hadoop table
   * <li>Append two data files (snap1)
   * <li>Overwrite: add a new file and delete an old file (snap2)
   * <li>Run IncrementalChangelogScan from snap1 to snap2 to produce ChangelogScanTasks
   * <li>Wrap tasks in IcebergSourceSplit.fromChangelogTasks()
   *
   * @param temporaryFolder Folder to place the data in
   * @return list of changelog splits containing AddedRowsScanTask and DeletedDataFileScanTask
   */
  public static List<IcebergSourceSplit> createChangelogSplitsFromTransientHadoopTable(
      Path temporaryFolder) throws Exception {
    final File warehouseFile = File.createTempFile("junit", null, temporaryFolder.toFile());
    assertThat(warehouseFile.delete()).isTrue();
    final String warehouse = "file:" + warehouseFile;
    Configuration hadoopConf = new Configuration();
    final HadoopCatalog catalog = new HadoopCatalog(hadoopConf, warehouse);
    ImmutableMap<String, String> properties =
        ImmutableMap.of(TableProperties.FORMAT_VERSION, "2");
    try {
      final Table table =
          catalog.createTable(
              TestFixtures.TABLE_IDENTIFIER,
              TestFixtures.SCHEMA,
              PartitionSpec.unpartitioned(),
              null,
              properties);
      final GenericAppenderHelper dataAppender =
          new GenericAppenderHelper(table, FileFormat.PARQUET, temporaryFolder);

      // Append two files (snap1)
      List<Record> records1 = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0);
      dataAppender.appendToTable(records1);
      List<Record> records2 = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 1);
      dataAppender.appendToTable(records2);
      Snapshot snap1 = table.currentSnapshot();

      // Overwrite: add a new file and delete an existing file (snap2)
      // This produces both AddedRowsScanTask and DeletedDataFileScanTask
      List<Record> records3 = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 2);
      dataAppender.appendToTable(records3);
      Snapshot appendSnap = table.currentSnapshot();

      // Get the file we just appended and the first file to create an overwrite
      org.apache.iceberg.DataFile addedFile =
          appendSnap.addedDataFiles(table.io()).iterator().next();
      org.apache.iceberg.DataFile firstFile =
          table.newScan().planFiles().iterator().next().file();

      // Revert the append and do an overwrite instead
      table.manageSnapshots().rollbackTo(snap1.snapshotId()).commit();

      table.newOverwrite().addFile(addedFile).deleteFile(firstFile).commit();
      Snapshot snap2 = table.currentSnapshot();

      // Scan for changelog tasks between snap1 and snap2
      IncrementalChangelogScan scan =
          table
              .newIncrementalChangelogScan()
              .fromSnapshotExclusive(snap1.snapshotId())
              .toSnapshot(snap2.snapshotId());

      List<ChangelogScanTask> changelogTasks;
      try (CloseableIterable<ChangelogScanTask> tasks = scan.planFiles()) {
        changelogTasks = Lists.newArrayList(tasks);
      }

      assertThat(changelogTasks).isNotEmpty();

      // Wrap all changelog tasks into a single split
      return ImmutableList.of(IcebergSourceSplit.fromChangelogTasks(changelogTasks));
    } finally {
      catalog.dropTable(TestFixtures.TABLE_IDENTIFIER);
      catalog.close();
    }
  }
}
