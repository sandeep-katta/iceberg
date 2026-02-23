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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AddedRowsScanTask;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeletedDataFileScanTask;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.IncrementalChangelogScan;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestChangelogDataIterator {

  @TempDir protected Path temporaryFolder;

  private static final Schema SCHEMA = TestFixtures.SCHEMA;

  @Test
  public void testAddedRowsEmitInsertRowKind() throws Exception {
    TestTableContext ctx = createTableWithOverwrite();
    try {
      List<ChangelogScanTask> tasks = scanChangelog(ctx.table, ctx.snap1, ctx.snap2);

      // Find AddedRowsScanTask
      List<ChangelogScanTask> addedTasks = Lists.newArrayList();
      for (ChangelogScanTask task : tasks) {
        if (task instanceof AddedRowsScanTask) {
          addedTasks.add(task);
        }
      }

      assertThat(addedTasks).isNotEmpty();

      ChangelogScanTaskReader reader = new ChangelogScanTaskReader(SCHEMA, SCHEMA, null, true);
      ChangelogDataIterator iterator =
          new ChangelogDataIterator(
              reader,
              addedTasks,
              new HadoopFileIO(new Configuration()),
              PlaintextEncryptionManager.instance());

      List<RowData> rows = Lists.newArrayList();
      while (iterator.hasNext()) {
        rows.add(iterator.next());
      }

      iterator.close();

      assertThat(rows).isNotEmpty();
      for (RowData row : rows) {
        assertThat(row.getRowKind()).isEqualTo(RowKind.INSERT);
      }
    } finally {
      ctx.close();
    }
  }

  @Test
  public void testDeletedDataFileEmitsDeleteRowKind() throws Exception {
    TestTableContext ctx = createTableWithOverwrite();
    try {
      List<ChangelogScanTask> tasks = scanChangelog(ctx.table, ctx.snap1, ctx.snap2);

      // Find DeletedDataFileScanTask
      List<ChangelogScanTask> deletedTasks = Lists.newArrayList();
      for (ChangelogScanTask task : tasks) {
        if (task instanceof DeletedDataFileScanTask) {
          deletedTasks.add(task);
        }
      }

      assertThat(deletedTasks).isNotEmpty();

      ChangelogScanTaskReader reader = new ChangelogScanTaskReader(SCHEMA, SCHEMA, null, true);
      ChangelogDataIterator iterator =
          new ChangelogDataIterator(
              reader,
              deletedTasks,
              new HadoopFileIO(new Configuration()),
              PlaintextEncryptionManager.instance());

      List<RowData> rows = Lists.newArrayList();
      while (iterator.hasNext()) {
        rows.add(iterator.next());
      }

      iterator.close();

      assertThat(rows).isNotEmpty();
      for (RowData row : rows) {
        assertThat(row.getRowKind()).isEqualTo(RowKind.DELETE);
      }
    } finally {
      ctx.close();
    }
  }

  @Test
  public void testMixedTasksIterateInOrder() throws Exception {
    TestTableContext ctx = createTableWithOverwrite();
    try {
      List<ChangelogScanTask> tasks = scanChangelog(ctx.table, ctx.snap1, ctx.snap2);

      // Should have both AddedRows and DeletedDataFile tasks
      boolean hasAdded = false;
      boolean hasDeleted = false;
      for (ChangelogScanTask task : tasks) {
        if (task instanceof AddedRowsScanTask) {
          hasAdded = true;
        }
        if (task instanceof DeletedDataFileScanTask) {
          hasDeleted = true;
        }
      }

      assertThat(hasAdded).isTrue();
      assertThat(hasDeleted).isTrue();

      ChangelogScanTaskReader reader = new ChangelogScanTaskReader(SCHEMA, SCHEMA, null, true);
      ChangelogDataIterator iterator =
          new ChangelogDataIterator(
              reader,
              tasks,
              new HadoopFileIO(new Configuration()),
              PlaintextEncryptionManager.instance());

      List<RowData> rows = Lists.newArrayList();
      while (iterator.hasNext()) {
        rows.add(iterator.next());
      }

      iterator.close();

      assertThat(rows).isNotEmpty();

      // Verify RowKind matches task order
      boolean seenInsert = false;
      boolean seenDelete = false;
      for (RowData row : rows) {
        if (row.getRowKind() == RowKind.INSERT) {
          seenInsert = true;
        } else if (row.getRowKind() == RowKind.DELETE) {
          seenDelete = true;
        }
      }

      assertThat(seenInsert).isTrue();
      assertThat(seenDelete).isTrue();
    } finally {
      ctx.close();
    }
  }

  @Test
  public void testFileOffsetAndRecordOffsetTracking() throws Exception {
    TestTableContext ctx = createTableWithOverwrite();
    try {
      List<ChangelogScanTask> tasks = scanChangelog(ctx.table, ctx.snap1, ctx.snap2);

      ChangelogScanTaskReader reader = new ChangelogScanTaskReader(SCHEMA, SCHEMA, null, true);
      ChangelogDataIterator iterator =
          new ChangelogDataIterator(
              reader,
              tasks,
              new HadoopFileIO(new Configuration()),
              PlaintextEncryptionManager.instance());

      // Before first next(), fileOffset starts at -1
      assertThat(iterator.fileOffset()).isEqualTo(-1);
      assertThat(iterator.recordOffset()).isEqualTo(0L);

      if (iterator.hasNext()) {
        iterator.next();
        // After first next(), fileOffset should be >= 0
        assertThat(iterator.fileOffset()).isGreaterThanOrEqualTo(0);
        assertThat(iterator.recordOffset()).isGreaterThan(0L);
      }

      // Drain the rest
      int prevFileOffset = iterator.fileOffset();
      while (iterator.hasNext()) {
        iterator.next();
        // fileOffset should only increase or stay the same
        assertThat(iterator.fileOffset()).isGreaterThanOrEqualTo(prevFileOffset);
        prevFileOffset = iterator.fileOffset();
      }

      iterator.close();
    } finally {
      ctx.close();
    }
  }

  @Test
  public void testSeekToStartingPosition() throws Exception {
    TestTableContext ctx = createTableWithOverwrite();
    try {
      List<ChangelogScanTask> tasks = scanChangelog(ctx.table, ctx.snap1, ctx.snap2);

      // First, read all rows without seeking
      ChangelogScanTaskReader reader = new ChangelogScanTaskReader(SCHEMA, SCHEMA, null, true);
      ChangelogDataIterator fullIterator =
          new ChangelogDataIterator(
              reader,
              tasks,
              new HadoopFileIO(new Configuration()),
              PlaintextEncryptionManager.instance());

      List<RowData> allRows = Lists.newArrayList();
      while (fullIterator.hasNext()) {
        allRows.add(fullIterator.next());
      }

      fullIterator.close();

      assertThat(allRows.size()).isGreaterThanOrEqualTo(2);

      // Now seek to the first task, skip the first record
      ChangelogDataIterator seekIterator =
          new ChangelogDataIterator(
              reader,
              tasks,
              new HadoopFileIO(new Configuration()),
              PlaintextEncryptionManager.instance());

      seekIterator.seek(0, 1);

      assertThat(seekIterator.fileOffset()).isEqualTo(0);
      assertThat(seekIterator.recordOffset()).isEqualTo(1);

      List<RowData> seekRows = Lists.newArrayList();
      while (seekIterator.hasNext()) {
        seekRows.add(seekIterator.next());
      }

      seekIterator.close();

      // Should have one fewer row from the first task
      assertThat(seekRows.size()).isEqualTo(allRows.size() - 1);
    } finally {
      ctx.close();
    }
  }

  @Test
  public void testSeekCalledTwiceThrows() throws Exception {
    TestTableContext ctx = createTableWithOverwrite();
    try {
      List<ChangelogScanTask> tasks = scanChangelog(ctx.table, ctx.snap1, ctx.snap2);

      ChangelogScanTaskReader reader = new ChangelogScanTaskReader(SCHEMA, SCHEMA, null, true);
      ChangelogDataIterator iterator =
          new ChangelogDataIterator(
              reader,
              tasks,
              new HadoopFileIO(new Configuration()),
              PlaintextEncryptionManager.instance());

      // First call to hasNext() triggers internal state update
      iterator.hasNext();

      // Now seek should fail because iterator has already been used
      assertThatThrownBy(() -> iterator.seek(0, 0))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("Seek should be called before any other iterator actions");

      iterator.close();
    } finally {
      ctx.close();
    }
  }

  @Test
  public void testEmptyChangelogTasks() throws Exception {
    ChangelogScanTaskReader reader = new ChangelogScanTaskReader(SCHEMA, SCHEMA, null, true);
    ChangelogDataIterator iterator =
        new ChangelogDataIterator(
            reader,
            Lists.newArrayList(),
            new HadoopFileIO(new Configuration()),
            PlaintextEncryptionManager.instance());

    assertThat(iterator.hasNext()).isFalse();
    iterator.close();
  }

  private static List<ChangelogScanTask> scanChangelog(
      Table table, Snapshot fromExclusive, Snapshot to) throws Exception {
    IncrementalChangelogScan scan =
        table
            .newIncrementalChangelogScan()
            .fromSnapshotExclusive(fromExclusive.snapshotId())
            .toSnapshot(to.snapshotId());

    try (CloseableIterable<ChangelogScanTask> tasks = scan.planFiles()) {
      return Lists.newArrayList(tasks);
    }
  }

  private TestTableContext createTableWithOverwrite() throws Exception {
    File warehouseFile = File.createTempFile("junit", null, temporaryFolder.toFile());
    assertThat(warehouseFile.delete()).isTrue();
    String warehouse = "file:" + warehouseFile;
    Configuration hadoopConf = new Configuration();
    HadoopCatalog catalog = new HadoopCatalog(hadoopConf, warehouse);

    Table table =
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
    Snapshot snap1 = table.currentSnapshot();

    // Write a new data file (but don't commit via append)
    DataFile newFile = appender.writeFile(RandomGenericData.generate(SCHEMA, 3, 2));

    // Find an existing file to delete
    DataFile existingFile = table.newScan().planFiles().iterator().next().file();

    // Overwrite: add a new file and delete an existing file (snap2)
    table.newOverwrite().addFile(newFile).deleteFile(existingFile).commit();
    Snapshot snap2 = table.currentSnapshot();

    return new TestTableContext(catalog, table, snap1, snap2);
  }

  private static class TestTableContext implements AutoCloseable {
    final HadoopCatalog catalog;
    final Table table;
    final Snapshot snap1;
    final Snapshot snap2;

    TestTableContext(HadoopCatalog catalog, Table table, Snapshot snap1, Snapshot snap2) {
      this.catalog = catalog;
      this.table = table;
      this.snap1 = snap1;
      this.snap2 = snap2;
    }

    @Override
    public void close() throws Exception {
      catalog.dropTable(TestFixtures.TABLE_IDENTIFIER);
      catalog.close();
    }
  }
}
