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

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkReadOptions;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.flink.MiniFlinkClusterExtension;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.flink.data.RowDataToRowMapper;
import org.apache.iceberg.flink.source.assigner.SimpleSplitAssignerFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/**
 * E2E tests for streaming source with different STREAMING_CHANGELOG_MODE values: none, upsert, and
 * changelog. Uses a Flink MiniCluster to run the full pipeline: IcebergSource → enumerator → split
 * planner → reader → RowData output.
 */
public class TestIcebergSourceChangelogE2E {

  private static final Schema SCHEMA = TestFixtures.SCHEMA;

  @TempDir protected Path temporaryFolder;

  @RegisterExtension
  public static MiniClusterExtension miniClusterExtension =
      MiniFlinkClusterExtension.createWithClassloaderCheckDisabled();

  @RegisterExtension
  private static final HadoopCatalogExtension CATALOG_EXTENSION =
      new HadoopCatalogExtension(TestFixtures.DATABASE, TestFixtures.TABLE);

  private final AtomicLong randomSeed = new AtomicLong(0L);
  private Table table;
  private GenericAppenderHelper dataAppender;

  @BeforeEach
  void setup() {
    table =
        CATALOG_EXTENSION
            .catalog()
            .createTable(
                TestFixtures.TABLE_IDENTIFIER,
                SCHEMA,
                PartitionSpec.unpartitioned(),
                ImmutableMap.of(TableProperties.FORMAT_VERSION, "2"));
    dataAppender = new GenericAppenderHelper(table, FileFormat.PARQUET, temporaryFolder);
  }

  @Test
  void testNoneModeSkipsOverwriteSnapshots() throws Exception {
    List<Record> batch1 = RandomGenericData.generate(SCHEMA, 3, randomSeed.incrementAndGet());
    dataAppender.appendToTable(batch1);

    try (CloseableIterator<Row> iter =
        createStream(FlinkReadOptions.STREAMING_CHANGELOG_MODE_NONE)
            .executeAndCollect(getClass().getSimpleName())) {
      // Initial table scan returns batch1
      List<Row> result1 = waitForResult(iter, 3);
      TestHelpers.assertRecords(result1, batch1, SCHEMA);

      // Perform overwrite: add new file, delete existing file (creates OVERWRITE snapshot)
      DataFile newFile =
          dataAppender.writeFile(
              RandomGenericData.generate(SCHEMA, 3, randomSeed.incrementAndGet()));
      DataFile existingFile = table.newScan().planFiles().iterator().next().file();
      table.newOverwrite().addFile(newFile).deleteFile(existingFile).commit();

      // Append batch3 (creates APPEND snapshot)
      List<Record> batch3 = RandomGenericData.generate(SCHEMA, 3, randomSeed.incrementAndGet());
      dataAppender.appendToTable(batch3);

      // None mode uses IncrementalAppendScan: skips OVERWRITE, only sees APPEND
      List<Row> result2 = waitForResult(iter, 3);
      TestHelpers.assertRecords(result2, batch3, SCHEMA);
    }
  }

  @Test
  void testUpsertModeIncludesOverwriteSnapshots() throws Exception {
    List<Record> batch1 = RandomGenericData.generate(SCHEMA, 3, randomSeed.incrementAndGet());
    dataAppender.appendToTable(batch1);

    try (CloseableIterator<Row> iter =
        createStream(FlinkReadOptions.STREAMING_CHANGELOG_MODE_UPSERT)
            .executeAndCollect(getClass().getSimpleName())) {
      // Initial table scan returns batch1
      List<Row> result1 = waitForResult(iter, 3);
      TestHelpers.assertRecords(result1, batch1, SCHEMA);

      // Perform overwrite: add new file, delete existing file (creates OVERWRITE snapshot)
      List<Record> overwriteRecords =
          RandomGenericData.generate(SCHEMA, 3, randomSeed.incrementAndGet());
      DataFile newFile = dataAppender.writeFile(overwriteRecords);
      DataFile existingFile = table.newScan().planFiles().iterator().next().file();
      table.newOverwrite().addFile(newFile).deleteFile(existingFile).commit();

      // Append batch3 (creates APPEND snapshot)
      List<Record> batch3 = RandomGenericData.generate(SCHEMA, 3, randomSeed.incrementAndGet());
      dataAppender.appendToTable(batch3);

      // Upsert mode uses IncrementalDataScan: sees OVERWRITE + APPEND data files
      List<Record> expectedIncremental = Lists.newArrayList();
      expectedIncremental.addAll(overwriteRecords);
      expectedIncremental.addAll(batch3);

      List<Row> result2 = waitForResult(iter, 6);
      TestHelpers.assertRecords(result2, expectedIncremental, SCHEMA);

      // All rows should be INSERT (upsert mode emits everything as INSERT)
      for (Row row : result2) {
        assertThat(row.getKind()).isEqualTo(RowKind.INSERT);
      }
    }
  }

  @Test
  void testChangelogModeEmitsInsertAndDelete() throws Exception {
    List<Record> batch1 = RandomGenericData.generate(SCHEMA, 3, randomSeed.incrementAndGet());
    dataAppender.appendToTable(batch1);

    try (CloseableIterator<Row> iter =
        createStream(FlinkReadOptions.STREAMING_CHANGELOG_MODE_CHANGELOG)
            .executeAndCollect(getClass().getSimpleName())) {
      // Initial table scan returns batch1, all INSERT
      List<Row> result1 = waitForResult(iter, 3);
      TestHelpers.assertRecords(result1, batch1, SCHEMA);
      for (Row row : result1) {
        assertThat(row.getKind()).isEqualTo(RowKind.INSERT);
      }

      // Perform overwrite: add new file, delete existing file (creates OVERWRITE snapshot)
      List<Record> overwriteRecords =
          RandomGenericData.generate(SCHEMA, 3, randomSeed.incrementAndGet());
      DataFile newFile = dataAppender.writeFile(overwriteRecords);
      DataFile existingFile = table.newScan().planFiles().iterator().next().file();
      table.newOverwrite().addFile(newFile).deleteFile(existingFile).commit();

      // Append batch3 (creates APPEND snapshot)
      List<Record> batch3 = RandomGenericData.generate(SCHEMA, 3, randomSeed.incrementAndGet());
      dataAppender.appendToTable(batch3);

      // Changelog mode uses IncrementalChangelogScan:
      // - 3 INSERT from overwrite's added file (overwriteRecords)
      // - 3 DELETE from overwrite's deleted file (batch1 data)
      // - 3 INSERT from batch3
      List<Row> result2 = waitForResult(iter, 9);

      List<Row> insertRows =
          result2.stream()
              .filter(r -> r.getKind() == RowKind.INSERT)
              .collect(Collectors.toList());
      List<Row> deleteRows =
          result2.stream()
              .filter(r -> r.getKind() == RowKind.DELETE)
              .collect(Collectors.toList());

      assertThat(insertRows).hasSize(6);
      assertThat(deleteRows).hasSize(3);

      // Verify INSERT rows contain overwriteRecords + batch3
      List<Record> expectedInserts = Lists.newArrayList();
      expectedInserts.addAll(overwriteRecords);
      expectedInserts.addAll(batch3);
      TestHelpers.assertRecords(insertRows, expectedInserts, SCHEMA);

      // Verify DELETE rows contain batch1's data (with DELETE RowKind)
      List<Row> expectedDeletes = TestHelpers.convertRecordToRow(batch1, SCHEMA);
      for (Row row : expectedDeletes) {
        row.setKind(RowKind.DELETE);
      }
      assertThat(deleteRows).containsExactlyInAnyOrderElementsOf(expectedDeletes);
    }
  }

  @Test
  void testChangelogModeWithAppendOnlySnapshots() throws Exception {
    List<Record> batch1 = RandomGenericData.generate(SCHEMA, 3, randomSeed.incrementAndGet());
    dataAppender.appendToTable(batch1);

    try (CloseableIterator<Row> iter =
        createStream(FlinkReadOptions.STREAMING_CHANGELOG_MODE_CHANGELOG)
            .executeAndCollect(getClass().getSimpleName())) {
      // Initial table scan returns batch1
      List<Row> result1 = waitForResult(iter, 3);
      TestHelpers.assertRecords(result1, batch1, SCHEMA);

      // Pure append (no overwrites) — changelog should emit all as INSERT
      List<Record> batch2 = RandomGenericData.generate(SCHEMA, 3, randomSeed.incrementAndGet());
      dataAppender.appendToTable(batch2);

      List<Row> result2 = waitForResult(iter, 3);
      TestHelpers.assertRecords(result2, batch2, SCHEMA);

      // All should be INSERT (no deletes since these are pure appends)
      for (Row row : result2) {
        assertThat(row.getKind()).isEqualTo(RowKind.INSERT);
      }
    }
  }

  @Test
  void testUpsertModeWithMultipleOverwrites() throws Exception {
    List<Record> batch1 = RandomGenericData.generate(SCHEMA, 3, randomSeed.incrementAndGet());
    dataAppender.appendToTable(batch1);

    try (CloseableIterator<Row> iter =
        createStream(FlinkReadOptions.STREAMING_CHANGELOG_MODE_UPSERT)
            .executeAndCollect(getClass().getSimpleName())) {
      // Initial table scan returns batch1
      List<Row> result1 = waitForResult(iter, 3);
      TestHelpers.assertRecords(result1, batch1, SCHEMA);

      // First overwrite
      List<Record> overwrite1 =
          RandomGenericData.generate(SCHEMA, 3, randomSeed.incrementAndGet());
      DataFile newFile1 = dataAppender.writeFile(overwrite1);
      DataFile existingFile1 = table.newScan().planFiles().iterator().next().file();
      table.newOverwrite().addFile(newFile1).deleteFile(existingFile1).commit();

      // Second overwrite
      List<Record> overwrite2 =
          RandomGenericData.generate(SCHEMA, 3, randomSeed.incrementAndGet());
      DataFile newFile2 = dataAppender.writeFile(overwrite2);
      DataFile existingFile2 = table.newScan().planFiles().iterator().next().file();
      table.newOverwrite().addFile(newFile2).deleteFile(existingFile2).commit();

      // Upsert mode should see added files from both overwrites
      List<Record> expectedIncremental = Lists.newArrayList();
      expectedIncremental.addAll(overwrite1);
      expectedIncremental.addAll(overwrite2);

      List<Row> result2 = waitForResult(iter, 6);
      TestHelpers.assertRecords(result2, expectedIncremental, SCHEMA);

      for (Row row : result2) {
        assertThat(row.getKind()).isEqualTo(RowKind.INSERT);
      }
    }
  }

  private DataStream<Row> createStream(String changelogMode) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    return env.fromSource(
            IcebergSource.forRowData()
                .tableLoader(CATALOG_EXTENSION.tableLoader())
                .assignerFactory(new SimpleSplitAssignerFactory())
                .streaming(true)
                .streamingStartingStrategy(
                    StreamingStartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
                .monitorInterval(Duration.ofMillis(10L))
                .set(FlinkReadOptions.STREAMING_CHANGELOG_MODE, changelogMode)
                .build(),
            WatermarkStrategy.noWatermarks(),
            "icebergSource",
            TypeInformation.of(RowData.class))
        .map(new RowDataToRowMapper(FlinkSchemaUtil.convert(table.schema())));
  }

  private static List<Row> waitForResult(CloseableIterator<Row> iter, int limit) {
    return TestIcebergSourceContinuous.waitForResult(iter, limit);
  }
}
