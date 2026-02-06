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

import org.apache.iceberg.flink.FlinkReadOptions;
import org.junit.jupiter.api.Test;

class TestFlinkSplitPlannerScanMode {

  @Test
  void testBatchModeWhenNoIncrementalRange() {
    ScanContext context = ScanContext.builder().build();
    assertThat(FlinkSplitPlanner.checkScanMode(context))
        .isEqualTo(FlinkSplitPlanner.ScanMode.BATCH);
  }

  @Test
  void testIncrementalAppendScanByDefault() {
    ScanContext context = ScanContext.builder().startSnapshotId(1L).build();
    assertThat(FlinkSplitPlanner.checkScanMode(context))
        .isEqualTo(FlinkSplitPlanner.ScanMode.INCREMENTAL_APPEND_SCAN);
  }

  @Test
  void testIncrementalAppendScanWithNoneMode() {
    ScanContext context =
        ScanContext.builder()
            .startSnapshotId(1L)
            .streamingChangelogMode(FlinkReadOptions.STREAMING_CHANGELOG_MODE_NONE)
            .build();
    assertThat(FlinkSplitPlanner.checkScanMode(context))
        .isEqualTo(FlinkSplitPlanner.ScanMode.INCREMENTAL_APPEND_SCAN);
  }

  @Test
  void testIncrementalDataScanWithUpsertMode() {
    ScanContext context =
        ScanContext.builder()
            .startSnapshotId(1L)
            .streamingChangelogMode(FlinkReadOptions.STREAMING_CHANGELOG_MODE_UPSERT)
            .build();
    assertThat(FlinkSplitPlanner.checkScanMode(context))
        .isEqualTo(FlinkSplitPlanner.ScanMode.INCREMENTAL_DATA_SCAN);
  }

  @Test
  void testIncrementalDataScanWithChangelogMode() {
    ScanContext context =
        ScanContext.builder()
            .startSnapshotId(1L)
            .streamingChangelogMode(FlinkReadOptions.STREAMING_CHANGELOG_MODE_CHANGELOG)
            .build();
    assertThat(FlinkSplitPlanner.checkScanMode(context))
        .isEqualTo(FlinkSplitPlanner.ScanMode.INCREMENTAL_DATA_SCAN);
  }

  @Test
  void testUpsertModeWithEndSnapshotId() {
    ScanContext context =
        ScanContext.builder()
            .endSnapshotId(1L)
            .streamingChangelogMode(FlinkReadOptions.STREAMING_CHANGELOG_MODE_UPSERT)
            .build();
    assertThat(FlinkSplitPlanner.checkScanMode(context))
        .isEqualTo(FlinkSplitPlanner.ScanMode.INCREMENTAL_DATA_SCAN);
  }

  @Test
  void testUpsertModeWithStartTag() {
    ScanContext context =
        ScanContext.builder()
            .startTag("tag1")
            .streamingChangelogMode(FlinkReadOptions.STREAMING_CHANGELOG_MODE_UPSERT)
            .build();
    assertThat(FlinkSplitPlanner.checkScanMode(context))
        .isEqualTo(FlinkSplitPlanner.ScanMode.INCREMENTAL_DATA_SCAN);
  }

  @Test
  void testUpsertModeWithEndTag() {
    ScanContext context =
        ScanContext.builder()
            .endTag("tag1")
            .streamingChangelogMode(FlinkReadOptions.STREAMING_CHANGELOG_MODE_UPSERT)
            .build();
    assertThat(FlinkSplitPlanner.checkScanMode(context))
        .isEqualTo(FlinkSplitPlanner.ScanMode.INCREMENTAL_DATA_SCAN);
  }

  @Test
  void testBatchModeIgnoresChangelogMode() {
    // Without incremental range, changelog mode should be ignored
    ScanContext context =
        ScanContext.builder()
            .streamingChangelogMode(FlinkReadOptions.STREAMING_CHANGELOG_MODE_UPSERT)
            .build();
    assertThat(FlinkSplitPlanner.checkScanMode(context))
        .isEqualTo(FlinkSplitPlanner.ScanMode.BATCH);
  }

  @Test
  void testUpsertModeCaseInsensitive() {
    ScanContext context =
        ScanContext.builder().startSnapshotId(1L).streamingChangelogMode("UPSERT").build();
    assertThat(FlinkSplitPlanner.checkScanMode(context))
        .isEqualTo(FlinkSplitPlanner.ScanMode.INCREMENTAL_DATA_SCAN);
  }
}
