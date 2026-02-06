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

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.flink.FlinkReadOptions;
import org.junit.jupiter.api.Test;

class TestIcebergTableSourceChangelogMode {

  private static ResolvedSchema testSchema() {
    return ResolvedSchema.of(
        Column.physical("id", TypeConversions.fromLogicalToDataType(new IntType())),
        Column.physical("data", TypeConversions.fromLogicalToDataType(new VarCharType(100))));
  }

  private IcebergTableSource createSource(Map<String, String> properties) {
    return new IcebergTableSource(null, testSchema(), properties, new Configuration());
  }

  @Test
  void testDefaultInsertOnlyMode() {
    IcebergTableSource source = createSource(new HashMap<>());
    ChangelogMode mode = source.getChangelogMode();
    assertThat(mode).isEqualTo(ChangelogMode.insertOnly());
  }

  @Test
  void testNoneMode() {
    Map<String, String> props = new HashMap<>();
    props.put(
        FlinkReadOptions.STREAMING_CHANGELOG_MODE,
        FlinkReadOptions.STREAMING_CHANGELOG_MODE_NONE);
    IcebergTableSource source = createSource(props);
    ChangelogMode mode = source.getChangelogMode();
    assertThat(mode).isEqualTo(ChangelogMode.insertOnly());
  }

  @Test
  void testUpsertMode() {
    Map<String, String> props = new HashMap<>();
    props.put(
        FlinkReadOptions.STREAMING_CHANGELOG_MODE,
        FlinkReadOptions.STREAMING_CHANGELOG_MODE_UPSERT);
    IcebergTableSource source = createSource(props);
    ChangelogMode mode = source.getChangelogMode();
    assertThat(mode).isEqualTo(ChangelogMode.upsert());
  }

  @Test
  void testChangelogMode() {
    Map<String, String> props = new HashMap<>();
    props.put(
        FlinkReadOptions.STREAMING_CHANGELOG_MODE,
        FlinkReadOptions.STREAMING_CHANGELOG_MODE_CHANGELOG);
    IcebergTableSource source = createSource(props);
    ChangelogMode mode = source.getChangelogMode();

    assertThat(mode.contains(RowKind.INSERT)).isTrue();
    assertThat(mode.contains(RowKind.UPDATE_BEFORE)).isTrue();
    assertThat(mode.contains(RowKind.UPDATE_AFTER)).isTrue();
    assertThat(mode.contains(RowKind.DELETE)).isTrue();
  }

  @Test
  void testUpsertModeCaseInsensitive() {
    Map<String, String> props = new HashMap<>();
    props.put(FlinkReadOptions.STREAMING_CHANGELOG_MODE, "UPSERT");
    IcebergTableSource source = createSource(props);
    ChangelogMode mode = source.getChangelogMode();
    assertThat(mode).isEqualTo(ChangelogMode.upsert());
  }
}
