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

import java.util.List;
import java.util.Set;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.FluentIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.TableScanUtil;

/**
 * An incremental scan that includes data files from all data-changing snapshot types (append,
 * overwrite, delete) between two snapshots. Unlike {@link BaseIncrementalAppendScan}, this scan
 * also includes delete manifests so that equality and position deletes are properly paired with data
 * files.
 *
 * <p>This scan is designed for reading incremental changes from upsert-enabled tables where
 * row-level mutations produce overwrite snapshots with equality delete files.
 */
class BaseIncrementalDataScan
    extends BaseIncrementalScan<IncrementalDataScan, FileScanTask, CombinedScanTask>
    implements IncrementalDataScan {

  BaseIncrementalDataScan(Table table, Schema schema, TableScanContext context) {
    super(table, schema, context);
  }

  @Override
  protected IncrementalDataScan newRefinedScan(
      Table newTable, Schema newSchema, TableScanContext newContext) {
    return new BaseIncrementalDataScan(newTable, newSchema, newContext);
  }

  @Override
  protected CloseableIterable<FileScanTask> doPlanFiles(
      Long fromSnapshotIdExclusive, long toSnapshotIdInclusive) {

    List<Snapshot> snapshots =
        dataChangingSnapshotsBetween(table(), fromSnapshotIdExclusive, toSnapshotIdInclusive);
    if (snapshots.isEmpty()) {
      return CloseableIterable.empty();
    }

    return filesFromSnapshots(snapshots);
  }

  @Override
  public CloseableIterable<CombinedScanTask> planTasks() {
    CloseableIterable<FileScanTask> fileScanTasks = planFiles();
    CloseableIterable<FileScanTask> splitFiles =
        TableScanUtil.splitFiles(fileScanTasks, targetSplitSize());
    return TableScanUtil.planTasks(
        splitFiles, targetSplitSize(), splitLookback(), splitOpenFileCost());
  }

  private CloseableIterable<FileScanTask> filesFromSnapshots(List<Snapshot> snapshots) {
    Set<Long> snapshotIds = Sets.newHashSet(Iterables.transform(snapshots, Snapshot::snapshotId));

    Set<ManifestFile> dataManifests =
        FluentIterable.from(snapshots)
            .transformAndConcat(snapshot -> snapshot.dataManifests(table().io()))
            .filter(manifestFile -> snapshotIds.contains(manifestFile.snapshotId()))
            .toSet();

    Iterable<ManifestFile> deleteManifests =
        FluentIterable.from(snapshots)
            .transformAndConcat(snapshot -> snapshot.deleteManifests(table().io()))
            .toSet();

    ManifestGroup manifestGroup =
        new ManifestGroup(table().io(), dataManifests, deleteManifests)
            .caseSensitive(isCaseSensitive())
            .select(scanColumns())
            .filterData(filter())
            .filterManifestEntries(
                manifestEntry ->
                    snapshotIds.contains(manifestEntry.snapshotId())
                        && manifestEntry.status() == ManifestEntry.Status.ADDED)
            .specsById(table().specs())
            .ignoreDeleted()
            .columnsToKeepStats(columnsToKeepStats());

    if (context().ignoreResiduals()) {
      manifestGroup = manifestGroup.ignoreResiduals();
    }

    if (dataManifests.size() > 1 && shouldPlanWithExecutor()) {
      manifestGroup = manifestGroup.planWith(planExecutor());
    }

    return manifestGroup.planFiles();
  }

  /**
   * Returns all data-changing snapshots between the given snapshot IDs, excluding REPLACE
   * operations (which are file rewrites that don't change data).
   *
   * <p>This includes APPEND, OVERWRITE, and DELETE operations, which covers upsert (RowDelta)
   * commits that produce OVERWRITE snapshots.
   */
  private static List<Snapshot> dataChangingSnapshotsBetween(
      Table table, Long fromSnapshotIdExclusive, long toSnapshotIdInclusive) {
    List<Snapshot> snapshots = Lists.newArrayList();
    for (Snapshot snapshot :
        SnapshotUtil.ancestorsBetween(
            toSnapshotIdInclusive, fromSnapshotIdExclusive, table::snapshot)) {
      if (!snapshot.operation().equals(DataOperations.REPLACE)) {
        snapshots.add(snapshot);
      }
    }

    return snapshots;
  }
}
