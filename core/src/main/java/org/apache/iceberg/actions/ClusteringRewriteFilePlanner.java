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
package org.apache.iceberg.actions;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.ClusteringSpec;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.RewriteDataFiles.FileGroupInfo;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.PropertyUtil;

/**
 * A rewrite file planner that implements incremental liquid clustering.
 *
 * <p>Files are grouped by their {@code clusterCubeId}:
 *
 * <ul>
 *   <li>Files with no cube ID (never clustered) → always eligible for rewrite
 *   <li>Files whose {@code clusterSpecId} differs from the current spec → treated as unclustered
 *       (cluster keys changed)
 *   <li>Files in a cube whose total size &ge; {@code minCubeSizeBytes} → <em>sealed</em>, skipped
 *   <li>Single-file cubes with no unclustered peers in the partition → skipped
 * </ul>
 *
 * <p>Eligible files are bin-packed into rewrite groups (inheriting from {@link
 * SizeBasedFileRewritePlanner}) and sorted using the configured curve (Z-order by default, Hilbert
 * when configured via {@link TableProperties#WRITE_CLUSTERING_CURVE}).
 */
public class ClusteringRewriteFilePlanner
    extends BinPackRewriteFilePlanner {

  /** Strategy name to pass to {@code RewriteDataFiles.option(STRATEGY, "cluster")}. */
  public static final String CLUSTERING_STRATEGY_NAME = "cluster";

  /**
   * Per-run override for the minimum cube size threshold (bytes). Falls back to the table property
   * {@link TableProperties#WRITE_CLUSTERING_MIN_CUBE_SIZE_BYTES}.
   */
  public static final String MIN_CUBE_SIZE_BYTES = "min-cube-size-bytes";

  /**
   * Per-run override for the target cube size threshold (bytes). Falls back to the table property
   * {@link TableProperties#WRITE_CLUSTERING_TARGET_CUBE_SIZE_BYTES}.
   */
  public static final String TARGET_CUBE_SIZE_BYTES = "target-cube-size-bytes";

  private long minCubeSizeBytes;

  public ClusteringRewriteFilePlanner(Table table) {
    this(table, Expressions.alwaysTrue());
  }

  public ClusteringRewriteFilePlanner(Table table, Expression filter) {
    super(table, filter);
  }

  @Override
  public Set<String> validOptions() {
    return ImmutableSet.<String>builder()
        .addAll(super.validOptions())
        .add(MIN_CUBE_SIZE_BYTES)
        .add(TARGET_CUBE_SIZE_BYTES)
        .build();
  }

  @Override
  public void init(Map<String, String> options) {
    super.init(options);
    this.minCubeSizeBytes =
        PropertyUtil.propertyAsLong(
            options,
            MIN_CUBE_SIZE_BYTES,
            PropertyUtil.propertyAsLong(
                table().properties(),
                TableProperties.WRITE_CLUSTERING_MIN_CUBE_SIZE_BYTES,
                TableProperties.WRITE_CLUSTERING_MIN_CUBE_SIZE_BYTES_DEFAULT));
  }

  /**
   * Overrides the base filter to apply sealed-cube logic on top of the standard size-based filter.
   *
   * <p>Only files that are either unclustered, belong to a stale clustering spec, or belong to an
   * unsealed cube are kept for rewriting.
   */
  @Override
  protected Iterable<FileScanTask> filterFiles(Iterable<FileScanTask> tasks) {
    int currentSpecId = currentClusteringSpecId();

    // Build cube map from all tasks (even those that might be filtered by size)
    Map<String, ClusterCubeInfo> cubeMap = buildCubeMap(tasks, currentSpecId);

    // Identify partitions that have unclustered files so we can decide whether to skip
    // single-file cubes in those partitions
    Set<String> partitionsWithUnclustered = partitionsWithUnclusteredFiles(tasks, currentSpecId);

    // Apply filter: keep files that need clustering
    List<FileScanTask> eligible = Lists.newArrayList();
    for (FileScanTask task : tasks) {
      DataFile file = task.file();
      String cubeId = file.clusterCubeId();
      Integer fileSpecId = file.clusterSpecId();

      // Unclustered (no cube ID)
      if (cubeId == null) {
        eligible.add(task);
        continue;
      }

      // Stale spec (cluster keys changed) — treat as unclustered
      if (fileSpecId == null || fileSpecId != currentSpecId) {
        eligible.add(task);
        continue;
      }

      // Check cube state
      ClusterCubeInfo cube = cubeMap.get(cubeId);
      if (cube == null) {
        eligible.add(task);
        continue;
      }

      // Sealed cube — skip
      if (cube.isSealed(minCubeSizeBytes)) {
        continue;
      }

      // Single-file cube with no unclustered peers → skip
      String partitionKey = partitionKey(task);
      if (cube.fileCount() == 1 && !partitionsWithUnclustered.contains(partitionKey)) {
        continue;
      }

      eligible.add(task);
    }

    return eligible;
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private int currentClusteringSpecId() {
    ClusteringSpec spec = table().clusteringSpec();
    return spec != null ? spec.specId() : ClusteringSpec.UNPARTITIONED_SPEC_ID;
  }

  /**
   * Builds a map of cubeId → {@link ClusterCubeInfo} for files that belong to the current
   * clustering spec.
   */
  private Map<String, ClusterCubeInfo> buildCubeMap(
      Iterable<FileScanTask> tasks, int currentSpecId) {
    // First pass: group tasks by cubeId
    Map<String, List<FileScanTask>> tasksByCube = Maps.newHashMap();
    Map<String, Integer> cubeSpecIds = Maps.newHashMap();

    for (FileScanTask task : tasks) {
      DataFile file = task.file();
      String cubeId = file.clusterCubeId();
      Integer fileSpecId = file.clusterSpecId();

      if (cubeId == null || fileSpecId == null || fileSpecId != currentSpecId) {
        continue;
      }

      tasksByCube.computeIfAbsent(cubeId, unused -> Lists.newArrayList()).add(task);
      cubeSpecIds.put(cubeId, fileSpecId);
    }

    // Second pass: build ClusterCubeInfo objects
    Map<String, ClusterCubeInfo> cubeMap = Maps.newHashMap();
    for (Map.Entry<String, List<FileScanTask>> entry : tasksByCube.entrySet()) {
      String cubeId = entry.getKey();
      int specId = cubeSpecIds.get(cubeId);
      cubeMap.put(cubeId, new ClusterCubeInfo(cubeId, specId, entry.getValue()));
    }

    return cubeMap;
  }

  /**
   * Returns the set of partition keys that contain at least one unclustered file (no cube ID or
   * stale spec ID).
   */
  private Set<String> partitionsWithUnclusteredFiles(
      Iterable<FileScanTask> tasks, int currentSpecId) {
    Set<String> result = Sets.newHashSet();
    for (FileScanTask task : tasks) {
      DataFile file = task.file();
      String cubeId = file.clusterCubeId();
      Integer fileSpecId = file.clusterSpecId();

      boolean isUnclustered =
          cubeId == null || fileSpecId == null || fileSpecId != currentSpecId;

      if (isUnclustered) {
        result.add(partitionKey(task));
      }
    }
    return result;
  }

  /** Returns a string key identifying the partition of a scan task. */
  private String partitionKey(FileScanTask task) {
    return task.file().partition().toString();
  }
}
