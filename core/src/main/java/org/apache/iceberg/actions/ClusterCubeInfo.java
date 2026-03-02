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
import org.apache.iceberg.FileScanTask;

/**
 * Tracks the accumulated state of a liquid cluster cube — a group of files that were written
 * together by a previous clustering OPTIMIZE run and share the same {@code clusterCubeId}.
 *
 * <p>A cube is considered "sealed" once its total size reaches {@code minCubeSizeBytes}. Sealed
 * cubes are skipped by the incremental planner and never rewritten, bounding write amplification.
 */
class ClusterCubeInfo {

  private final String cubeId;
  private final int clusterSpecId;
  private final List<FileScanTask> tasks;
  private final long totalSizeBytes;

  ClusterCubeInfo(String cubeId, int clusterSpecId, List<FileScanTask> tasks) {
    this.cubeId = cubeId;
    this.clusterSpecId = clusterSpecId;
    this.tasks = tasks;
    this.totalSizeBytes = tasks.stream().mapToLong(t -> t.file().fileSizeInBytes()).sum();
  }

  String cubeId() {
    return cubeId;
  }

  int clusterSpecId() {
    return clusterSpecId;
  }

  List<FileScanTask> tasks() {
    return tasks;
  }

  long totalSizeBytes() {
    return totalSizeBytes;
  }

  int fileCount() {
    return tasks.size();
  }

  /**
   * Returns true if this cube has grown large enough to be sealed. Sealed cubes are never rewritten
   * by subsequent OPTIMIZE runs.
   */
  boolean isSealed(long minCubeSizeBytes) {
    return totalSizeBytes >= minCubeSizeBytes;
  }
}
