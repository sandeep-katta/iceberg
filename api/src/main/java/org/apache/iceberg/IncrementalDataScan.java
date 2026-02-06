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

/**
 * API for configuring an incremental table scan that includes all data-changing snapshots.
 *
 * <p>Unlike {@link IncrementalAppendScan} which only considers append snapshots, this scan includes
 * data files from all snapshot types (append, overwrite, delete) except replace operations. This is
 * useful for reading changes from upsert-enabled tables where row-level mutations produce overwrite
 * snapshots with equality delete files.
 *
 * <p>Delete files from the scanned snapshots are included so that the reader can properly apply
 * them and filter out logically deleted rows.
 */
public interface IncrementalDataScan
    extends IncrementalScan<IncrementalDataScan, FileScanTask, CombinedScanTask> {}
