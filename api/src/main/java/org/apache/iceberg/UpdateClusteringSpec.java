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

import org.apache.iceberg.expressions.Term;

/**
 * API for defining or replacing the liquid clustering spec for a table.
 *
 * <p>The clustering spec defines which columns are used by the incremental OPTIMIZE operation to
 * co-locate related data using a space-filling curve (Z-order or Hilbert).
 *
 * <p>Apply returns the new {@link ClusteringSpec} for validation.
 *
 * <p>When committing, these changes will be applied to the current table metadata. Commit conflicts
 * will be resolved by applying the pending changes to the new table metadata.
 */
public interface UpdateClusteringSpec extends PendingUpdate<ClusteringSpec> {

  /**
   * Add columns to cluster by (using identity transform).
   *
   * @param columns one or more column names to cluster by
   * @return this for method chaining
   */
  UpdateClusteringSpec clusterBy(String... columns);

  /**
   * Add an expression term to cluster by (supports transforms like {@code month(ts)}, {@code
   * bucket(id, 16)}).
   *
   * @param term an expression term
   * @param fieldName name to use for this clustering field
   * @return this for method chaining
   */
  UpdateClusteringSpec clusterBy(Term term, String fieldName);

  /**
   * Remove all clustering columns, making the table unclustered.
   *
   * @return this for method chaining
   */
  UpdateClusteringSpec removeClusteringSpec();

  /**
   * Set case sensitivity for column name resolution.
   *
   * @param caseSensitive whether column names are case-sensitive
   * @return this for method chaining
   */
  UpdateClusteringSpec caseSensitive(boolean caseSensitive);
}
