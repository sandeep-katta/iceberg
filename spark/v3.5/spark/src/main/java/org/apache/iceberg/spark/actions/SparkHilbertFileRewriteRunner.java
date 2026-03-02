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
package org.apache.iceberg.spark.actions;

import static org.apache.spark.sql.functions.array;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.ZOrderByteUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spark file rewrite runner that sorts files using a Hilbert space-filling curve.
 *
 * <p>The Hilbert curve provides better spatial locality than Z-order because it avoids the
 * quadrant-crossing discontinuities present in Morton (Z-order) codes. This produces tighter data
 * clustering and faster range scans on multi-column predicates.
 *
 * <p>Used by the liquid clustering strategy ({@code strategy = "cluster"}) when {@code
 * write.clustering.curve = hilbert}.
 */
class SparkHilbertFileRewriteRunner extends SparkShufflingFileRewriteRunner {
  private static final Logger LOG = LoggerFactory.getLogger(SparkHilbertFileRewriteRunner.class);

  private static final String H_COLUMN = "ICEHVALUE";
  private static final Schema H_SCHEMA =
      new Schema(Types.NestedField.required(0, H_COLUMN, Types.BinaryType.get()));
  private static final SortOrder H_SORT_ORDER =
      SortOrder.builderFor(H_SCHEMA)
          .sortBy(H_COLUMN, SortDirection.ASC, NullOrder.NULLS_LAST)
          .build();

  /**
   * Controls the maximum number of bytes used in the Hilbert index output. Reducing this truncates
   * the precision of the index but speeds up sorting. Default is no truncation.
   */
  public static final String MAX_OUTPUT_SIZE = "max-output-size";

  public static final int MAX_OUTPUT_SIZE_DEFAULT = Integer.MAX_VALUE;

  /**
   * Controls the number of bytes considered from an input column of a variable-length type (String,
   * Binary). Default matches the primitive buffer size.
   */
  public static final String VAR_LENGTH_CONTRIBUTION = "var-length-contribution";

  public static final int VAR_LENGTH_CONTRIBUTION_DEFAULT = ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE;

  private final List<String> colNames;
  private int maxOutputSize;
  private int varLengthContribution;

  SparkHilbertFileRewriteRunner(SparkSession spark, Table table, List<String> colNames) {
    super(spark, table);
    this.colNames = validColNames(spark, table, colNames);
  }

  @Override
  public String description() {
    return "HILBERT";
  }

  @Override
  public Set<String> validOptions() {
    return ImmutableSet.<String>builder()
        .addAll(super.validOptions())
        .add(MAX_OUTPUT_SIZE)
        .add(VAR_LENGTH_CONTRIBUTION)
        .build();
  }

  @Override
  public void init(Map<String, String> options) {
    super.init(options);
    this.maxOutputSize = maxOutputSize(options);
    this.varLengthContribution = varLengthContribution(options);
  }

  @Override
  protected SortOrder sortOrder() {
    return H_SORT_ORDER;
  }

  @Override
  protected Schema sortSchema() {
    return new Schema(
        new ImmutableList.Builder<Types.NestedField>()
            .addAll(table().schema().columns())
            .addAll(H_SCHEMA.columns())
            .build());
  }

  @Override
  protected Dataset<Row> sortedDF(Dataset<Row> df, Function<Dataset<Row>, Dataset<Row>> sortFunc) {
    Dataset<Row> hValueDF = df.withColumn(H_COLUMN, hValue(df));
    Dataset<Row> sortedDF = sortFunc.apply(hValueDF);
    return sortedDF.drop(H_COLUMN);
  }

  private Column hValue(Dataset<Row> df) {
    SparkHilbertUDF hilbertUDF =
        new SparkHilbertUDF(colNames.size(), varLengthContribution, maxOutputSize);

    Column[] hCols =
        colNames.stream()
            .map(df.schema()::apply)
            .map(col -> hilbertUDF.sortedLexicographically(df.col(col.name()), col.dataType()))
            .toArray(Column[]::new);

    return hilbertUDF.hilbertIndexCol(array(hCols));
  }

  private int varLengthContribution(Map<String, String> options) {
    int value =
        PropertyUtil.propertyAsInt(
            options, VAR_LENGTH_CONTRIBUTION, VAR_LENGTH_CONTRIBUTION_DEFAULT);
    Preconditions.checkArgument(
        value > 0,
        "Cannot use less than 1 byte for variable length types with Hilbert clustering, "
            + "'%s' was set to %s",
        VAR_LENGTH_CONTRIBUTION,
        value);
    return value;
  }

  private int maxOutputSize(Map<String, String> options) {
    int value = PropertyUtil.propertyAsInt(options, MAX_OUTPUT_SIZE, MAX_OUTPUT_SIZE_DEFAULT);
    Preconditions.checkArgument(
        value > 0,
        "Cannot have the Hilbert index use less than 1 byte, '%s' was set to %s",
        MAX_OUTPUT_SIZE,
        value);
    return value;
  }

  private List<String> validColNames(SparkSession spark, Table table, List<String> inputColNames) {
    Preconditions.checkArgument(
        inputColNames != null && !inputColNames.isEmpty(),
        "Cannot cluster when no columns are specified");

    Schema schema = table.schema();
    Set<Integer> identityPartitionFieldIds = table.spec().identitySourceIds();
    boolean caseSensitive = SparkUtil.caseSensitive(spark);

    List<String> valid = Lists.newArrayList();

    for (String colName : inputColNames) {
      Types.NestedField field =
          caseSensitive ? schema.findField(colName) : schema.caseInsensitiveFindField(colName);
      Preconditions.checkArgument(
          field != null,
          "Cannot find column '%s' in table schema (case sensitive = %s): %s",
          colName,
          caseSensitive,
          schema.asStruct());

      if (identityPartitionFieldIds.contains(field.fieldId())) {
        LOG.warn("Ignoring '{}' as such values are constant within a partition", colName);
      } else {
        valid.add(colName);
      }
    }

    Preconditions.checkArgument(
        !valid.isEmpty(),
        "Cannot cluster, all columns provided were identity partition columns and cannot be used");

    return valid;
  }
}
