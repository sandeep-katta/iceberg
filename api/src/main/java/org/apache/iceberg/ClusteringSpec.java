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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.BoundReference;
import org.apache.iceberg.expressions.BoundTerm;
import org.apache.iceberg.expressions.BoundTransform;
import org.apache.iceberg.expressions.Term;
import org.apache.iceberg.expressions.UnboundTerm;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Type;

/**
 * A clustering spec that defines the columns used for liquid clustering of data files in a table.
 *
 * <p>Unlike sort orders, clustering specs do not impose a strict ordering — they define the
 * dimensions used by a space-filling curve (Z-order or Hilbert) to co-locate related data.
 */
public class ClusteringSpec implements Serializable {

  /** Spec ID reserved for tables with no clustering. */
  public static final int UNPARTITIONED_SPEC_ID = 0;

  private static final ClusteringSpec UNCLUSTERED =
      new ClusteringSpec(new Schema(), UNPARTITIONED_SPEC_ID, Collections.emptyList());

  private final Schema schema;
  private final int specId;
  private final ClusteringField[] fields;

  private transient volatile List<ClusteringField> fieldList;

  private ClusteringSpec(Schema schema, int specId, List<ClusteringField> fields) {
    this.schema = schema;
    this.specId = specId;
    this.fields = fields.toArray(new ClusteringField[0]);
  }

  /** Returns the {@link Schema} for this clustering spec. */
  public Schema schema() {
    return schema;
  }

  /** Returns the ID of this clustering spec. */
  public int specId() {
    return specId;
  }

  /** Returns the list of {@link ClusteringField clustering fields} for this spec. */
  public List<ClusteringField> fields() {
    return lazyFieldList();
  }

  /** Returns true if this spec defines clustering columns. */
  public boolean isClustered() {
    return fields.length >= 1;
  }

  /** Returns true if this spec has no clustering columns. */
  public boolean isUnclustered() {
    return fields.length < 1;
  }

  /**
   * Checks whether this spec is equivalent to another spec while ignoring the spec id.
   *
   * @param anotherSpec another clustering spec
   * @return true if this spec has the same fields as the given spec
   */
  public boolean sameSpec(ClusteringSpec anotherSpec) {
    return Arrays.equals(fields, anotherSpec.fields);
  }

  public UnboundClusteringSpec toUnbound() {
    UnboundClusteringSpec.Builder builder = UnboundClusteringSpec.builder().withSpecId(specId);
    for (ClusteringField field : fields) {
      builder.addField(field.transform().toString(), field.sourceId(), field.name());
    }
    return builder.build();
  }

  private List<ClusteringField> lazyFieldList() {
    if (fieldList == null) {
      synchronized (this) {
        if (fieldList == null) {
          this.fieldList = ImmutableList.copyOf(fields);
        }
      }
    }
    return fieldList;
  }

  /** Returns a clustering spec for unclustered tables. */
  public static ClusteringSpec unclustered() {
    return UNCLUSTERED;
  }

  /**
   * Creates a new {@link Builder clustering spec builder} for the given {@link Schema}.
   *
   * @param schema a schema
   * @return a clustering spec builder for the given schema
   */
  public static Builder builderFor(Schema schema) {
    return new Builder(schema);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    for (ClusteringField field : fields) {
      sb.append("\n");
      sb.append("  ").append(field);
    }
    if (fields.length > 0) {
      sb.append("\n");
    }
    sb.append("]");
    return sb.toString();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (other == null || getClass() != other.getClass()) {
      return false;
    }

    ClusteringSpec that = (ClusteringSpec) other;
    return specId == that.specId && sameSpec(that);
  }

  @Override
  public int hashCode() {
    return 31 * Integer.hashCode(specId) + Arrays.hashCode(fields);
  }

  /** A builder for {@link ClusteringSpec}. */
  public static class Builder {
    private final Schema schema;
    private final List<ClusteringField> fields = Lists.newArrayList();
    private Integer specId = null;
    private boolean caseSensitive = true;

    private Builder(Schema schema) {
      this.schema = schema;
    }

    /**
     * Add a column name as a clustering key (identity transform).
     *
     * @param name a column name
     * @return this for method chaining
     */
    public Builder clusterBy(String name) {
      return addField(name, name);
    }

    /**
     * Add an expression term as a clustering key (supports transforms like month(ts),
     * bucket(id,N)).
     *
     * @param term an expression term
     * @param fieldName name to use for this clustering field
     * @return this for method chaining
     */
    public Builder clusterBy(Term term, String fieldName) {
      return addField(term, fieldName);
    }

    public Builder withSpecId(int newSpecId) {
      this.specId = newSpecId;
      return this;
    }

    public Builder caseSensitive(boolean isCaseSensitive) {
      this.caseSensitive = isCaseSensitive;
      return this;
    }

    private Builder addField(String name, String fieldName) {
      return addField(
          org.apache.iceberg.expressions.Expressions.ref(name), fieldName);
    }

    private Builder addField(Term term, String fieldName) {
      Preconditions.checkArgument(term instanceof UnboundTerm, "Term must be unbound");
      BoundTerm<?> boundTerm = ((UnboundTerm<?>) term).bind(schema.asStruct(), caseSensitive);
      int sourceId = boundTerm.ref().fieldId();
      ClusteringField field =
          new ClusteringField(toTransform(boundTerm), sourceId, fieldName);
      fields.add(field);
      return this;
    }

    Builder addField(Transform<?, ?> transform, int sourceId, String fieldName) {
      fields.add(new ClusteringField(transform, sourceId, fieldName));
      return this;
    }

    public ClusteringSpec build() {
      ClusteringSpec spec = buildUnchecked();
      checkCompatibility(spec, schema);
      return spec;
    }

    ClusteringSpec buildUnchecked() {
      if (fields.isEmpty()) {
        if (specId != null && specId != UNPARTITIONED_SPEC_ID) {
          throw new IllegalArgumentException("Unclustered spec ID must be 0");
        }
        return ClusteringSpec.unclustered();
      }

      if (specId != null && specId == UNPARTITIONED_SPEC_ID) {
        throw new IllegalArgumentException(
            "Clustering spec ID 0 is reserved for unclustered spec");
      }

      int actualSpecId = specId != null ? specId : 1;
      return new ClusteringSpec(schema, actualSpecId, fields);
    }

    private Transform<?, ?> toTransform(BoundTerm<?> term) {
      if (term instanceof BoundReference) {
        return Transforms.identity(term.type());
      } else if (term instanceof BoundTransform) {
        return ((BoundTransform<?, ?>) term).transform();
      } else {
        throw new ValidationException(
            "Invalid term: %s, expected either a bound reference or transform", term);
      }
    }
  }

  public static void checkCompatibility(ClusteringSpec spec, Schema schema) {
    for (ClusteringField field : spec.fields) {
      Type sourceType = schema.findType(field.sourceId());
      ValidationException.check(
          sourceType != null, "Cannot find source column for clustering field: %s", field);
      ValidationException.check(
          sourceType.isPrimitiveType(),
          "Cannot cluster by non-primitive source field: %s",
          sourceType);
      ValidationException.check(
          field.transform().canTransform(sourceType),
          "Invalid source type %s for transform: %s",
          sourceType,
          field.transform());
    }
  }
}
