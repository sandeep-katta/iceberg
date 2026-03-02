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

import java.util.Collections;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Type;

/** An unbound {@link ClusteringSpec} used for deserialization before schema binding. */
public class UnboundClusteringSpec {

  private static final UnboundClusteringSpec UNCLUSTERED =
      new UnboundClusteringSpec(ClusteringSpec.UNPARTITIONED_SPEC_ID, Collections.emptyList());

  private final int specId;
  private final List<UnboundClusteringField> fields;

  private UnboundClusteringSpec(int specId, List<UnboundClusteringField> fields) {
    this.specId = specId;
    this.fields = fields;
  }

  public int specId() {
    return specId;
  }

  List<UnboundClusteringField> fields() {
    return fields;
  }

  /** Binds this unbound spec to the given schema, validating field compatibility. */
  public ClusteringSpec bind(Schema schema) {
    ClusteringSpec.Builder builder = ClusteringSpec.builderFor(schema).withSpecId(specId);
    for (UnboundClusteringField field : fields) {
      Type sourceType = schema.findType(field.sourceId);
      Transform<?, ?> transform;
      if (sourceType != null) {
        transform = Transforms.fromString(sourceType, field.transform.toString());
      } else {
        transform = field.transform;
      }
      builder.addField(transform, field.sourceId, field.name);
    }
    return builder.build();
  }

  /** Binds without validation — used when reading older metadata that may be schema-stale. */
  ClusteringSpec bindUnchecked(Schema schema) {
    ClusteringSpec.Builder builder = ClusteringSpec.builderFor(schema).withSpecId(specId);
    for (UnboundClusteringField field : fields) {
      builder.addField(field.transform, field.sourceId, field.name);
    }
    return builder.buildUnchecked();
  }

  static Builder builder() {
    return new Builder();
  }

  static class Builder {
    private final List<UnboundClusteringField> fields = Lists.newArrayList();
    private Integer specId = null;

    private Builder() {}

    Builder withSpecId(int newSpecId) {
      this.specId = newSpecId;
      return this;
    }

    Builder addField(String transformAsString, int sourceId, String name) {
      fields.add(new UnboundClusteringField(transformAsString, sourceId, name));
      return this;
    }

    UnboundClusteringSpec build() {
      if (fields.isEmpty()) {
        if (specId != null && specId != ClusteringSpec.UNPARTITIONED_SPEC_ID) {
          throw new IllegalArgumentException("Unclustered spec ID must be 0");
        }
        return UNCLUSTERED;
      }

      if (specId != null && specId == ClusteringSpec.UNPARTITIONED_SPEC_ID) {
        throw new IllegalArgumentException(
            "Clustering spec ID 0 is reserved for unclustered spec");
      }

      int actualSpecId = specId != null ? specId : 1;
      return new UnboundClusteringSpec(actualSpecId, fields);
    }
  }

  static class UnboundClusteringField {
    private final Transform<?, ?> transform;
    private final int sourceId;
    private final String name;

    private UnboundClusteringField(String transformAsString, int sourceId, String name) {
      this.transform = Transforms.fromString(transformAsString);
      this.sourceId = sourceId;
      this.name = name;
    }

    public String transformAsString() {
      return transform.toString();
    }

    public int sourceId() {
      return sourceId;
    }

    public String name() {
      return name;
    }
  }
}
