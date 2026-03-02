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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Iterator;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

/** Parser for {@link ClusteringSpec} — serializes to/from JSON in table metadata. */
public class ClusteringSpecParser {

  static final String SPEC_ID = "spec-id";
  static final String FIELDS = "fields";
  static final String SOURCE_ID = "source-id";
  static final String TRANSFORM = "transform";
  static final String NAME = "name";

  private ClusteringSpecParser() {}

  public static void toJson(ClusteringSpec spec, JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeNumberField(SPEC_ID, spec.specId());
    generator.writeFieldName(FIELDS);
    generator.writeStartArray();
    for (ClusteringField field : spec.fields()) {
      generator.writeStartObject();
      generator.writeStringField(TRANSFORM, field.transform().toString());
      generator.writeNumberField(SOURCE_ID, field.sourceId());
      generator.writeStringField(NAME, field.name());
      generator.writeEndObject();
    }
    generator.writeEndArray();
    generator.writeEndObject();
  }

  public static String toJson(ClusteringSpec spec) {
    return JsonUtil.generate(gen -> toJson(spec, gen), false);
  }

  public static ClusteringSpec fromJson(Schema schema, String json) {
    UnboundClusteringSpec unbound = JsonUtil.parse(json, ClusteringSpecParser::fromJson);
    return unbound.bind(schema);
  }

  public static ClusteringSpec fromJson(Schema schema, JsonNode json, int defaultSpecId) {
    UnboundClusteringSpec unboundSpec = fromJson(json);
    if (unboundSpec.specId() == defaultSpecId) {
      return unboundSpec.bind(schema);
    } else {
      return unboundSpec.bindUnchecked(schema);
    }
  }

  public static UnboundClusteringSpec fromJson(JsonNode json) {
    Preconditions.checkArgument(
        json.isObject(), "Cannot parse clustering spec from non-object: %s", json);
    int specId = JsonUtil.getInt(SPEC_ID, json);
    UnboundClusteringSpec.Builder builder = UnboundClusteringSpec.builder().withSpecId(specId);

    JsonNode fieldsNode = JsonUtil.get(FIELDS, json);
    Preconditions.checkArgument(
        fieldsNode.isArray(), "Cannot parse clustering spec fields, not an array: %s", fieldsNode);

    Iterator<JsonNode> elements = fieldsNode.elements();
    while (elements.hasNext()) {
      JsonNode element = elements.next();
      Preconditions.checkArgument(
          element.isObject(), "Cannot parse clustering field, not an object: %s", element);
      String transform = JsonUtil.getString(TRANSFORM, element);
      int sourceId = JsonUtil.getInt(SOURCE_ID, element);
      String name = JsonUtil.getString(NAME, element);
      builder.addField(transform, sourceId, name);
    }

    return builder.build();
  }
}
