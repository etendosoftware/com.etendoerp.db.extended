/*
 *************************************************************************
 * The contents of this file are subject to the Etendo License
 * (the "License"), you may not use this file except in compliance with
 * the License.
 * You may obtain a copy of the License at
 * https://github.com/etendosoftware/etendo_core/blob/main/legal/Etendo_license.txt
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 * All portions are Copyright © 2026 FUTIT SERVICES, S.L
 * All Rights Reserved.
 * Contributor(s): Futit Services S.L.
 *************************************************************************
 */
package com.etendoerp.db.extended.vector;

/** Immutable configured source information used by generic semantic search. */
final class VectorSearchSource {
  private final String id;
  private final String namespace;
  private final DistanceMetric metric;
  private final String providerType;
  private final String model;
  private final int dimensions;

  VectorSearchSource(String id, String namespace, DistanceMetric metric, String providerType,
      String model, int dimensions) {
    this.id = id;
    this.namespace = namespace;
    this.metric = metric;
    this.providerType = providerType;
    this.model = model;
    this.dimensions = dimensions;
  }

  String getId() { return id; }
  String getNamespace() { return namespace; }
  DistanceMetric getMetric() { return metric; }
  int getDimensions() { return dimensions; }

  boolean hasCompatibleEmbeddingProfile(VectorSearchSource other) {
    return metric == other.metric && dimensions == other.dimensions
        && providerType.equals(other.providerType) && model.equals(other.model);
  }
}
