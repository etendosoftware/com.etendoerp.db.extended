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
