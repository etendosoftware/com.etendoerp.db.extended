package com.etendoerp.db.extended.vector;

import java.util.Arrays; import java.util.Objects;

/** Immutable exact-search request. Metadata is a JSON object, never a SQL fragment. */
public final class VectorQuery {
  private final String namespace, metadata; private final double[] vector; private final int topK; private final DistanceMetric metric;
  private final String clientId, organizationId;
  public VectorQuery(String namespace, double[] vector, int topK, DistanceMetric metric, String metadata, String clientId, String organizationId) {
    this.namespace = Objects.requireNonNull(namespace, "namespace"); this.vector = Arrays.copyOf(Objects.requireNonNull(vector, "vector"), vector.length);
    if (vector.length == 0 || topK < 1 || topK > 1000) throw new IllegalArgumentException("Invalid vector query");
    this.topK = topK; this.metric = Objects.requireNonNull(metric, "metric"); this.metadata = metadata == null ? "{}" : metadata;
    this.clientId = clientId; this.organizationId = organizationId;
  }
  public String getNamespace() { return namespace; } public double[] getVector() { return Arrays.copyOf(vector, vector.length); }
  public int getTopK() { return topK; } public DistanceMetric getMetric() { return metric; } public String getMetadata() { return metadata; }
  public String getClientId() { return clientId; } public String getOrganizationId() { return organizationId; }
}
