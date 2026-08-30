package com.etendoerp.db.extended.vector;

import java.util.Objects;
import java.util.regex.Pattern;

/** Immutable generic vector collection definition. */
public final class VectorCollection {
  private static final Pattern NAMESPACE = Pattern.compile("^[A-Za-z][A-Za-z0-9_.-]{0,127}$");
  private final String namespace; private final int dimensions; private final DistanceMetric metric;
  private final boolean clientScoped; private final boolean organizationScoped;
  public VectorCollection(String namespace, int dimensions, DistanceMetric metric, boolean clientScoped, boolean organizationScoped) {
    if (namespace == null || !NAMESPACE.matcher(namespace).matches()) throw new IllegalArgumentException("Invalid vector namespace");
    if (dimensions < 1 || dimensions > 2000) throw new IllegalArgumentException("Vector dimensions must be between 1 and 2000");
    this.namespace = namespace; this.dimensions = dimensions; this.metric = Objects.requireNonNull(metric, "metric");
    this.clientScoped = clientScoped; this.organizationScoped = organizationScoped;
  }
  public String getNamespace() { return namespace; } public int getDimensions() { return dimensions; }
  public DistanceMetric getMetric() { return metric; } public boolean isClientScoped() { return clientScoped; }
  public boolean isOrganizationScoped() { return organizationScoped; }
}
