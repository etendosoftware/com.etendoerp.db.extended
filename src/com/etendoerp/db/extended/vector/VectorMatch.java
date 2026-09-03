package com.etendoerp.db.extended.vector;

/** Generic nearest-neighbour search result. */
public final class VectorMatch {
  private final String key, metadata; private final double distance;
  public VectorMatch(String key, String metadata, double distance) { this.key = key; this.metadata = metadata; this.distance = distance; }
  public String getKey() { return key; } public String getMetadata() { return metadata; } public double getDistance() { return distance; }
}
