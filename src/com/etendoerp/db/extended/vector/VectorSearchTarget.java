package com.etendoerp.db.extended.vector;

/** Physical source plus its target-owned, already validated metadata predicate. */
final class VectorSearchTarget {
  private final String key;
  private final VectorSearchSource source;
  private final VectorMetadataFilter filter;

  VectorSearchTarget(String key, VectorSearchSource source, VectorMetadataFilter filter) {
    this.key = key;
    this.source = source;
    this.filter = filter;
  }

  String getKey() { return key; }
  VectorSearchSource getSource() { return source; }
  VectorMetadataFilter getFilter() { return filter; }
}
