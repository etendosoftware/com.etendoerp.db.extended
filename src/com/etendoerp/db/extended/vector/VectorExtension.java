package com.etendoerp.db.extended.vector;

/** Optional CDI SPI for namespace-specific validation/enrichment. */
public interface VectorExtension {
  String namespace();
  default VectorRecord beforeUpsert(VectorRecord record) { return record; }
  default VectorQuery beforeSearch(VectorQuery query) { return query; }
  default void afterDelete(String key) { }
}
