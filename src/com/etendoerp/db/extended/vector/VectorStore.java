package com.etendoerp.db.extended.vector;

import java.util.List;

/** Public generic vector storage API; it has no business-entity dependencies. */
public interface VectorStore {
  void createCollection(VectorCollection collection);
  void upsert(VectorRecord record);
  List<VectorMatch> search(VectorQuery query);
  void delete(String namespace, String key);
  void deleteCollection(String namespace);
}
