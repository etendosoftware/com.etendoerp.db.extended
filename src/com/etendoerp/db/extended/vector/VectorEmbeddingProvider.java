package com.etendoerp.db.extended.vector;

/** Provider-neutral conversion of indexable text into a vector. */
public interface VectorEmbeddingProvider {
  double[] embed(String text);
  int dimensions();
}
