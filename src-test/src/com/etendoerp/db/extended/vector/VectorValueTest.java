package com.etendoerp.db.extended.vector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

class VectorValueTest {
  @Test void collectionValidatesNamespaceAndDimensions() {
    assertThrows(IllegalArgumentException.class, () -> new VectorCollection("bad namespace", 3, DistanceMetric.COSINE, false, false));
    assertThrows(IllegalArgumentException.class, () -> new VectorCollection("catalog.product", 0, DistanceMetric.COSINE, false, false));
  }
  @Test void metricOwnsTheOnlySearchOperators() {
    assertEquals("<=>", DistanceMetric.COSINE.getOperator());
    assertEquals("<->", DistanceMetric.L2.getOperator());
    assertEquals("<#>", DistanceMetric.INNER_PRODUCT.getOperator());
  }

  @Test void globalSearchOnlyCombinesCompatibleEmbeddingProfiles() {
    VectorSearchSource first = new VectorSearchSource("source-a", "catalog.product",
        DistanceMetric.COSINE, "OPENAI", "text-embedding-3-small", 1536);
    VectorSearchSource compatible = new VectorSearchSource("source-b", "catalog.category",
        DistanceMetric.COSINE, "OPENAI", "text-embedding-3-small", 1536);
    VectorSearchSource incompatible = new VectorSearchSource("source-c", "catalog.vendor",
        DistanceMetric.COSINE, "OPENAI", "text-embedding-3-large", 3072);

    assertTrue(first.hasCompatibleEmbeddingProfile(compatible));
    assertFalse(first.hasCompatibleEmbeddingProfile(incompatible));
  }
}
