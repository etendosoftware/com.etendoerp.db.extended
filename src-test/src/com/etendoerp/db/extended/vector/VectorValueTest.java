package com.etendoerp.db.extended.vector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
}
