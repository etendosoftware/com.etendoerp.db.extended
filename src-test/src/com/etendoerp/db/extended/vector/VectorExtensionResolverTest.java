package com.etendoerp.db.extended.vector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class VectorExtensionResolverTest {
  @Test void resolvesTheOnlyExtensionForANamespace() {
    VectorExtension extension = () -> "catalog.product";
    assertEquals(extension, new VectorExtensionResolver(Arrays.asList(extension)).resolve("catalog.product"));
  }
  @Test void returnsNullWhenNoExtensionClaimsTheNamespace() {
    assertNull(new VectorExtensionResolver(Arrays.<VectorExtension>asList()).resolve("catalog.product"));
  }
  @Test void rejectsDuplicateClaimsDeterministically() {
    VectorExtension first = () -> "catalog.product"; VectorExtension second = () -> "catalog.product";
    VectorException failure = assertThrows(VectorException.class, () -> new VectorExtensionResolver(Arrays.asList(first, second)).resolve("catalog.product"));
    assertEquals(VectorErrorCode.VECTOR_EXTENSION_CONFLICT, failure.getCode());
  }
}
