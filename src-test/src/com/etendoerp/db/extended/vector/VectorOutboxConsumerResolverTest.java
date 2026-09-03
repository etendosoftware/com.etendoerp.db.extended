package com.etendoerp.db.extended.vector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.Collections;

import org.junit.jupiter.api.Test;

class VectorOutboxConsumerResolverTest {

  @Test
  void resolvesTheOnlyConsumerForANamespace() {
    VectorOutboxConsumer consumer = new TestConsumer("catalog.product");

    assertEquals(consumer, new VectorOutboxConsumerResolver(Arrays.asList(consumer))
        .resolve("catalog.product"));
  }

  @Test
  void returnsNullWhenNoConsumerOwnsTheNamespace() {
    assertNull(new VectorOutboxConsumerResolver(Collections.<VectorOutboxConsumer>emptyList())
        .resolve("catalog.product"));
  }

  @Test
  void rejectsMultipleConsumersForTheSameNamespace() {
    VectorOutboxConsumer first = new TestConsumer("catalog.product");
    VectorOutboxConsumer second = new TestConsumer("catalog.product");

    VectorException failure = assertThrows(VectorException.class,
        () -> new VectorOutboxConsumerResolver(Arrays.asList(first, second))
            .resolve("catalog.product"));

    assertEquals(VectorErrorCode.VECTOR_EXTENSION_CONFLICT, failure.getCode());
  }

  private static class TestConsumer implements VectorOutboxConsumer {
    private final String namespace;

    private TestConsumer(String namespace) {
      this.namespace = namespace;
    }

    @Override
    public String namespace() {
      return namespace;
    }

    @Override
    public void consume(VectorOutboxEvent event) {
      // The resolver test does not execute the consumer.
    }
  }
}
