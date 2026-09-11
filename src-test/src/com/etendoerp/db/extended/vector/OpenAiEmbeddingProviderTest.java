package com.etendoerp.db.extended.vector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.sun.net.httpserver.HttpServer;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Exercises the embeddings request against a real HTTP server instead of a mock.
 *
 * <p>What matters here is the shape of the conversation with the provider: how many requests leave
 * for a given number of inputs, and how the answers are matched back. Both used to be impossible to
 * assert without a provider account, which is why the delivery path had no coverage at all.</p>
 */
class OpenAiEmbeddingProviderTest {

  private static final String KEY_REFERENCE = "ETP5118_TEST_KEY";
  private static final int DIMENSIONS = 3;

  private HttpServer server;
  private String endpoint;
  private final List<JSONObject> received = new ArrayList<>();
  private final AtomicReference<String> response = new AtomicReference<>();

  @BeforeEach
  void startServer() throws Exception {
    System.setProperty(KEY_REFERENCE, "test-key");
    received.clear();
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext("/v1/embeddings", exchange -> {
      try (InputStream in = exchange.getRequestBody()) {
        received.add(new JSONObject(new String(in.readAllBytes(), StandardCharsets.UTF_8)));
      } catch (Exception e) {
        throw new java.io.IOException(e);
      }
      byte[] body = response.get().getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(200, body.length);
      try (OutputStream out = exchange.getResponseBody()) {
        out.write(body);
      }
    });
    server.start();
    endpoint = "http://localhost:" + server.getAddress().getPort() + "/v1/embeddings";
  }

  @AfterEach
  void stopServer() {
    server.stop(0);
    System.clearProperty(KEY_REFERENCE);
  }

  @Test
  void sendsEveryInputInASingleRequest() throws Exception {
    response.set(embeddings(0, 1, 2));

    List<double[]> vectors = provider(25).embed(List.of("uno", "dos", "tres"));

    assertEquals(1, received.size(), "three inputs must travel in one request, not one request each");
    JSONArray input = received.get(0).getJSONArray("input");
    assertEquals(3, input.length());
    assertEquals("uno", input.getString(0));
    assertEquals(3, vectors.size());
  }

  @Test
  void mapsEmbeddingsByTheirIndexNotByArrivalOrder() throws Exception {
    // The provider is allowed to answer out of order, so position in the array proves nothing.
    response.set(embeddings(2, 0, 1));

    List<double[]> vectors = provider(25).embed(List.of("uno", "dos", "tres"));

    assertArrayEquals(new double[] { 0, 0, 0 }, vectors.get(0));
    assertArrayEquals(new double[] { 1, 1, 1 }, vectors.get(1));
    assertArrayEquals(new double[] { 2, 2, 2 }, vectors.get(2));
  }

  @Test
  void refusesMoreInputsThanTheConfiguredBatchSize() {
    response.set(embeddings(0, 1, 2));

    VectorException failure = assertThrows(VectorException.class,
        () -> provider(2).embed(List.of("uno", "dos", "tres")));

    assertTrue(failure.getMessage().contains("batch size"));
    assertEquals(0, received.size(), "the request must not leave when it exceeds the batch size");
  }

  @Test
  void failsWhenTheProviderAnswersFewerEmbeddingsThanInputs() {
    response.set(embeddings(0, 1));

    assertThrows(VectorException.class, () -> provider(25).embed(List.of("uno", "dos", "tres")));
  }

  @Test
  void truncatesEachInputToTheConfiguredMaximum() throws Exception {
    response.set(embeddings(0));

    new OpenAiEmbeddingProvider(KEY_REFERENCE, "test-model", DIMENSIONS, 5, 4, endpoint, 25)
        .embed(List.of("abcdefghij"));

    assertEquals("abcd", received.get(0).getJSONArray("input").getString(0));
  }

  private OpenAiEmbeddingProvider provider(int batchSize) {
    return new OpenAiEmbeddingProvider(KEY_REFERENCE, "test-model", DIMENSIONS, 5, 1000, endpoint, batchSize);
  }

  /** Builds a response whose entries carry the supplied indexes, in the supplied order. */
  private static String embeddings(int... indexes) {
    try {
      JSONArray data = new JSONArray();
      for (int index : indexes) {
        JSONArray embedding = new JSONArray();
        for (int i = 0; i < DIMENSIONS; i++) {
          embedding.put(index);
        }
        data.put(new JSONObject().put("index", index).put("embedding", embedding));
      }
      return new JSONObject().put("data", data).toString();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
}
