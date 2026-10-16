package com.etendoerp.db.extended.vector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import com.sun.net.httpserver.HttpServer;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.openbravo.base.session.OBPropertiesProvider;

/**
 * Exercises the embeddings request against a real HTTP server instead of a mock.
 *
 * <p>What matters here is the shape of the conversation with the provider: how many requests leave
 * for a given number of inputs, and how the answers are matched back. Both used to be impossible to
 * assert without a provider account, which is why the delivery path had no coverage at all.</p>
 *
 * @covers com.etendoerp.db.extended.vector.OpenAiEmbeddingProvider
 */
class OpenAiEmbeddingProviderTest {

  private static final String KEY_REFERENCE = "ETP5118_TEST_KEY";
  private static final int DIMENSIONS = 3;

  private HttpServer server;
  private String endpoint;
  private final List<JSONObject> received = new ArrayList<>();
  private final AtomicReference<String> response = new AtomicReference<>();
  private final AtomicReference<Integer> status = new AtomicReference<>(200);
  private final AtomicReference<String> retryAfter = new AtomicReference<>();
  private final AtomicReference<Long> delayMillis = new AtomicReference<>(0L);

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
      sleep(delayMillis.get());
      byte[] body = response.get().getBytes(StandardCharsets.UTF_8);
      if (retryAfter.get() != null) {
        exchange.getResponseHeaders().add("Retry-After", retryAfter.get());
      }
      exchange.sendResponseHeaders(status.get(), body.length);
      try (OutputStream out = exchange.getResponseBody()) {
        out.write(body);
      }
    });
    server.start();
    // What an administrator configures is the base, exactly as every OpenAI-compatible client
    // expects it; the path below is the provider's business and never appears in the window.
    endpoint = "http://localhost:" + server.getAddress().getPort() + "/v1";
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

  @Test
  void appendsTheEmbeddingsPathToTheConfiguredBase() throws Exception {
    response.set(embeddings(0));

    provider(5).embed(List.of("a"));

    assertEquals(1, received.size(),
        "the request has to reach /v1/embeddings even though only /v1 was configured");
  }

  @Test
  void acceptsABaseWrittenWithOrWithoutATrailingSlash() throws Exception {
    response.set(embeddings(0));

    new OpenAiEmbeddingProvider(KEY_REFERENCE, "test-model", DIMENSIONS, 5, 1000, endpoint + "/", 5)
        .embed(List.of("a"));

    assertEquals(1, received.size(), "a trailing slash is a spelling, not a different endpoint");
  }

  @Test
  void sendsTheModelExactlyAsConfiguredSoAProxyCanBeAddressedByPrefix() throws Exception {
    response.set(embeddings(0));

    new OpenAiEmbeddingProvider(KEY_REFERENCE, "openai/text-embedding-3-small", DIMENSIONS, 5, 1000,
        endpoint, 5).embed(List.of("a"));

    assertEquals("openai/text-embedding-3-small", received.get(0).getString("model"),
        "a provider-agnostic gateway is told which provider to use in the model name, and the "
            + "name has to arrive the way it was configured");
  }

  @Test
  void prefersTheProvidersOwnEndpointOverTheOneConfiguredForTheInstance() {
    assertEquals("https://own.example/v1/embeddings",
        OpenAiEmbeddingProvider.embeddingsUrl("https://own.example/v1", "https://shared.example/v1"),
        "a provider that names its own endpoint is addressing something the instance default is "
            + "not, so the field wins");
  }

  @Test
  void fallsBackToTheEndpointConfiguredForTheInstanceWhenTheProviderNamesNone() {
    assertEquals("https://shared.example/v1/embeddings",
        OpenAiEmbeddingProvider.embeddingsUrl("  ", "https://shared.example/v1"),
        "an installation behind one gateway configures the address once, and a provider left "
            + "blank uses it");
  }

  @Test
  void fallsBackToOpenAiWhenNeitherTheProviderNorTheInstanceNamesAnEndpoint() {
    assertEquals("https://api.openai.com/v1/embeddings",
        OpenAiEmbeddingProvider.embeddingsUrl(null, null),
        "with nothing configured anywhere the provider still has to reach OpenAI");
  }

  @Test
  void normalisesATrailingSlashOnTheInstanceEndpointToo() {
    assertEquals("https://shared.example/v1/embeddings",
        OpenAiEmbeddingProvider.embeddingsUrl(null, "https://shared.example/v1/"),
        "the property is written by hand, so it gets the same forgiveness as the field");
  }

  @Test
  void readsTheInstanceEndpointFromOpenbravoProperties() throws Exception {
    response.set(embeddings(0));
    Properties properties = new Properties();
    properties.setProperty("vector.embeddings.endpoint", endpoint);
    OBPropertiesProvider provider = Mockito.mock(OBPropertiesProvider.class);
    Mockito.when(provider.getOpenbravoProperties()).thenReturn(properties);

    try (MockedStatic<OBPropertiesProvider> statics = Mockito.mockStatic(OBPropertiesProvider.class)) {
      statics.when(OBPropertiesProvider::getInstance).thenReturn(provider);
      new OpenAiEmbeddingProvider(KEY_REFERENCE, "test-model", DIMENSIONS, 5, 1000, null, 5)
          .embed(List.of("a"));
    }

    assertEquals(1, received.size(),
        "a provider left without an API Endpoint has to end up at the address Openbravo.properties "
            + "names, or the default is only a default on paper");
  }

  @Test
  void carriesTheStatusAndRetryAfterOfAThrottledRequest() {
    response.set("{\"error\":{\"message\":\"Rate limit reached\"}}");
    status.set(429);
    retryAfter.set("30");

    VectorProviderException failure = assertThrows(VectorProviderException.class,
        () -> provider(25).embed(List.of("a")));

    assertEquals(429, failure.getHttpStatus(),
        "the status has to travel as data: whether to retry must not depend on parsing a message");
    assertEquals(Duration.ofSeconds(30), failure.getRetryAfter());
    assertTrue(failure.getMessage().contains("HTTP 429"));
  }

  @Test
  void readsRetryAfterWrittenAsAnHttpDate() {
    response.set("{}");
    status.set(429);
    retryAfter.set(DateTimeFormatter.RFC_1123_DATE_TIME.format(
        ZonedDateTime.now(ZoneOffset.UTC).plusSeconds(120)));

    VectorProviderException failure = assertThrows(VectorProviderException.class,
        () -> provider(25).embed(List.of("a")));

    long seconds = failure.getRetryAfter().getSeconds();
    assertTrue(seconds > 100 && seconds <= 120, "Retry-After may also be a date; got " + seconds + "s");
  }

  @Test
  void carriesTheStatusOfAnAuthenticationFailureWithoutRetryAfter() {
    response.set("{}");
    status.set(401);

    VectorProviderException failure = assertThrows(VectorProviderException.class,
        () -> provider(25).embed(List.of("a")));

    assertEquals(401, failure.getHttpStatus());
    assertNull(failure.getRetryAfter());
  }

  @Test
  void reportsATimeoutWithTheIoFailureAsItsCause() {
    response.set(embeddings(0));
    delayMillis.set(2500L);

    VectorException failure = assertThrows(VectorException.class,
        () -> new OpenAiEmbeddingProvider(KEY_REFERENCE, "test-model", DIMENSIONS, 1, 1000, endpoint, 5)
            .embed(List.of("a")));

    assertTrue(failure.getCause() instanceof SocketTimeoutException,
        "a timeout has no status, so the delivery policy recognises it by its cause; got "
            + failure.getCause());
  }

  // ---------------------------------------------------------------- Retry-After parsing

  @Test
  void readsRetryAfterInSeconds() {
    assertEquals(Duration.ofSeconds(45), OpenAiEmbeddingProvider.retryAfter(" 45 "));
  }

  @Test
  void ignoresANegativeRetryAfter() {
    assertNull(OpenAiEmbeddingProvider.retryAfter("-5"),
        "a negative wait is not a wait; the header is treated as absent so the backoff decides");
  }

  @Test
  void ignoresAnUnreadableRetryAfter() {
    assertNull(OpenAiEmbeddingProvider.retryAfter("soon"));
  }

  @Test
  void ignoresARetryAfterTooLargeToBeANumberOfSeconds() {
    assertNull(OpenAiEmbeddingProvider.retryAfter("99999999999999999999999"),
        "a value that overflows a long is neither seconds nor a date, so it is ignored");
  }

  @Test
  void ignoresABlankOrMissingRetryAfter() {
    assertNull(OpenAiEmbeddingProvider.retryAfter(null));
    assertNull(OpenAiEmbeddingProvider.retryAfter("   "));
  }

  @Test
  void readsAnHttpDateInThePastAsNoWaitAtAll() {
    String past = DateTimeFormatter.RFC_1123_DATE_TIME.format(
        ZonedDateTime.now(ZoneOffset.UTC).minusHours(1));

    assertEquals(Duration.ZERO, OpenAiEmbeddingProvider.retryAfter(past),
        "a date already gone means the provider accepts again now, never a negative delay");
  }

  @Test
  void readsAnHttpDateInTheFutureAsTheTimeLeftUntilIt() {
    String future = DateTimeFormatter.RFC_1123_DATE_TIME.format(
        ZonedDateTime.now(ZoneOffset.UTC).plusSeconds(600));

    long seconds = OpenAiEmbeddingProvider.retryAfter(future).getSeconds();

    assertTrue(seconds > 590 && seconds <= 600, "expected about 600s, got " + seconds + "s");
  }

  private static void sleep(long millis) {
    if (millis <= 0) {
      return;
    }
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
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
