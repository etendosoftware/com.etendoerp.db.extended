/*
 *************************************************************************
 * The contents of this file are subject to the Etendo License
 * (the "License"), you may not use this file except in compliance with
 * the License.
 * You may obtain a copy of the License at
 * https://github.com/etendosoftware/etendo_core/blob/main/legal/Etendo_license.txt
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 * All portions are Copyright © 2026 FUTIT SERVICES, S.L
 * All Rights Reserved.
 * Contributor(s): Futit Services S.L.
 *************************************************************************
 */
package com.etendoerp.db.extended.vector;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openbravo.base.session.OBPropertiesProvider;

/** Optional OpenAI implementation using the embeddings endpoint. */
public final class OpenAiEmbeddingProvider implements VectorEmbeddingProvider {
  /** The base every OpenAI-compatible service is addressed by; the path is this class's business. */
  private static final Logger log = LogManager.getLogger();

  private static final String BASE_URL = "https://api.openai.com/v1";
  private static final String EMBEDDINGS_PATH = "/embeddings";
  /**
   * Openbravo.properties entry naming the endpoint every provider reaches by default.
   *
   * <p>For an installation that talks to one gateway: set it once instead of repeating the URL on
   * every provider, and keep an environment's address out of a dataset that ships with a module.
   * A provider that fills in its own API Endpoint still uses that one.</p>
   */
  private static final String ENDPOINT_PROPERTY = "vector.embeddings.endpoint";
  private final String apiKeyReference;
  private final String model;
  private final String endpoint;
  private final int dimensions;
  private final int timeoutSeconds;
  private final int maximumInputCharacters;
  private final int batchSize;

  /**
   * Creates a provider that calls an OpenAI-compatible embeddings API.
   *
   * @param endpoint
   *     base URL of an OpenAI-compatible API, up to and including {@code /v1} and no further, or
   *     {@code null} to call OpenAI itself. The path of the embeddings call is appended here and
   *     is never configured, which is the convention every OpenAI-compatible client follows and
   *     the only one that keeps a single base usable for more than one kind of call.
   *     <p>It makes the same configuration usable against the Etendo LLM proxy, an Azure OpenAI
   *     deployment, a corporate gateway or a local stub, which is also what allows the delivery
   *     path to be exercised without a real provider account.</p>
   */
  public OpenAiEmbeddingProvider(String apiKeyReference, String model, int dimensions,
      int timeoutSeconds, int maximumInputCharacters, String endpoint, int batchSize) {
    this.apiKeyReference = require(apiKeyReference, "apiKeyReference");
    this.model = require(model, "model"); this.dimensions = positive(dimensions, "dimensions");
    this.timeoutSeconds = positive(timeoutSeconds, "timeoutSeconds");
    this.maximumInputCharacters = positive(maximumInputCharacters, "maximumInputCharacters");
    this.endpoint = embeddingsUrl(endpoint);
    this.batchSize = positive(batchSize, "batchSize");
  }

  /**
   * Builds the embeddings URL from the configured base.
   *
   * <p>A trailing slash is dropped so a base written either way reaches the same place; nothing
   * else is interpreted. A base that already carried the path would become {@code
   * /embeddings/embeddings} and fail with a 404 naming the URL it called, which is a clearer
   * answer than silently accepting two spellings of one field.</p>
   */
  private static String embeddingsUrl(String base) {
    // The property is only worth reading when the provider left the field empty.
    return embeddingsUrl(base, isBlank(base) ? instanceEndpoint() : null);
  }

  /**
   * Chooses the base to call, most specific first.
   *
   * @param base
   *     the provider's own API Endpoint, if it has one
   * @param instanceDefault
   *     the endpoint configured for the whole installation, if there is one
   * @return the embeddings URL
   */
  static String embeddingsUrl(String base, String instanceDefault) {
    String chosen = base;
    if (isBlank(chosen)) {
      chosen = instanceDefault;
    }
    String trimmed = isBlank(chosen) ? BASE_URL : chosen.trim();
    while (trimmed.endsWith("/")) {
      trimmed = trimmed.substring(0, trimmed.length() - 1);
    }
    return trimmed + EMBEDDINGS_PATH;
  }

  private static boolean isBlank(String value) {
    return value == null || value.trim().isEmpty();
  }

  /** Read defensively: outside a running application there are no properties, and that is fine. */
  private static String instanceEndpoint() {
    try {
      return OBPropertiesProvider.getInstance().getOpenbravoProperties()
          .getProperty(ENDPOINT_PROPERTY);
    } catch (Exception e) {
      log.debug("No Openbravo.properties to read {} from; falling back to {}.", ENDPOINT_PROPERTY,
          BASE_URL, e);
      return null;
    }
  }

  @Override public int batchSize() { return batchSize; }

  @Override public int dimensions() { return dimensions; }

  @Override
  public List<double[]> embed(List<String> texts) {
    validateInputs(texts);
    String key = resolveKey();
    if (key == null || key.isEmpty()) {
      throw failed("The OpenAI API key is not configured. Set the system property, environment"
          + " variable or Openbravo.properties entry named in the provider's API Key Reference"
          + " field.", null);
    }
    try {
      return parseEmbeddings(post(key, requestBody(texts)), texts.size());
    } catch (VectorException e) {
      throw e;
    } catch (Exception e) {
      throw failed("Could not request an OpenAI embedding.", e);
    }
  }

  /**
   * Rejects a batch the provider would reject, before it costs a request.
   *
   * @param texts
   *     the texts about to be embedded
   */
  private void validateInputs(List<String> texts) {
    if (texts == null || texts.isEmpty()) {
      throw failed("Embedding input cannot be empty.", null);
    }
    if (texts.size() > batchSize) {
      throw failed("Embedding request exceeds the configured batch size of " + batchSize + ".",
          null);
    }
    for (String text : texts) {
      if (text == null || text.trim().isEmpty()) {
        throw failed("Embedding input cannot be empty.", null);
      }
    }
  }

  /**
   * Builds the request body, truncating any text past the configured maximum.
   *
   * @param texts
   *     the texts to embed
   * @return the body to post
   * @throws Exception
   *     if the body cannot be assembled
   */
  private JSONObject requestBody(List<String> texts) throws Exception {
    JSONArray inputs = new JSONArray();
    for (String text : texts) {
      inputs.put(text.length() > maximumInputCharacters
          ? text.substring(0, maximumInputCharacters)
          : text);
    }
    JSONObject request = new JSONObject();
    request.put("model", model);
    request.put("input", inputs);
    request.put("dimensions", dimensions);
    request.put("encoding_format", "float");
    return request;
  }

  /**
   * Posts the request to the configured endpoint.
   *
   * @param key
   *     the resolved API key
   * @param request
   *     the body to post
   * @return the response body
   * @throws Exception
   *     if the call cannot be made
   */
  private String post(String key, JSONObject request) throws Exception {
    HttpURLConnection connection = (HttpURLConnection) new URL(endpoint).openConnection();
    connection.setRequestMethod("POST");
    connection.setConnectTimeout(timeoutSeconds * 1000);
    connection.setReadTimeout(timeoutSeconds * 1000);
    connection.setDoOutput(true);
    connection.setRequestProperty("Authorization", "Bearer " + key);
    connection.setRequestProperty("Content-Type", "application/json");
    try (OutputStream output = connection.getOutputStream()) {
      output.write(request.toString().getBytes(StandardCharsets.UTF_8));
    }
    int status = connection.getResponseCode();
    boolean ok = status >= 200 && status < 300;
    String body = read(ok ? connection.getInputStream() : connection.getErrorStream());
    if (!ok) {
      throw failed("OpenAI embedding request failed with HTTP " + status + ".", null);
    }
    return body;
  }

  /**
   * Reads the embeddings out of a response, in the order the inputs were sent.
   *
   * @param body
   *     the response body
   * @param expected
   *     how many embeddings the request asked for
   * @return one embedding per input, in input order
   * @throws Exception
   *     if the response cannot be read
   */
  private List<double[]> parseEmbeddings(String body, int expected) throws Exception {
    JSONArray data = new JSONObject(body).getJSONArray("data");
    if (data.length() != expected) {
      throw failed("OpenAI returned " + data.length() + " embeddings for " + expected + " inputs.",
          null);
    }
    // The response carries an explicit index and is not guaranteed to preserve request order.
    List<double[]> embeddings = new ArrayList<>(Collections.nCopies(expected, null));
    for (int entry = 0; entry < data.length(); entry++) {
      JSONObject item = data.getJSONObject(entry);
      int index = item.has("index") ? item.getInt("index") : entry;
      if (index < 0 || index >= expected) {
        throw failed("OpenAI returned an embedding with an out of range index.", null);
      }
      embeddings.set(index, vector(item.getJSONArray("embedding")));
    }
    for (double[] embedding : embeddings) {
      if (embedding == null) {
        throw failed("OpenAI did not return an embedding for every input.", null);
      }
    }
    return embeddings;
  }

  /**
   * Converts one embedding of the response into a vector of the configured width.
   *
   * @param values
   *     the embedding as the provider returned it
   * @return the embedding
   * @throws Exception
   *     if a value cannot be read
   */
  private double[] vector(JSONArray values) throws Exception {
    if (values.length() != dimensions) {
      throw failed("OpenAI returned an unexpected embedding dimension.", null);
    }
    double[] embedding = new double[dimensions];
    for (int i = 0; i < dimensions; i++) {
      embedding[i] = values.getDouble(i);
    }
    return embedding;
  }

  private static String read(java.io.InputStream stream) throws Exception { if (stream == null) return ""; StringBuilder value = new StringBuilder(); try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) { String line; while ((line = reader.readLine()) != null) value.append(line); } return value.toString(); }
  private String resolveKey() {
    String key = System.getProperty(apiKeyReference);
    if (key == null || key.isEmpty()) key = System.getenv(apiKeyReference);
    if (key == null || key.isEmpty()) {
      key = OBPropertiesProvider.getInstance().getOpenbravoProperties().getProperty(apiKeyReference);
    }
    return key;
  }
  private static String require(String value, String name) { if (value == null || value.trim().isEmpty()) throw new IllegalArgumentException(name + " is required"); return value; }
  private static int positive(int value, String name) { if (value < 1) throw new IllegalArgumentException(name + " must be positive"); return value; }
  private static VectorException failed(String message, Throwable cause) { return cause == null ? new VectorException(VectorErrorCode.VECTOR_EMBEDDING_OPERATION_FAILED, message) : new VectorException(VectorErrorCode.VECTOR_EMBEDDING_OPERATION_FAILED, message, cause); }
}
