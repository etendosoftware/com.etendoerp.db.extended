package com.etendoerp.db.extended.vector;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.openbravo.base.session.OBPropertiesProvider;

/** Optional OpenAI implementation using the embeddings endpoint. */
public final class OpenAiEmbeddingProvider implements VectorEmbeddingProvider {
  private static final String ENDPOINT = "https://api.openai.com/v1/embeddings";
  private final String apiKeyReference, model;
  private final int dimensions, timeoutSeconds, maximumInputCharacters;

  public OpenAiEmbeddingProvider(String apiKeyReference, String model, int dimensions,
      int timeoutSeconds, int maximumInputCharacters) {
    this.apiKeyReference = require(apiKeyReference, "apiKeyReference");
    this.model = require(model, "model"); this.dimensions = positive(dimensions, "dimensions");
    this.timeoutSeconds = positive(timeoutSeconds, "timeoutSeconds");
    this.maximumInputCharacters = positive(maximumInputCharacters, "maximumInputCharacters");
  }

  @Override public int dimensions() { return dimensions; }

  @Override public double[] embed(String text) {
    if (text == null || text.trim().isEmpty()) throw failed("Embedding input cannot be empty.", null);
    String key = resolveKey();
    if (key == null || key.isEmpty()) throw failed("OpenAI API key reference is not configured: " + apiKeyReference, null);
    String input = text.length() > maximumInputCharacters ? text.substring(0, maximumInputCharacters) : text;
    try {
      JSONObject request = new JSONObject(); request.put("model", model); request.put("input", input);
      request.put("dimensions", dimensions); request.put("encoding_format", "float");
      HttpURLConnection connection = (HttpURLConnection) new URL(ENDPOINT).openConnection();
      connection.setRequestMethod("POST"); connection.setConnectTimeout(timeoutSeconds * 1000);
      connection.setReadTimeout(timeoutSeconds * 1000); connection.setDoOutput(true);
      connection.setRequestProperty("Authorization", "Bearer " + key);
      connection.setRequestProperty("Content-Type", "application/json");
      try (OutputStream output = connection.getOutputStream()) { output.write(request.toString().getBytes(StandardCharsets.UTF_8)); }
      int status = connection.getResponseCode();
      String body = read(status >= 200 && status < 300 ? connection.getInputStream() : connection.getErrorStream());
      if (status < 200 || status >= 300) throw failed("OpenAI embedding request failed with HTTP " + status + ".", null);
      JSONArray values = new JSONObject(body).getJSONArray("data").getJSONObject(0).getJSONArray("embedding");
      if (values.length() != dimensions) throw failed("OpenAI returned an unexpected embedding dimension.", null);
      double[] embedding = new double[dimensions]; for (int i = 0; i < dimensions; i++) embedding[i] = values.getDouble(i);
      return embedding;
    } catch (VectorException e) { throw e; } catch (Exception e) { throw failed("Could not request an OpenAI embedding.", e); }
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
