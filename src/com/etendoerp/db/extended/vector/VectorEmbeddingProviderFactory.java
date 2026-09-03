package com.etendoerp.db.extended.vector;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.openbravo.database.ConnectionProvider;

/** Resolves the configured provider for a source without exposing its secret. */
final class VectorEmbeddingProviderFactory {
  private final ConnectionProvider connectionProvider;
  VectorEmbeddingProviderFactory(ConnectionProvider connectionProvider) { this.connectionProvider = connectionProvider; }
  VectorEmbeddingProvider forSource(String sourceId) {
    String sql = "SELECT p.provider_type, p.api_key_reference, p.model, p.dimensions, p.timeout_seconds, p.max_input_characters "
        + "FROM etarc_vector_source s JOIN etarc_vector_embed_provider p ON p.etarc_vector_embed_provider_id = s.etarc_vector_embed_provider_id "
        + "WHERE s.etarc_vector_source_id = ? AND p.isactive = 'Y'";
    try (PreparedStatement statement = connectionProvider.getPreparedStatement(sql)) {
      statement.setString(1, sourceId); try (ResultSet result = statement.executeQuery()) {
        if (!result.next()) throw new VectorException(VectorErrorCode.VECTOR_EMBEDDING_OPERATION_FAILED, "No active embedding provider is configured for this source.");
        if (!"OPENAI".equals(result.getString(1))) throw new VectorException(VectorErrorCode.VECTOR_EMBEDDING_OPERATION_FAILED, "Unsupported embedding provider type.");
        return new OpenAiEmbeddingProvider(result.getString(2), result.getString(3), result.getInt(4), result.getInt(5), result.getInt(6));
      }
    } catch (VectorException e) { throw e; } catch (Exception e) { throw new VectorException(VectorErrorCode.VECTOR_EMBEDDING_OPERATION_FAILED, "Could not resolve the embedding provider.", e); }
  }
}
