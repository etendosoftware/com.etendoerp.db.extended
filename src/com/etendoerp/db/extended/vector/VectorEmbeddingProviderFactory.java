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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.openbravo.database.ConnectionProvider;

/**
 * Resolves the configured provider for a source without exposing its secret.
 *
 * <p>Not final so a test can stand in for the provider: every provider this builds reaches an
 * external service on {@code embed}, which is not something a unit test can be asked to do.</p>
 */
class VectorEmbeddingProviderFactory {
  private final ConnectionProvider connectionProvider;
  VectorEmbeddingProviderFactory(ConnectionProvider connectionProvider) { this.connectionProvider = connectionProvider; }
  VectorEmbeddingProvider forSource(String sourceId) {
    String sql = "SELECT p.provider_type, p.api_key_reference, p.model, p.dimensions, p.timeout_seconds, p.max_input_characters, p.api_endpoint, p.batch_size "
        + "FROM etarc_vector_source s JOIN etarc_vector_embed_provider p ON p.etarc_vector_embed_provider_id = s.etarc_vector_embed_provider_id "
        + "WHERE s.etarc_vector_source_id = ? AND p.isactive = 'Y'";
    try (PreparedStatement statement = connectionProvider.getPreparedStatement(sql)) {
      statement.setString(1, sourceId); try (ResultSet result = statement.executeQuery()) {
        if (!result.next()) throw new VectorException(VectorErrorCode.VECTOR_EMBEDDING_OPERATION_FAILED, "No active embedding provider is configured for this source.");
        if (!"OPENAI".equals(result.getString(1))) throw new VectorException(VectorErrorCode.VECTOR_EMBEDDING_OPERATION_FAILED, "Unsupported embedding provider type.");
        return new OpenAiEmbeddingProvider(result.getString(2), result.getString(3), result.getInt(4), result.getInt(5), result.getInt(6), result.getString(7), result.getInt(8));
      }
    } catch (VectorException e) { throw e; } catch (Exception e) { throw new VectorException(VectorErrorCode.VECTOR_EMBEDDING_OPERATION_FAILED, "Could not resolve the embedding provider.", e); }
  }
}
