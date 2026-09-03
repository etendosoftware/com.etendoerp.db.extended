package com.etendoerp.db.extended.vector;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.openbravo.database.ConnectionProvider;

/** Reads only active source, collection, and provider metadata required for semantic search. */
final class VectorSearchSourceResolver {
  private final ConnectionProvider connectionProvider;

  VectorSearchSourceResolver(ConnectionProvider connectionProvider) {
    this.connectionProvider = connectionProvider;
  }

  VectorSearchSource resolve(String namespace) {
    String sql = "SELECT s.etarc_vector_source_id, s.namespace, c.metric, p.provider_type, p.model, p.dimensions "
        + "FROM etarc_vector_source s "
        + "JOIN etarc_vector_collection c ON c.namespace = s.namespace AND c.active = true "
        + "JOIN etarc_vector_embed_provider p ON p.etarc_vector_embed_provider_id = s.etarc_vector_embed_provider_id "
        + "WHERE s.namespace = ? AND s.isactive = 'Y' AND p.isactive = 'Y'";
    try (PreparedStatement statement = connectionProvider.getPreparedStatement(sql)) {
      statement.setString(1, namespace);
      try (ResultSet result = statement.executeQuery()) {
        if (!result.next()) {
          throw new VectorException(VectorErrorCode.VECTOR_COLLECTION_NOT_FOUND,
              "No active vector source, collection, and provider were found for the namespace.");
        }
        return new VectorSearchSource(result.getString(1), result.getString(2),
            DistanceMetric.valueOf(result.getString(3)), result.getString(4), result.getString(5),
            result.getInt(6));
      }
    } catch (VectorException e) {
      throw e;
    } catch (Exception e) {
      throw new VectorException(VectorErrorCode.VECTOR_SEARCH_OPERATION_FAILED,
          "Could not resolve the vector search source.", e);
    }
  }
}
