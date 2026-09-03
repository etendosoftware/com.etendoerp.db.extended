package com.etendoerp.db.extended.vector;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.openbravo.database.ConnectionProvider;

/** Resolves configured target keys to a physical source and a compiled Display Logic predicate. */
final class VectorSearchTargetResolver {
  private final ConnectionProvider connectionProvider;
  private final VectorSearchSourceResolver sources;

  VectorSearchTargetResolver(ConnectionProvider connectionProvider) {
    this.connectionProvider = connectionProvider;
    this.sources = new VectorSearchSourceResolver(connectionProvider);
  }

  VectorSearchTarget resolve(String searchKey) {
    String sql = "SELECT t.etarc_vector_source_id, s.namespace, t.filter_display_logic "
        + "FROM etarc_vector_search_target t JOIN etarc_vector_source s "
        + "ON s.etarc_vector_source_id=t.etarc_vector_source_id "
        + "WHERE t.search_key=? AND t.isactive='Y' AND s.isactive='Y'";
    try (PreparedStatement statement = connectionProvider.getPreparedStatement(sql)) {
      statement.setString(1, searchKey);
      try (ResultSet result = statement.executeQuery()) {
        if (!result.next()) throw new VectorException(VectorErrorCode.VECTOR_COLLECTION_NOT_FOUND,
            "No active vector search target and source were found for the target key.");
        String sourceId = result.getString(1);
        VectorSearchSource source = sources.resolve(result.getString(2));
        return new VectorSearchTarget(searchKey, source,
            VectorDisplayLogicCompiler.compile(result.getString(3), metadataFields(sourceId)));
      }
    } catch (VectorException e) {
      throw e;
    } catch (Exception e) {
      throw new VectorException(VectorErrorCode.VECTOR_SEARCH_OPERATION_FAILED,
          "Could not resolve the vector search target.", e);
    }
  }

  private Map<String, String> metadataFields(String sourceId) throws Exception {
    Map<String, String> fields = new HashMap<>();
    String sql = "SELECT c.columnname FROM etarc_vector_source_column sc "
        + "JOIN ad_column c ON c.ad_column_id=sc.ad_column_id "
        + "WHERE sc.etarc_vector_source_id=? AND sc.isactive='Y'";
    try (PreparedStatement statement = connectionProvider.getPreparedStatement(sql)) {
      statement.setString(1, sourceId);
      try (ResultSet result = statement.executeQuery()) {
        while (result.next()) {
          String columnName = result.getString(1);
          fields.put(columnName.toLowerCase(Locale.ROOT), columnName);
        }
      }
    }
    return fields;
  }
}
