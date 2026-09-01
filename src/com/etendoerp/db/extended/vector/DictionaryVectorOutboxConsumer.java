package com.etendoerp.db.extended.vector;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import org.codehaus.jettison.json.JSONObject;
import org.openbravo.database.ConnectionProvider;

/** Generic consumer that reads configured dictionary columns, with no business-entity dependency. */
public final class DictionaryVectorOutboxConsumer implements VectorOutboxConsumer {
  private final ConnectionProvider cp; private final VectorStore store; private final VectorEmbeddingProviderFactory providers;
  public DictionaryVectorOutboxConsumer(ConnectionProvider cp, VectorStore store) { this.cp = cp; this.store = store; providers = new VectorEmbeddingProviderFactory(cp); }
  @Override public String namespace() { return "*"; }
  @Override public boolean supports(String namespace) { return true; }
  @Override public void consume(VectorOutboxEvent event) throws Exception {
    if ("DELETE".equals(event.getEventType())) { store.delete(event.getNamespace(), event.getRecordId()); return; }
    Source source = source(event.getSourceId()); if (source.version != event.getConfigVersion()) return;
    List<String> columns = contentColumns(event.getSourceId()); if (columns.isEmpty()) throw new VectorException(VectorErrorCode.VECTOR_INVALID_METADATA, "The vector source has no content columns.");
    String sql = "SELECT " + quoted(String.join(",", columns)) + " FROM " + quote(source.table) + " WHERE " + quote(source.key) + " = ?" + source.filterClause();
    try (PreparedStatement statement = cp.getPreparedStatement(sql)) { statement.setString(1, event.getRecordId()); if (source.filterColumn != null) statement.setString(2, source.filterValue); try (ResultSet result = statement.executeQuery()) {
      if (!result.next()) { store.delete(event.getNamespace(), event.getRecordId()); return; } JSONObject fields = new JSONObject(); StringBuilder text = new StringBuilder();
      for (String column : columns) { String value = result.getString(column); if (value != null) { fields.put(column, value); text.append(column).append(": ").append(value).append('\n'); } }
      JSONObject metadata = new JSONObject(); metadata.put("sourceId", event.getSourceId()); metadata.put("configVersion", event.getConfigVersion()); metadata.put("fields", fields);
      VectorEmbeddingProvider provider = providers.forSource(event.getSourceId());
      store.upsert(new VectorRecord(event.getNamespace(), event.getRecordId(), provider.embed(text.toString()), metadata.toString(), event.getClientId(), event.getOrganizationId()));
    } }
  }
  private Source source(String id) throws Exception { try (PreparedStatement s = cp.getPreparedStatement("SELECT s.config_version, t.tablename, k.columnname, f.columnname, s.filter_value FROM etarc_vector_source s JOIN ad_table t ON t.ad_table_id=s.ad_table_id JOIN ad_column k ON k.ad_table_id=t.ad_table_id AND k.iskey='Y' AND k.isactive='Y' LEFT JOIN ad_column f ON f.ad_column_id=s.ad_filter_column_id AND f.ad_table_id=t.ad_table_id AND f.isactive='Y' WHERE s.etarc_vector_source_id=?")) { s.setString(1,id); try(ResultSet r=s.executeQuery()){if(!r.next()) throw new VectorException(VectorErrorCode.VECTOR_OUTBOX_OPERATION_FAILED,"Vector source was not found."); return new Source(r.getLong(1),r.getString(2),r.getString(3),r.getString(4),r.getString(5));} } }
  private List<String> contentColumns(String id) throws Exception { List<String> result=new ArrayList<>(); try(PreparedStatement s=cp.getPreparedStatement("SELECT c.columnname FROM etarc_vector_source_column sc JOIN ad_column c ON c.ad_column_id=sc.ad_column_id WHERE sc.etarc_vector_source_id=? AND sc.isactive='Y' AND sc.iscontent='Y' ORDER BY sc.seqno")){s.setString(1,id);try(ResultSet r=s.executeQuery()){while(r.next())result.add(r.getString(1));}}return result; }
  private static String quoted(String values) { StringBuilder result=new StringBuilder(); for(String value:values.split(",")){if(result.length()>0)result.append(',');result.append(quote(value));}return result.toString(); }
  private static String quote(String value) { return "\""+value.toLowerCase().replace("\"","\"\"")+"\""; }
  private static final class Source { final long version; final String table,key,filterColumn,filterValue; Source(long version,String table,String key,String filterColumn,String filterValue){this.version=version;this.table=table;this.key=key;this.filterColumn=filterColumn;this.filterValue=filterValue;} String filterClause(){return filterColumn == null ? "" : " AND " + quote(filterColumn) + " = ?";} }
}
