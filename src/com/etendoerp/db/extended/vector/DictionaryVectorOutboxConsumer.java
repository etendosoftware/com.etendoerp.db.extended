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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.codehaus.jettison.json.JSONObject;
import org.openbravo.database.ConnectionProvider;

/** Generic consumer that reads configured dictionary columns, with no business-entity dependency. */
public final class DictionaryVectorOutboxConsumer implements VectorOutboxConsumer {
  private final ConnectionProvider cp; private final VectorStore store; private final VectorEmbeddingProviderFactory providers;
  /**
   * Per-source configuration resolved once per consumer instance.
   *
   * <p>Both the column mapping and the embedding provider are the same for every event of a given
   * source, yet they were queried on each one: a batch of one hundred events issued two hundred
   * identical queries. A consumer lives for a single run of the scheduled process, so caching here
   * is bounded by that run.</p>
   *
   * <p>The source record itself is deliberately not cached: it carries the configuration version
   * that gates stale events, and reading it fresh keeps that fence exact.</p>
   */
  private final Map<String, List<SourceColumn>> columnsBySource = new HashMap<>();
  private final Map<String, VectorEmbeddingProvider> providerBySource = new HashMap<>();
  /** Embeddings resolved by {@link #prepare(List)}, drained as each event is consumed. */
  private final Map<String, double[]> embeddingByEvent = new HashMap<>();
  /**
   * Builds a consumer that resolves its providers from the dictionary.
   *
   * @param cp
   *     connection to read the source configuration and the indexed rows with
   * @param store
   *     where the resulting vectors are written
   */
  public DictionaryVectorOutboxConsumer(ConnectionProvider cp, VectorStore store) {
    this(cp, store, new VectorEmbeddingProviderFactory(cp));
  }

  /** Takes the provider factory so a test can deliver without reaching an external service. */
  DictionaryVectorOutboxConsumer(ConnectionProvider cp, VectorStore store,
      VectorEmbeddingProviderFactory providers) {
    this.cp = cp;
    this.store = store;
    this.providers = providers;
  }
  @Override public String namespace() { return "*"; }
  @Override public boolean supports(String namespace) { return true; }
  @Override public int batchSize(VectorOutboxEvent event) {
    return cachedProvider(event.getSourceId()).batchSize();
  }

  /**
   * Embeds the whole chunk in a single provider request.
   *
   * <p>The embedding call is the expensive part of delivery: it leaves the tenant, it is billed and
   * it dominates the latency. Resolving it per event meant one HTTP round trip per row while the
   * provider accepts many inputs at once. Everything else stays per event, so a row that cannot be
   * read or upserted still fails on its own.</p>
   */
  @Override public void prepare(List<VectorOutboxEvent> events) {
    embeddingByEvent.clear();
    List<VectorOutboxEvent> embeddable = new ArrayList<>();
    List<String> texts = new ArrayList<>();
    try {
      for (VectorOutboxEvent event : events) {
        Payload payload = payload(event);
        if (payload == null) {
          continue;
        }
        embeddable.add(event);
        texts.add(payload.text);
      }
    } catch (VectorException e) {
      throw e;
    } catch (Exception e) {
      throw new VectorException(VectorErrorCode.VECTOR_OUTBOX_OPERATION_FAILED,
          "Could not read the records of a delivery chunk.", e);
    }
    if (texts.isEmpty()) {
      return;
    }
    List<double[]> vectors = cachedProvider(embeddable.get(0).getSourceId()).embed(texts);
    for (int i = 0; i < embeddable.size(); i++) {
      embeddingByEvent.put(embeddable.get(i).getId(), vectors.get(i));
    }
  }

  @Override public void consume(VectorOutboxEvent event) throws Exception {
    if ("DELETE".equals(event.getEventType())) { store.delete(event.getNamespace(), event.getRecordId()); return; }
    Payload payload = payload(event);
    if (payload == null) {
      return;
    }
    double[] embedding = embeddingByEvent.remove(event.getId());
    if (embedding == null) {
      // Not prepared, either because the dispatcher delivers one by one or because the row changed
      // between prepare and consume. Resolving it here keeps the event deliverable either way.
      embedding = cachedProvider(event.getSourceId()).embed(payload.text);
    }
    store.upsert(new VectorRecord(event.getNamespace(), event.getRecordId(), embedding,
        payload.metadata, event.getClientId(), event.getOrganizationId()));
  }

  /**
   * Reads the source row and builds the text to embed plus the metadata to store.
   *
   * @return {@code null} when the event needs no embedding, which happens when its configuration
   *     version is stale or when the record no longer exists, in which case the vector is deleted
   */
  private Payload payload(VectorOutboxEvent event) throws Exception {
    // The configuration version fence lives in the dispatcher: it is delivery policy, not content.
    Source source = source(event.getSourceId());
    List<SourceColumn> columns = cachedColumns(event.getSourceId());
    if (columns.stream().noneMatch(SourceColumn::isContent)) throw new VectorException(VectorErrorCode.VECTOR_INVALID_METADATA, "The vector source has no content columns.");
    String sql = "SELECT " + quoted(columnNames(columns)) + " FROM " + quote(source.table) + " WHERE " + quote(source.key) + " = ?";
    try (PreparedStatement statement = cp.getPreparedStatement(sql)) { statement.setString(1, event.getRecordId()); try (ResultSet result = statement.executeQuery()) {
      if (!result.next()) { store.delete(event.getNamespace(), event.getRecordId()); return null; }
      JSONObject fields = new JSONObject();
      StringBuilder text = new StringBuilder();
      for (SourceColumn column : columns) { String value = result.getString(column.name); if (value != null) { fields.put(column.name, value); if (column.isContent()) text.append(column.name).append(": ").append(value).append('\n'); } }
      JSONObject metadata = new JSONObject(); metadata.put("sourceId", event.getSourceId()); metadata.put("configVersion", event.getConfigVersion()); metadata.put("fields", fields);
      return new Payload(text.toString(), metadata.toString());
    } }
  }

  private static final class Payload {
    private final String text;
    private final String metadata;

    private Payload(String text, String metadata) { this.text = text; this.metadata = metadata; }
  }
  private Source source(String id) throws Exception { try (PreparedStatement s = cp.getPreparedStatement("SELECT s.config_version, t.tablename, k.columnname FROM etarc_vector_source s JOIN ad_table t ON t.ad_table_id=s.ad_table_id JOIN ad_column k ON k.ad_table_id=t.ad_table_id AND k.iskey='Y' AND k.isactive='Y' WHERE s.etarc_vector_source_id=?")) { s.setString(1,id); try(ResultSet r=s.executeQuery()){if(!r.next()) throw new VectorException(VectorErrorCode.VECTOR_OUTBOX_OPERATION_FAILED,"Vector source was not found."); return new Source(r.getLong(1),r.getString(2),r.getString(3));} } }
  private List<SourceColumn> cachedColumns(String id) {
    return columnsBySource.computeIfAbsent(id, this::sourceColumns);
  }

  private VectorEmbeddingProvider cachedProvider(String id) {
    return providerBySource.computeIfAbsent(id, providers::forSource);
  }

  private List<SourceColumn> sourceColumns(String id) {
    try {
      List<SourceColumn> result=new ArrayList<>(); try(PreparedStatement s=cp.getPreparedStatement("SELECT c.columnname, sc.iscontent FROM etarc_vector_source_column sc JOIN ad_column c ON c.ad_column_id=sc.ad_column_id WHERE sc.etarc_vector_source_id=? AND sc.isactive='Y' ORDER BY sc.seqno")){s.setString(1,id);try(ResultSet r=s.executeQuery()){while(r.next())result.add(new SourceColumn(r.getString(1), "Y".equals(r.getString(2))));}}return result;
    } catch (VectorException e) {
      throw e;
    } catch (Exception e) {
      throw new VectorException(VectorErrorCode.VECTOR_OUTBOX_OPERATION_FAILED,
          "Could not read the columns of a vector source.", e);
    }
  }
  private static String columnNames(List<SourceColumn> columns) { StringBuilder result = new StringBuilder(); for(SourceColumn column:columns){if(result.length()>0)result.append(',');result.append(column.name);}return result.toString(); }
  private static String quoted(String values) { StringBuilder result=new StringBuilder(); for(String value:values.split(",")){if(result.length()>0)result.append(',');result.append(quote(value));}return result.toString(); }
  private static String quote(String value) { return "\""+value.toLowerCase().replace("\"","\"\"")+"\""; }
  private static final class Source { final long version; final String table,key; Source(long version,String table,String key){this.version=version;this.table=table;this.key=key;} }
  private static final class SourceColumn { final String name; final boolean content; SourceColumn(String name, boolean content){this.name=name;this.content=content;} boolean isContent(){return content;} }
}
