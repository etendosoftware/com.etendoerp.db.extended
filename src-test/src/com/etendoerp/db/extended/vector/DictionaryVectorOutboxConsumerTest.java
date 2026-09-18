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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.codehaus.jettison.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.openbravo.database.ConnectionProvider;

/**
 * Covers what the dictionary consumer sends to the provider and what it stores.
 *
 * <p>The embedding call is the expensive part of delivery: it leaves the tenant, it is billed per
 * request and it dominates the latency. How many requests a chunk becomes is therefore behaviour,
 * not an implementation detail. What goes into the text decides what a search can find at all,
 * and a record that no longer exists has to take its vector with it or searches keep answering
 * with rows that are gone.</p>
 */
class DictionaryVectorOutboxConsumerTest {

  private final RecordingStore store = new RecordingStore();
  private final RecordingProvider provider = new RecordingProvider();
  private boolean recordExists = true;
  private boolean hasContentColumn = true;

  @Test
  void embedsAWholeChunkInOneRequest() throws Exception {
    DictionaryVectorOutboxConsumer consumer = consumer();

    consumer.prepare(List.of(event("e1", "UPDATE"), event("e2", "UPDATE"), event("e3", "UPDATE")));

    assertEquals(1, provider.calls.size(),
        "one request for the chunk, not one per row: the round trip is what delivery pays for");
    assertEquals(3, provider.calls.get(0).size());
  }

  @Test
  void usesWhatItPreparedInsteadOfAskingAgain() throws Exception {
    DictionaryVectorOutboxConsumer consumer = consumer();
    VectorOutboxEvent event = event("e1", "UPDATE");

    consumer.prepare(List.of(event));
    consumer.consume(event);

    assertEquals(1, provider.calls.size(), "the chunk was already embedded");
    assertEquals(1, store.upserted.size());
  }

  @Test
  void embedsOnItsOwnWhenNothingWasPrepared() throws Exception {
    // The dispatcher may deliver one by one, and a row can change between prepare and consume.
    consumer().consume(event("e1", "UPDATE"));

    assertEquals(1, provider.calls.size(), "the event stays deliverable either way");
    assertEquals(1, store.upserted.size());
  }

  @Test
  void neverReusesAnEmbeddingForASecondDelivery() throws Exception {
    DictionaryVectorOutboxConsumer consumer = consumer();
    VectorOutboxEvent event = event("e1", "UPDATE");

    consumer.prepare(List.of(event));
    consumer.consume(event);
    consumer.consume(event);

    assertEquals(2, provider.calls.size(),
        "the prepared vector is drained on use, so a redelivery embeds the record as it is now "
            + "rather than as it was when the chunk started");
  }

  @Test
  void removesTheVectorOfADeletedRecordWithoutEmbeddingAnything() throws Exception {
    consumer().consume(event("e1", "DELETE"));

    assertEquals(List.of("go.example/rec-1"), store.deleted);
    assertTrue(provider.calls.isEmpty(), "there is nothing left to embed");
    assertTrue(store.upserted.isEmpty());
  }

  @Test
  void removesTheVectorWhenTheRecordIsAlreadyGone() throws Exception {
    recordExists = false;

    consumer().consume(event("e1", "UPDATE"));

    assertEquals(List.of("go.example/rec-1"), store.deleted,
        "an update for a row deleted before delivery caught up still has to take its vector away, "
            + "or a search keeps answering with a record that no longer exists");
    assertTrue(store.upserted.isEmpty());
  }

  @Test
  void refusesASourceWithNothingToEmbed() {
    hasContentColumn = false;

    VectorException failure =
        assertThrows(VectorException.class, () -> consumer().consume(event("e1", "UPDATE")));

    assertEquals(VectorErrorCode.VECTOR_INVALID_METADATA, failure.getCode());
  }

  @Test
  void embedsOnlyTheContentColumnsButStoresEveryField() throws Exception {
    consumer().consume(event("e1", "UPDATE"));

    String text = provider.calls.get(0).get(0);
    assertTrue(text.contains("name: Widget"), "a content column is what the search matches on");
    assertTrue(text.contains("description: A thing"));
    assertTrue(!text.contains("docstatus"),
        "a metadata column is there to filter by, and embedding it would blur the meaning of the "
            + "text a search is compared against");

    JSONObject metadata = new JSONObject(store.upserted.get(0).getMetadata());
    assertEquals("CO", metadata.getJSONObject("fields").getString("docstatus"),
        "but it still has to be stored, or the target filter has nothing to read");
    assertEquals("Widget", metadata.getJSONObject("fields").getString("name"));
  }

  @Test
  void storesTheConfigurationVersionTheEventWasBuiltFor() throws Exception {
    consumer().consume(event("e1", "UPDATE"));

    JSONObject metadata = new JSONObject(store.upserted.get(0).getMetadata());
    assertEquals("SRC1", metadata.getString("sourceId"));
    assertEquals(7, metadata.getInt("configVersion"),
        "the vector records which configuration produced it, so a later one can tell it is stale");
  }

  @Test
  void keepsTheTenantOfTheEventOnTheStoredVector() throws Exception {
    consumer().consume(event("e1", "UPDATE"));

    VectorRecord stored = store.upserted.get(0);
    assertEquals("CLIENT-A", stored.getClientId());
    assertEquals("ORG-A", stored.getOrganizationId(),
        "the scope travels with the vector, because it is what a search filters on");
  }

  @Test
  void takesTheChunkSizeFromTheProvider() {
    assertEquals(25, consumer().batchSize(event("e1", "UPDATE")),
        "how many inputs fit in one request is the provider's limit, not ours");
  }

  // --- fixtures -------------------------------------------------------------------------------

  private static VectorOutboxEvent event(String id, String type) {
    return new VectorOutboxEvent(id, "SRC1", 7L, "go.example", "rec-1", type, null, "CLIENT-A",
        "ORG-A", 7L);
  }

  private DictionaryVectorOutboxConsumer consumer() {
    ConnectionProvider cp = mock(ConnectionProvider.class);
    try {
      when(cp.getPreparedStatement(anyString())).thenAnswer(invocation -> {
        String sql = invocation.getArgument(0);
        ResultSet rows = rowsFor(sql);
        PreparedStatement statement = mock(PreparedStatement.class);
        when(statement.executeQuery()).thenReturn(rows);
        return statement;
      });
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
    return new DictionaryVectorOutboxConsumer(cp, store, new VectorEmbeddingProviderFactory(cp) {
      @Override
      VectorEmbeddingProvider forSource(String sourceId) {
        return provider;
      }
    });
  }

  private ResultSet rowsFor(String sql) throws Exception {
    ResultSet rs = mock(ResultSet.class);
    if (sql.contains("FROM etarc_vector_source_column")) {
      when(rs.next()).thenReturn(true, true, hasContentColumn, false);
      when(rs.getString(1)).thenReturn("name", "description", "docstatus");
      when(rs.getString(2)).thenReturn("Y", "Y", "N");
      if (!hasContentColumn) {
        when(rs.next()).thenReturn(true, false);
        when(rs.getString(1)).thenReturn("docstatus");
        when(rs.getString(2)).thenReturn("N");
      }
    } else if (sql.contains("JOIN ad_column k")) {
      when(rs.next()).thenReturn(true);
      when(rs.getLong(1)).thenReturn(7L);
      when(rs.getString(2)).thenReturn("M_Product");
      when(rs.getString(3)).thenReturn("M_Product_ID");
    } else if (sql.startsWith("SELECT \"")) {
      when(rs.next()).thenReturn(recordExists);
      when(rs.getString("name")).thenReturn("Widget");
      when(rs.getString("description")).thenReturn("A thing");
      when(rs.getString("docstatus")).thenReturn("CO");
    } else {
      when(rs.next()).thenReturn(false);
    }
    return rs;
  }

  /** Remembers what was embedded, and how many requests it took. */
  private static final class RecordingProvider implements VectorEmbeddingProvider {
    private final List<List<String>> calls = new ArrayList<>();

    @Override
    public List<double[]> embed(List<String> texts) {
      calls.add(List.copyOf(texts));
      return texts.stream().map(t -> new double[] { 0.1d, 0.2d }).toList();
    }

    @Override
    public int batchSize() {
      return 25;
    }

    @Override
    public int dimensions() {
      return 2;
    }
  }

  private static final class RecordingStore implements VectorStore {
    private final List<VectorRecord> upserted = new ArrayList<>();
    private final List<String> deleted = new ArrayList<>();

    @Override
    public void upsert(VectorRecord record) {
      upserted.add(record);
    }

    @Override
    public void delete(String namespace, String key) {
      deleted.add(namespace + "/" + key);
    }

    @Override
    public void createCollection(VectorCollection collection) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<VectorMatch> search(VectorQuery query) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void deleteCollection(String namespace) {
      throw new UnsupportedOperationException();
    }
  }
}
