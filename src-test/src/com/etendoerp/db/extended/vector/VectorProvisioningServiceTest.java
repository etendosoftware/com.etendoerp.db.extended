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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.openbravo.database.ConnectionProvider;

/**
 * Covers what the update installs, and — more importantly — what it does not.
 *
 * <p>Both are invisible afterwards. Storage created for an instance that never asked for it is a
 * PostgreSQL extension nobody decided to run; a collection missing when the triggers start firing
 * is a queue of events that fail one by one on delivery.</p>
 */
class VectorProvisioningServiceTest {

  @Test
  void installsNothingWhenNoSourceAsksForStorage() throws Exception {
    Run run = provision(source().withColumns(3, 0));

    assertEquals(0, run.count("CREATE EXTENSION"),
        "having the module installed is not asking for pgvector: a source that cannot be "
            + "delivered must not bring an extension into the database");
    assertEquals(0, run.count("CREATE TABLE"));
    assertTrue(run.store.created.isEmpty());
  }

  @Test
  void installsTheStorageWhenOneSourceDoesAskForIt() throws Exception {
    Run run = provision(source());

    assertEquals(1, run.count("CREATE EXTENSION"));
    assertEquals(1, run.store.created.size());
  }

  @Test
  void leavesTheExtensionAloneWhenItIsAlreadyThere() throws Exception {
    Run run = provision(source().withExtensionAlreadyInstalled());

    assertEquals(0, run.count("CREATE EXTENSION"),
        "the statement is refused to an unprivileged role, so it must not be issued for nothing");
    assertEquals(1, run.store.created.size(), "and the rest of the provisioning still happens");
  }

  @Test
  void createsTheStorageBeforeTheCollectionThatLivesInIt() throws Exception {
    Run run = provision(source());

    assertTrue(run.indexOf("CREATE TABLE IF NOT EXISTS etarc_vector.etarc_vector_record")
        < run.store.createdAt, "a collection row needs its table to exist first");
  }

  @Test
  void createsTheCollectionBeforeTheTableStartsEnqueueingIntoIt() throws Exception {
    Run run = provision(source());

    assertTrue(run.store.createdAt < run.indexOf("CREATE TRIGGER"),
        "a trigger firing before its collection exists enqueues events that fail on delivery");
  }

  @Test
  void sweepsTheTriggersEvenWhenNothingIsProvisioned() throws Exception {
    Run run = provision(source().withColumns(3, 0));

    assertTrue(run.statements.stream().anyMatch(s -> s.startsWith("SELECT t.tgname")),
        "a source turned off since the last update leaves triggers that nothing else removes");
  }

  // --- fixtures ---------------------------------------------------------------------------------

  private static SourceRow source() {
    return new SourceRow();
  }

  /** One row of the source query, ready for everything, so a test only says how it differs. */
  private static final class SourceRow {
    private Integer dimensions = 1536;
    private int columns = 3;
    private int contentColumns = 2;
    private boolean extensionInstalled = false;

    private SourceRow withExtensionAlreadyInstalled() {
      extensionInstalled = true;
      return this;
    }

    private SourceRow withColumns(int total, int content) {
      columns = total;
      contentColumns = content;
      return this;
    }
  }

  /** A recording store, so creating a collection is observable without a database. */
  private static final class RecordingStore implements VectorStore {
    private final List<VectorCollection> created = new ArrayList<>();
    private final List<String> statements;
    private int createdAt = -1;

    private RecordingStore(List<String> statements) {
      this.statements = statements;
    }

    @Override
    public void createCollection(VectorCollection collection) {
      created.add(collection);
      createdAt = statements.size();
    }

    @Override
    public void upsert(VectorRecord vectorRecord) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<VectorMatch> search(VectorQuery query) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void delete(String namespace, String key) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void deleteCollection(String namespace) {
      throw new UnsupportedOperationException();
    }
  }

  /** One execution, with everything it did written down in order. */
  private static final class Run {
    private final List<String> statements = new ArrayList<>();
    private RecordingStore store;

    private int indexOf(String prefix) {
      for (int i = 0; i < statements.size(); i++) {
        if (statements.get(i).startsWith(prefix)) {
          return i;
        }
      }
      return -1;
    }

    private int count(String prefix) {
      return (int) statements.stream().filter(s -> s.startsWith(prefix)).count();
    }
  }

  private Run provision(SourceRow row) throws Exception {
    Run run = new Run();
    run.store = new RecordingStore(run.statements);
    ConnectionProvider cp = mock(ConnectionProvider.class);
    when(cp.getRDBMS()).thenReturn("POSTGRE");
    when(cp.getPreparedStatement(anyString())).thenAnswer(invocation -> {
      String sql = invocation.getArgument(0);
      run.statements.add(sql);
      // Built before the stubbing starts: rowsFor stubs a mock of its own, and doing that inside
      // an open when() is what Mockito calls unfinished stubbing.
      ResultSet rows = rowsFor(sql, row);
      PreparedStatement statement = mock(PreparedStatement.class);
      when(statement.executeQuery()).thenReturn(rows);
      when(statement.executeUpdate()).thenReturn(1);
      return statement;
    });

    new VectorProvisioningService(cp, run.store).provision();
    return run;
  }

  private ResultSet rowsFor(String sql, SourceRow row) throws Exception {
    ResultSet rs = mock(ResultSet.class);
    boolean deliverable = row.dimensions != null && row.contentColumns > 0;
    if (sql.contains("s.namespace") && sql.contains("content_columns")) {
      when(rs.next()).thenReturn(true, false);
      when(rs.getString("etarc_vector_source_id")).thenReturn("SRC1");
      when(rs.getString("name")).thenReturn("Example");
      when(rs.getString("namespace")).thenReturn("go.example");
      when(rs.getString("distance_metric")).thenReturn("COSINE");
      when(rs.getString("isactive")).thenReturn("Y");
      when(rs.getString("isenabled")).thenReturn("Y");
      when(rs.getInt("dimensions")).thenReturn(row.dimensions == null ? 0 : row.dimensions);
      when(rs.wasNull()).thenReturn(row.dimensions == null);
      when(rs.getInt("columns")).thenReturn(row.columns);
      when(rs.getInt("content_columns")).thenReturn(row.contentColumns);
      when(rs.getInt("key_columns")).thenReturn(1);
    } else if (sql.startsWith("SELECT dimensions, metric")) {
      when(rs.next()).thenReturn(false);
    } else if (sql.contains("t.tablename")) {
      when(rs.next()).thenReturn(deliverable, false);
      when(rs.getString("etarc_vector_source_id")).thenReturn("SRC1");
      when(rs.getString("tablename")).thenReturn("C_BPartner");
      when(rs.getString("key_column")).thenReturn("C_BPartner_ID");
      when(rs.getBoolean("ready")).thenReturn(true);
      when(rs.getString("isinsertenabled")).thenReturn("Y");
      when(rs.getString("isupdateenabled")).thenReturn("N");
      when(rs.getString("isdeleteenabled")).thenReturn("N");
    } else if (sql.contains("installed")) {
      when(rs.next()).thenReturn(true);
      when(rs.getBoolean("installed")).thenReturn(row.extensionInstalled);
      when(rs.getBoolean("available")).thenReturn(true);
    } else if (sql.contains("state = 'ACTIVE'")) {
      when(rs.next()).thenReturn(true);
      when(rs.getBoolean(1)).thenReturn(true);
    } else {
      when(rs.next()).thenReturn(false);
    }
    return rs;
  }
}
