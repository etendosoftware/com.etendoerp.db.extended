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
import static org.junit.jupiter.api.Assertions.assertFalse;
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

import com.etendoerp.db.extended.vector.VectorSourceReadiness.Candidate;
import com.etendoerp.db.extended.vector.VectorSourceReadiness.Collection;
import com.etendoerp.db.extended.vector.ActivateVectorSource.Report;
import com.etendoerp.db.extended.vector.VectorSourceReadiness.Verdict;

/**
 * Covers the two halves of the activation button: what it decides about a source, and the order it
 * works in.
 *
 * <p>Both are invisible afterwards. A source wrongly called ready looks exactly like a working one
 * until its first delivery fails; a checksum accepted at the wrong moment looks like nothing at
 * all until an update.database months later refuses to run, or fails to refuse.</p>
 */
class ActivateVectorSourceTest {

  // --- the decision ---------------------------------------------------------------------------

  @Test
  void refusesASourceThatCaptureWouldOnlyBreakOn() {
    assertEquals(Verdict.DISABLED, VectorSourceReadiness.verdict(source().disabled(), null));
    assertEquals(Verdict.WITHOUT_PROVIDER, VectorSourceReadiness.verdict(source().withoutProvider(), null));
    assertEquals(Verdict.WITHOUT_COLUMNS, VectorSourceReadiness.verdict(source().withColumns(0, 0), null));
    assertEquals(Verdict.WITHOUT_CONTENT, VectorSourceReadiness.verdict(source().withColumns(3, 0), null),
        "columns that are all metadata leave the consumer with nothing to embed");
  }

  @Test
  void treatsEveryRefusalAsNotReady() {
    for (Verdict verdict : Verdict.values()) {
      boolean expected = verdict == Verdict.COLLECTION_MISSING || verdict == Verdict.READY;
      assertEquals(expected, verdict.isUsable(), verdict + " must not drift from what it means");
      assertFalse(verdict.getMessageKey().isEmpty(),
          verdict + " has to name a message, or the administrator is told nothing");
    }
  }

  @Test
  void createsTheCollectionOfASourceThatHasNoneYet() {
    assertEquals(Verdict.COLLECTION_MISSING, VectorSourceReadiness.verdict(source().build(), null));
  }

  @Test
  void refusesASourceWhoseCollectionNoLongerMatchesItsProvider() {
    Candidate candidate = source().withDimensions(3072).build();

    assertEquals(Verdict.DIMENSION_DRIFT,
        VectorSourceReadiness.verdict(candidate, new Collection(1536, "COSINE")),
        "writing 3072-dimension vectors into a 1536 collection fails on every row");
  }

  @Test
  void refusesASourceWhoseCollectionNoLongerMatchesItsMetric() {
    Candidate candidate = source().withMetric("L2").build();

    assertEquals(Verdict.METRIC_DRIFT,
        VectorSourceReadiness.verdict(candidate, new Collection(1536, "COSINE")),
        "the collection decides which operator a search uses, so a changed metric is not applied");
  }

  @Test
  void leavesASourceThatAlreadyAgreesWithItsCollectionAlone() {
    assertEquals(Verdict.READY,
        VectorSourceReadiness.verdict(source().build(), new Collection(1536, "COSINE")));
  }

  @Test
  void checksTheDictionaryBeforeTheCollection() {
    // A disabled source with a drifted collection has two problems; the one to report is the one
    // the administrator can act on in the window.
    assertEquals(Verdict.DISABLED,
        VectorSourceReadiness.verdict(source().disabled(), new Collection(99, "L2")));
  }

  // --- the order of the run -------------------------------------------------------------------

  @Test
  void readsTheStructureBeforeTheFirstStatementThatCanAlterIt() throws Exception {
    Run run = run(structureAccepted(true), source().build());

    int baseline = run.indexOf("SELECT ad_db_modified('N')");
    int firstDdl = run.indexOf("CREATE TABLE IF NOT EXISTS etarc_vector.etarc_vector_activation");
    assertTrue(baseline >= 0, "the run has to look before it leaps");
    assertTrue(baseline < firstDdl,
        "activation creates tables of its own, so reading after it would report our own change "
            + "and the structure would never be accepted");
  }

  @Test
  void acceptsTheStructureItChangedWhenItWasAcceptedBefore() throws Exception {
    Run run = run(structureAccepted(true), source().build());

    assertTrue(run.statements.contains("SELECT ad_db_modified('Y') FROM DUAL"));
    assertTrue(run.indexOf("SELECT ad_db_modified('Y')") > run.indexOf("CREATE TRIGGER"),
        "the structure is accepted once the run has finished changing it");
  }

  @Test
  void leavesTheStructureAloneWhenNoChecksumWasEverStamped() throws Exception {
    Run run = run(structureNeverStamped(), source().build());

    assertFalse(run.statements.contains("SELECT ad_db_modified('Y') FROM DUAL"),
        "ad_db_modified answers N both when the stored checksum matches and when there is none, "
            + "so an unstamped database would have its existing changes accepted along with ours");
  }

  @Test
  void leavesTheStructureAloneWhenTheDatabaseDeniesTheChangeItJustMade() throws Exception {
    Run run = run(structureThatNeverMoves(), source().build());

    assertFalse(run.statements.contains("SELECT ad_db_modified('Y') FROM DUAL"),
        "the run installed triggers, so a verdict of N means the function is not answering -- it "
            + "ends in EXCEPTION WHEN OTHERS THEN RETURN 'N' -- and its answer cannot be trusted "
            + "to say whose the delta is");
  }

  @Test
  void serialisesActivationsAgainstEachOther() throws Exception {
    Run run = run(structureAccepted(true), source().build());

    int lock = run.indexOf("SELECT pg_advisory_lock");
    int unlock = run.indexOf("SELECT pg_advisory_unlock");
    assertTrue(lock >= 0, "two administrators pressing the button at once would interleave");
    assertTrue(lock < run.indexOf("SELECT ad_db_modified('N')"),
        "the lock has to cover the reading, not just the writing");
    assertTrue(unlock > run.indexOf("SELECT ad_db_modified('Y')"),
        "the lock is held until the structure has been accepted");
  }

  @Test
  void leavesTheStructureAloneWhenSomethingElseHadAlreadyChangedIt() throws Exception {
    Run run = run(structureAccepted(false), source().build());

    assertFalse(run.statements.contains("SELECT ad_db_modified('Y') FROM DUAL"),
        "the delta would hold somebody else's change as well, and catching that is what the "
            + "check exists for");
  }

  @Test
  void createsTheCollectionBeforeTheTableStartsEnqueueingIntoIt() throws Exception {
    Run run = run(structureAccepted(true), source().build());

    assertEquals(1, run.store.created.size());
    assertTrue(run.store.createdAt < run.indexOf("CREATE TRIGGER"),
        "a trigger firing before its collection exists enqueues events that fail on delivery");
  }

  @Test
  void takesDownTheCaptureOfASourceItRefuses() throws Exception {
    Run run = run(structureAccepted(true), source().withColumns(3, 0));

    assertTrue(run.store.created.isEmpty(), "no collection for a source that cannot be delivered");
    assertEquals(0, run.count("CREATE TRIGGER"));
    assertTrue(run.statements.stream().anyMatch(s -> s.startsWith("DROP TRIGGER")));
  }

  @Test
  void commitsTheActivationBeforeTouchingAnySource() throws Exception {
    Run run = run(structureAccepted(true), source().build());

    // A commit records how many statements had run when it happened, so it sits between the last
    // statement before it and the first one after.
    assertTrue(run.indexOf("CREATE TABLE IF NOT EXISTS etarc_vector_record") < run.commits.get(0),
        "the runtime storage has to be durable before a source is written into it");
    assertTrue(run.commits.get(0) <= run.indexOf("SELECT dimensions, metric"),
        "and no source may be looked at before that commit");
    assertEquals(3, run.commits.size(), "activation, the sources, and the accepted structure");
  }

  @Test
  void reportsAWholeRunAsUnreadyWhenAnySourceIs() throws Exception {
    Report report = run(structureAccepted(true), source().build(), source().disabled()).report;

    assertEquals(2, report.lines.size());
    assertFalse(report.allReady(), "one unusable source is not a successful run");
  }

  @Test
  void carriesBothSidesOfADriftSoTheMessageCanNameThem() throws Exception {
    Run run = run(structureAccepted(true), existingCollection(1536, "COSINE"),
        source().withDimensions(3072).build());

    String[] params = run.report.lines.get(0).messageParameters();
    assertEquals(2, params.length);
    assertEquals("1536", params[0], "what the collection holds");
    assertEquals("3072", params[1], "what the provider now produces");
  }

  @Test
  void saysNothingAboutValuesWhenThereIsNoDriftToExplain() throws Exception {
    Run run = run(structureAccepted(true), source().build());

    assertEquals(0, run.report.lines.get(0).messageParameters().length);
  }

  // --- fixtures -------------------------------------------------------------------------------

  private static SourceBuilder source() {
    return new SourceBuilder();
  }

  /** A source that is ready for everything, so each test only says how it differs. */
  private static final class SourceBuilder {
    private boolean enabled = true;
    private Integer dimensions = 1536;
    private String metric = "COSINE";
    private int columns = 3;
    private int contentColumns = 2;

    Candidate build() {
      return new Candidate("SRC1", "Example", "go.example", metric, enabled, dimensions,
          new VectorSourceReadiness.Columns(columns, contentColumns));
    }

    Candidate disabled() {
      enabled = false;
      return build();
    }

    Candidate withoutProvider() {
      dimensions = null;
      return build();
    }

    Candidate withColumns(int total, int content) {
      columns = total;
      contentColumns = content;
      return build();
    }

    SourceBuilder withDimensions(int value) {
      dimensions = value;
      return this;
    }

    SourceBuilder withMetric(String value) {
      metric = value;
      return this;
    }
  }

  /**
   * How the database answers the two questions the run asks about its structure.
   *
   * <p>A real database does not answer the same thing before and after the run: installing the
   * triggers is what moves the checksum. Modelling it as one fixed value let the run look accepted
   * after changing the schema, which is the state the run now refuses to stamp.</p>
   */
  private static final class Structure {
    private final boolean stamped;
    private final boolean accepted;
    private final boolean movesWhenChanged;
    private int verdicts;

    private Structure(boolean stamped, boolean accepted, boolean movesWhenChanged) {
      this.stamped = stamped;
      this.accepted = accepted;
      this.movesWhenChanged = movesWhenChanged;
    }

    private String verdict() {
      return verdicts++ == 0 ? (accepted ? "N" : "Y") : (movesWhenChanged ? "Y" : "N");
    }
  }

  private static Structure structureAccepted(boolean value) {
    return new Structure(true, value, true);
  }

  /** A database that carries no checksum at all, so nothing about it was ever accepted. */
  private static Structure structureNeverStamped() {
    return new Structure(false, true, true);
  }

  /** A database that reports itself unchanged even after the run installed triggers. */
  private static Structure structureThatNeverMoves() {
    return new Structure(true, true, false);
  }

  private static Collection existingCollection(int dimensions, String metric) {
    return new Collection(dimensions, metric);
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
    public void upsert(VectorRecord record) {
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
    private final List<Integer> commits = new ArrayList<>();
    private RecordingStore store;
    private Report report;

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

  private Run run(Structure structure, Candidate... candidates) throws Exception {
    return run(structure, null, candidates);
  }

  private Run run(Structure structure, Collection existing, Candidate... candidates)
      throws Exception {
    Run run = new Run();
    run.store = new RecordingStore(run.statements);
    ConnectionProvider cp = mock(ConnectionProvider.class);
    when(cp.getRDBMS()).thenReturn("POSTGRE");
    when(cp.getPreparedStatement(anyString())).thenAnswer(invocation -> {
      String sql = invocation.getArgument(0);
      run.statements.add(sql);
      ResultSet rows = rowsFor(sql, structure, existing);
      PreparedStatement statement = mock(PreparedStatement.class);
      when(statement.executeQuery()).thenReturn(rows);
      when(statement.executeUpdate()).thenReturn(1);
      return statement;
    });

    run.report = new ActivateVectorSource().run(List.of(candidates), cp, run.store,
        () -> run.commits.add(run.statements.size()));
    return run;
  }

  private ResultSet rowsFor(String sql, Structure structure, Collection existing)
      throws Exception {
    ResultSet rs = mock(ResultSet.class);
    if (sql.startsWith("SELECT ad_db_modified('N')")) {
      when(rs.next()).thenReturn(true);
      when(rs.getString(1)).thenReturn(structure.verdict());
    } else if (sql.startsWith("SELECT db_checksum IS NOT NULL")) {
      when(rs.next()).thenReturn(true);
      when(rs.getBoolean(1)).thenReturn(structure.stamped);
    } else if (sql.startsWith("SELECT dimensions, metric")) {
      when(rs.next()).thenReturn(existing != null);
      if (existing != null) {
        when(rs.getInt(1)).thenReturn(existing.dimensions);
        when(rs.getString(2)).thenReturn(existing.metric);
      }
    } else if (sql.contains("FROM etarc_vector_source ")) {
      when(rs.next()).thenReturn(true, false);
      when(rs.getString("etarc_vector_source_id")).thenReturn("SRC1");
      when(rs.getString("tablename")).thenReturn("C_BPartner");
      when(rs.getString("key_column")).thenReturn("C_BPartner_ID");
      when(rs.getBoolean("ready")).thenReturn(true);
      when(rs.getString("isinsertenabled")).thenReturn("Y");
      when(rs.getString("isupdateenabled")).thenReturn("N");
      when(rs.getString("isdeleteenabled")).thenReturn("N");
    } else if (sql.startsWith("SELECT t.tgname")) {
      when(rs.next()).thenReturn(true, false);
      when(rs.getString(1)).thenReturn("etarc_vsrc_src1_u_00000000");
      when(rs.getString(2)).thenReturn("c_bpartner");
    } else if (sql.contains("installed")) {
      when(rs.next()).thenReturn(true);
      when(rs.getBoolean("installed")).thenReturn(true);
    } else if (sql.contains("state = 'ACTIVE' FROM etarc_vector.etarc_vector_activation")) {
      when(rs.next()).thenReturn(true);
      when(rs.getBoolean(1)).thenReturn(true);
    } else {
      when(rs.next()).thenReturn(false);
    }
    return rs;
  }
}
