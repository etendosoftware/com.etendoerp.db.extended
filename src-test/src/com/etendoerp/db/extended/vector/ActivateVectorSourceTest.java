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
 * Covers what the button decides about a source, and that deciding is all it does.
 *
 * <p>A source wrongly called ready looks exactly like a working one until its first delivery
 * fails, which is the whole reason to ask before running the update. And a check that wrote
 * something would be back to changing the database from a window, which is what this stopped
 * doing.</p>
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
  void reportsASourceWithNoCollectionAsWaitingForTheUpdate() {
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

  // --- and deciding is all it does -------------------------------------------------------------

  @Test
  void writesNothingAtAll() throws Exception {
    Run run = check(source().build());

    assertTrue(run.statements.stream().noneMatch(VectorProvisioningIsTheOnlyWriter::writes),
        "the button runs while the application does, and DDL from there moves the structure "
            + "checksum nobody can then accept without accepting everything else too: "
            + run.statements);
  }

  @Test
  void reportsEverySourceAsWaitingWhenTheStorageIsNotThereYet() throws Exception {
    Run run = check(notProvisioned(), source().build());

    assertEquals(Verdict.COLLECTION_MISSING, run.report.lines.get(0).verdict);
    assertEquals(0, run.count("SELECT dimensions, metric"),
        "before the first update there is no collection table to ask, and asking anyway fails "
            + "with a missing relation instead of the true answer");
  }

  @Test
  void reportsAWholeRunAsUnreadyWhenAnySourceIs() throws Exception {
    Report report = check(source().build(), source().disabled()).report;

    assertEquals(2, report.lines.size());
    assertFalse(report.allReady(), "one unusable source is not a successful check");
  }

  @Test
  void carriesBothSidesOfADriftSoTheMessageCanNameThem() throws Exception {
    Run run = check(provisioned(), existingCollection(1536, "COSINE"),
        source().withDimensions(3072).build());

    String[] params = run.report.lines.get(0).messageParameters();
    assertEquals(2, params.length);
    assertEquals("1536", params[0], "what the collection holds");
    assertEquals("3072", params[1], "what the provider now produces");
  }

  @Test
  void saysNothingAboutValuesWhenThereIsNoDriftToExplain() throws Exception {
    Run run = check(source().build());

    assertEquals(0, run.report.lines.get(0).messageParameters().length);
  }

  /** Every statement that changes something, named once so the check above cannot drift. */
  private static final class VectorProvisioningIsTheOnlyWriter {
    private static final List<String> PREFIXES = List.of("CREATE ", "DROP ", "ALTER ", "INSERT ",
        "UPDATE ", "DELETE ", "DO $");

    private VectorProvisioningIsTheOnlyWriter() {
    }

    private static boolean writes(String sql) {
      String normalised = sql.trim().toUpperCase();
      return PREFIXES.stream().anyMatch(normalised::startsWith);
    }
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

  private static Collection existingCollection(int dimensions, String metric) {
    return new Collection(dimensions, metric);
  }

  /** One execution, with everything it did written down in order. */
  private static final class Run {
    private final List<String> statements = new ArrayList<>();
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

  private static boolean provisioned() {
    return true;
  }

  /** A database on which no update has provisioned anything yet. */
  private static boolean notProvisioned() {
    return false;
  }

  private Run check(Candidate... candidates) throws Exception {
    return check(true, null, candidates);
  }

  private Run check(boolean provisioned, Candidate... candidates) throws Exception {
    return check(provisioned, null, candidates);
  }

  private Run check(boolean provisioned, Collection existing, Candidate... candidates)
      throws Exception {
    Run run = new Run();
    ConnectionProvider cp = mock(ConnectionProvider.class);
    when(cp.getRDBMS()).thenReturn("POSTGRE");
    when(cp.getPreparedStatement(anyString())).thenAnswer(invocation -> {
      String sql = invocation.getArgument(0);
      run.statements.add(sql);
      ResultSet rows = rowsFor(sql, provisioned, existing);
      PreparedStatement statement = mock(PreparedStatement.class);
      when(statement.executeQuery()).thenReturn(rows);
      when(statement.executeUpdate()).thenReturn(1);
      return statement;
    });

    run.report = new ActivateVectorSource().check(List.of(candidates), cp);
    return run;
  }

  private ResultSet rowsFor(String sql, boolean provisioned, Collection existing)
      throws Exception {
    ResultSet rs = mock(ResultSet.class);
    if (sql.startsWith("SELECT dimensions, metric")) {
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
    } else if (sql.contains("state = 'ACTIVE'")) {
      when(rs.next()).thenReturn(provisioned);
      when(rs.getBoolean(1)).thenReturn(provisioned);
    } else {
      when(rs.next()).thenReturn(false);
    }
    return rs;
  }
}
