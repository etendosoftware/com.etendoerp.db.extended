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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.openbravo.database.ConnectionProvider;

/**
 * Exercises the statements the trigger service issues, because none of them leaves a trace worth
 * asserting on afterwards.
 *
 * <p>A trigger built on the wrong column, a sweep that reached past the source it was given, or a
 * checksum accepted when it should not have been all look identical once the run is over: the
 * table simply stops enqueueing what it should, or starts enqueueing what it should not, months
 * later and far from here. The statements are the observable behaviour, so they are what these
 * tests read.</p>
 */
class VectorTriggerServiceTest {

  private static final String SOURCE_ID = "B1C2D3E4F5A6478899AABBCCDDEEFF11";
  private static final String PREFIX = "etarc_vsrc_" + SOURCE_ID.toLowerCase();

  /** Every statement the service asked for, in order. */
  private final List<String> statements = new ArrayList<>();

  @Test
  void instrumentsAReadySourceForInsertDeleteAndEachWatchedColumn() throws Exception {
    VectorTriggerService.Deployment deployment =
        service(ready(true), watching("2901", "Value"), watching("2902", "Name")).deploy(SOURCE_ID);

    assertEquals(4, deployment.getInstalled(), "insert, delete and one trigger per watched column");
    assertEquals(1, deployment.getRemoved(), "and the trigger of a column no longer watched goes");
    assertTrue(deployment.isInstrumented());
    assertTrue(created(PREFIX + "_ai"), "an insert has to enqueue");
    assertTrue(created(PREFIX + "_ad"), "a delete has to enqueue");
    assertEquals(2, countMatching("CREATE TRIGGER \"" + PREFIX + "_u_"),
        "one update trigger per watched column, never one for the whole row");
  }

  @Test
  void asksPostgresToFilterTheUpdateByColumnInsteadOfFilteringInTheTrigger() throws Exception {
    service(ready(true), watching("2901", "Value")).deploy(SOURCE_ID);

    String trigger = onlyMatching("CREATE TRIGGER \"" + PREFIX + "_u_");
    assertTrue(trigger.contains("AFTER UPDATE OF \"value\""),
        "the column has to be in the event, so an update elsewhere never reaches the trigger");
    assertTrue(trigger.contains("WHEN (OLD.\"value\" IS DISTINCT FROM NEW.\"value\")"),
        "a write that does not change the value must not enqueue an event");
  }

  @Test
  void givesEachWatchedColumnItsOwnTriggerName() throws Exception {
    // Two columns of one table share their first characters, which is what the previous naming
    // rule truncated to: the second CREATE silently replaced the first and that column stopped
    // enqueueing anything.
    service(ready(true), watching("FF8081812FBFF0CC012FBFF53E16000A", "Value"),
        watching("FF8081812FBFF0CC012FBFF53E16000B", "Name")).deploy(SOURCE_ID);

    List<String> names = matching("CREATE TRIGGER \"" + PREFIX + "_u_");
    assertEquals(2, names.size());
    assertFalse(names.get(0).equals(names.get(1)), "two watched columns may not collide on one name");
  }

  @Test
  void writesEveryOutboxTimestampInUtc() throws Exception {
    service(ready(true), watching("2901", "Value")).deploy(SOURCE_ID);

    String function = onlyMatching("CREATE OR REPLACE FUNCTION");
    assertFalse(function.matches("(?s).*now\\(\\)(?! AT TIME ZONE).*"),
        "PgJDBC takes the session time zone from the JVM, so a bare now() records a local time "
            + "the consumer then compares against UTC");
    assertTrue(function.contains("now() AT TIME ZONE 'UTC'"));
  }

  @Test
  void removesTheTriggersOfASourceItRefusesToInstrument() throws Exception {
    VectorTriggerService.Deployment deployment =
        service(ready(false), watching("2901", "Value")).deploy(SOURCE_ID);

    assertFalse(deployment.isInstrumented());
    assertEquals(0, deployment.getInstalled());
    assertEquals(0, countMatching("CREATE TRIGGER"),
        "a source that cannot be delivered must not capture changes at all");
    assertTrue(statements.stream().anyMatch(s -> s.startsWith("DROP TRIGGER")),
        "and what it had has to go, or the table keeps enqueueing events bound to fail");
    assertEquals(1, deployment.getRemoved());
    assertTrue(statements.stream().anyMatch(s -> s.startsWith("DROP FUNCTION")),
        "the function it fired goes with them, or it lingers with no trigger left to call it");
  }

  @Test
  void tearsDownEvenASourceTheDictionaryConsidersReady() throws Exception {
    VectorTriggerService.Deployment deployment =
        service(ready(true), watching("2901", "Value")).teardown(SOURCE_ID);

    assertFalse(deployment.isInstrumented(),
        "the caller can see runtime state the dictionary cannot, such as a collection that no "
            + "longer matches the provider");
    assertEquals(0, countMatching("CREATE TRIGGER"));
    assertEquals(1, deployment.getRemoved(), "and the capture it had is taken down");
  }

  @Test
  void keepsThePerSourceSweepInsideThatSourcesOwnObjects() throws Exception {
    service(ready(true), watching("2901", "Value")).deploy(SOURCE_ID);

    String sweep = onlyMatching("SELECT t.tgname");
    assertTrue(sweep.contains("starts_with(t.tgname, '" + PREFIX + "')"),
        "activating a few sources says nothing about the rest");
    assertFalse(sweep.contains("LIKE 'etarc_vsrc_%'"),
        "the global sweep would tear down a source that merely happens to be disabled right now");
  }

  @Test
  void sweepsEverythingWhenRebuildingTheWholeDictionary() throws Exception {
    service(ready(true), watching("2901", "Value")).deployAll();

    String sweep = onlyMatching("SELECT t.tgname");
    assertTrue(sweep.contains("LIKE 'etarc_vsrc_%'"),
        "an update has to remove the triggers of a source deleted since the previous run, and "
            + "nothing else would");
  }

  @Test
  void quotesIdentifiersThatComeFromTheDictionary() throws Exception {
    service(ready(true), watching("2901", "we\"ird")).deploy(SOURCE_ID);

    String trigger = onlyMatching("CREATE TRIGGER \"" + PREFIX + "_u_");
    assertTrue(trigger.contains("\"we\"\"ird\""),
        "a column name reaches this as an identifier, not as text to concatenate");
  }

  @Test
  void putsTheDictionaryWatermarkBackAfterAcceptingTheStructure() throws Exception {
    Timestamp watermark = Timestamp.valueOf("2026-09-14 18:28:21");
    service(ready(true), watermark).acceptDatabaseStructure("a test");

    assertTrue(statements.contains("SELECT ad_db_modified('Y') FROM DUAL"));
    assertTrue(statements.contains("UPDATE ad_system_info SET last_dbupdate = ?"),
        "ad_db_modified moves LAST_DBUPDATE along with the checksum, and that column is the "
            + "watermark the dataset check compares row timestamps against: letting it move would "
            + "silence the request to export pending dictionary changes across the instance");
    assertTrue(statements.indexOf("SELECT ad_db_modified('Y') FROM DUAL")
        < statements.indexOf("UPDATE ad_system_info SET last_dbupdate = ?"),
        "the watermark has to be restored after the stamp, not before");
  }

  @Test
  void recordsTheChecksumItReplacedWhenAcceptingTheStructure() throws Exception {
    Timestamp watermark = Timestamp.valueOf("2026-09-14 18:28:21");
    service(ready(true), watermark).acceptDatabaseStructure("a test");

    long readings = statements.stream()
        .filter(sql -> sql.equals("SELECT db_checksum FROM ad_system_info"))
        .count();
    assertEquals(2, readings,
        "accepting the structure is the one act here that can absorb a change nobody meant to "
            + "accept, so the checksum before and the checksum after are both read and logged: "
            + "without them there is no way to tell afterwards that it happened");
  }

  // --- fixtures -------------------------------------------------------------------------------

  private static final class Column {
    private final String id;
    private final String name;

    private Column(String id, String name) {
      this.id = id;
      this.name = name;
    }
  }

  private static Column watching(String id, String name) {
    return new Column(id, name);
  }

  private static boolean ready(boolean value) {
    return value;
  }

  private VectorTriggerService service(boolean sourceIsReady, Column... columns) throws Exception {
    return service(sourceIsReady, null, columns);
  }

  private VectorTriggerService service(boolean sourceIsReady, Timestamp watermark, Column... columns)
      throws Exception {
    ConnectionProvider cp = mock(ConnectionProvider.class);
    when(cp.getRDBMS()).thenReturn("POSTGRE");
    when(cp.getPreparedStatement(anyString())).thenAnswer(invocation -> {
      String sql = invocation.getArgument(0);
      statements.add(sql);
      // Built before the statement is stubbed: Mockito cannot stub one mock while the stubbing of
      // another is still open, and the result set is itself a mock.
      ResultSet rows = resultFor(sql, sourceIsReady, watermark, columns);
      PreparedStatement statement = mock(PreparedStatement.class);
      when(statement.executeQuery()).thenReturn(rows);
      when(statement.executeUpdate()).thenReturn(1);
      return statement;
    });
    return new VectorTriggerService(cp);
  }

  /** One result set shaped for whichever query the service is running. */
  private ResultSet resultFor(String sql, boolean sourceIsReady, Timestamp watermark, Column[] columns)
      throws Exception {
    ResultSet rs = mock(ResultSet.class);
    if (sql.contains("FROM etarc_vector_source ")) {
      when(rs.next()).thenReturn(true, false);
      when(rs.getString("etarc_vector_source_id")).thenReturn(SOURCE_ID);
      when(rs.getString("tablename")).thenReturn("C_BPartner");
      when(rs.getString("key_column")).thenReturn("C_BPartner_ID");
      when(rs.getString("client_column")).thenReturn("AD_Client_ID");
      when(rs.getString("organization_column")).thenReturn("AD_Org_ID");
      when(rs.getBoolean("ready")).thenReturn(sourceIsReady);
      when(rs.getString("isinsertenabled")).thenReturn("Y");
      when(rs.getString("isupdateenabled")).thenReturn("Y");
      when(rs.getString("isdeleteenabled")).thenReturn("Y");
    } else if (sql.contains("FROM etarc_vector_source_column")) {
      Boolean[] more = new Boolean[columns.length + 1];
      for (int i = 0; i < columns.length; i++) {
        more[i] = Boolean.TRUE;
      }
      more[columns.length] = Boolean.FALSE;
      when(rs.next()).thenReturn(more[0], java.util.Arrays.copyOfRange(more, 1, more.length));
      if (columns.length > 0) {
        when(rs.getString("ad_column_id")).thenReturn(columns[0].id,
            java.util.Arrays.stream(columns).skip(1).map(c -> c.id).toArray(String[]::new));
        when(rs.getString("columnname")).thenReturn(columns[0].name,
            java.util.Arrays.stream(columns).skip(1).map(c -> c.name).toArray(String[]::new));
      }
    } else if (sql.startsWith("SELECT t.tgname")) {
      // What the source already had installed, so a sweep has something real to consider. The
      // suffix is deliberately not one this run produces: that is what makes it stale.
      when(rs.next()).thenReturn(true, false);
      when(rs.getString(1)).thenReturn(PREFIX + "_u_00000000");
      when(rs.getString(2)).thenReturn("c_bpartner");
    } else if (sql.startsWith("SELECT proname")) {
      when(rs.next()).thenReturn(true, false);
      when(rs.getString(1)).thenReturn(PREFIX + "_fn");
    } else if (sql.contains("last_dbupdate FROM ad_system_info")) {
      when(rs.next()).thenReturn(true);
      when(rs.getTimestamp(1)).thenReturn(watermark);
    } else {
      when(rs.next()).thenReturn(false);
    }
    return rs;
  }

  // --- reading the statements -----------------------------------------------------------------

  private List<String> matching(String prefix) {
    return statements.stream().filter(s -> s.startsWith(prefix)).toList();
  }

  private int countMatching(String prefix) {
    return matching(prefix).size();
  }

  private String onlyMatching(String prefix) {
    List<String> found = matching(prefix);
    assertFalse(found.isEmpty(), "no statement starts with " + prefix);
    return found.get(0);
  }

  private boolean created(String triggerName) {
    return statements.stream().anyMatch(s -> s.startsWith("CREATE TRIGGER \"" + triggerName + "\""));
  }
}
