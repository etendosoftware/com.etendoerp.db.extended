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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
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
 * Covers the backfill, whose whole job is to walk a table nobody can afford to read at once.
 *
 * <p>Everything that can go wrong here is silent. A cursor that does not advance re-enqueues the
 * same page forever; one that advances past what was written skips records that are then missing
 * from the index with nothing to say so. A run that ignores the backlog fills the queue faster
 * than delivery drains it, and a failure that forgets to record itself leaves a request claimed
 * and never picked up again.</p>
 */
class VectorReindexServiceTest {

  /** Statements and transaction boundaries in one list, so order can be asserted across both. */
  private final List<String> log = new ArrayList<>();
  private final List<List<Object>> parameters = new ArrayList<>();

  /** How many records each successive chunk reports, and the key it left off at. */
  private int[] chunkSizes = { 2, 0 };
  private long backlog = 0;
  private int claimed = 1;
  private Long totalCount = 1000L;

  @Test
  void refusesSizesThatWouldNeverMakeProgress() {
    assertThrows(IllegalArgumentException.class, () -> service().process(0, 1, 1));
    assertThrows(IllegalArgumentException.class, () -> service().process(1, 0, 1));
    assertThrows(IllegalArgumentException.class, () -> service().process(1, 1, 0),
        "a backlog limit of zero would stop before enqueueing anything, forever");
  }

  @Test
  void doesNothingWhenNoRequestWasClaimed() {
    claimed = 0;

    assertEquals(0, service().process(100, 10, 1000));
    assertFalse(log.stream().anyMatch(s -> s.startsWith("WITH page AS")),
        "a run that claimed nothing has no request to walk");
  }

  @Test
  void makesTheClaimDurableBeforeWalkingTheTable() {
    service().process(100, 10, 1000);

    assertEquals("commit", log.get(indexOf("UPDATE etarc_vector_reindex_req SET status = 'PROCESSING'") + 1),
        "the claim has to be committed before the work, or a second run claims the same request");
  }

  @Test
  void readsAndInsertsAndMovesTheCursorInOneStatement() {
    service().process(100, 10, 1000);

    String chunk = first("WITH page AS");
    assertTrue(chunk.contains("INSERT INTO etarc_vector_outbox"),
        "reading the page and enqueueing from it in one statement is what keeps them in step");
    assertTrue(chunk.contains("SELECT count(*), max(page."),
        "and the new cursor comes from the same page that was just written");
    assertTrue(chunk.contains("ORDER BY") && chunk.contains("LIMIT ?"),
        "a keyset page, not an offset: an offset re-reads everything it already walked");
  }

  @Test
  void continuesTheNextChunkFromWhereTheLastOneStopped() {
    chunkSizes = new int[] { 2, 2, 0 };

    service().process(100, 10, 1000);

    List<List<Object>> chunks = parametersOf("WITH page AS");
    assertEquals(3, chunks.size());
    assertEquals(null, chunks.get(0).get(0), "the first page starts at the beginning of the table");
    assertEquals("key-1", chunks.get(1).get(0),
        "the second page starts after the last key the first one wrote");
    assertEquals("key-2", chunks.get(2).get(0));
  }

  @Test
  void commitsEachChunkSoAFailureNeverCostsTheWholeWalk() {
    chunkSizes = new int[] { 2, 2, 0 };

    service().process(100, 10, 1000);

    long commitsBetweenChunks = 0;
    int first = indexOf("WITH page AS");
    int last = lastIndexOf("WITH page AS");
    for (int i = first; i < last; i++) {
      if ("commit".equals(log.get(i))) {
        commitsBetweenChunks++;
      }
    }
    assertTrue(commitsBetweenChunks >= 2, "each page is durable before the next one is read");
  }

  @Test
  void stopsAtTheChunkLimitEvenWithTableLeft() {
    chunkSizes = new int[] { 2, 2, 2, 2, 2 };

    int enqueued = service().process(100, 3, 1000);

    assertEquals(6, enqueued);
    assertEquals(3, count("WITH page AS"), "a run is bounded so it cannot hold the process forever");
    assertFalse(log.stream().anyMatch(s -> s.contains("status = 'DONE'")),
        "there is table left, so the request stays claimed for the next run");
  }

  @Test
  void stopsWhenTheQueueIsAlreadyAsFullAsDeliveryCanDrain() {
    backlog = 5000;

    int enqueued = service().process(100, 10, 1000);

    assertEquals(0, enqueued);
    assertEquals(0, count("WITH page AS"), "nothing is added on top of a backlog already too deep");
    assertFalse(log.stream().anyMatch(s -> s.contains("status = 'DONE'")),
        "the request is left PROCESSING so the next run continues from the stored cursor");
  }

  @Test
  void marksTheRequestDoneOnlyWhenTheTableIsExhausted() {
    chunkSizes = new int[] { 2, 0 };

    service().process(100, 10, 1000);

    assertTrue(log.stream().anyMatch(s -> s.contains("status = 'DONE'")),
        "a page that returns nothing is the end of the table");
  }

  @Test
  void countsTheRecordsItEnqueued() {
    chunkSizes = new int[] { 3, 4, 0 };

    assertEquals(7, service().process(100, 10, 1000));
  }

  @Test
  void estimatesTheSizeOnlyOnceAndNeverByCountingRows() {
    totalCount = null;

    service().process(100, 10, 1000);

    String estimate = first("UPDATE etarc_vector_reindex_req SET total_count");
    assertTrue(estimate.contains("reltuples"),
        "an exact count scans the whole table, which is the thing this service exists to avoid");
    assertEquals(1, count("UPDATE etarc_vector_reindex_req SET total_count"));
  }

  @Test
  void leavesTheEstimateAloneWhenItIsAlreadyKnown() {
    totalCount = 1000L;

    service().process(100, 10, 1000);

    assertEquals(0, count("UPDATE etarc_vector_reindex_req SET total_count"));
  }

  @Test
  void recoversTheTransactionBeforeRecordingAFailure() {
    chunkSizes = new int[] { -1 };

    assertThrows(VectorException.class, () -> service().process(100, 10, 1000));

    int failure = indexOf("UPDATE etarc_vector_reindex_req SET status = 'FAILED'");
    assertTrue(failure > 0, "a request that failed has to say so, or it stays claimed forever");
    assertEquals("rollback", log.get(failure - 1),
        "the failing statement left the transaction aborted, so the record of the failure could "
            + "not be written without recovering it first");
    assertEquals("commit", log.get(failure + 1), "and the failure itself has to be durable");
  }

  @Test
  void enqueuesExactlyWhatOngoingChangesWould() {
    service(true).process(100, 10, 1000);

    assertTrue(first("WITH page AS").contains("IS NOT DISTINCT FROM ?"),
        "a filtered source captures only the records matching its filter, so a backfill that "
            + "ignored it would index records the triggers never enqueue");
  }

  @Test
  void leavesOutTheFilterWhenTheSourceHasNone() {
    service(false).process(100, 10, 1000);

    assertFalse(first("WITH page AS").contains("IS NOT DISTINCT FROM"));
  }

  @Test
  void writesTheOutboxTimestampsInUtc() {
    service().process(100, 10, 1000);

    assertFalse(first("WITH page AS").matches("(?s).*now\\(\\)(?! AT TIME ZONE).*"),
        "the consumer compares these against UTC, and PgJDBC takes the session zone from the JVM");
  }

  // --- fixtures -------------------------------------------------------------------------------

  private VectorReindexService service() {
    return service(false);
  }

  private VectorReindexService service(boolean filtered) {
    ConnectionProvider cp = mock(ConnectionProvider.class);
    try {
      int[] chunk = { 0 };
      when(cp.getPreparedStatement(anyString())).thenAnswer(invocation -> {
        String sql = invocation.getArgument(0);
        log.add(sql);
        List<Object> captured = new ArrayList<>();
        parameters.add(captured);
        ResultSet rows = rowsFor(sql, filtered, chunk);
        PreparedStatement statement = mock(PreparedStatement.class);
        when(statement.executeQuery()).thenReturn(rows);
        when(statement.executeUpdate()).thenReturn(
            sql.contains("status = 'PROCESSING'") ? claimed : 1);
        org.mockito.Mockito.doAnswer(i -> capture(captured, i)).when(statement).setString(anyInt(), anyString());
        org.mockito.Mockito.doAnswer(i -> capture(captured, i)).when(statement).setInt(anyInt(), anyInt());
        org.mockito.Mockito.doAnswer(i -> capture(captured, i)).when(statement).setLong(anyInt(), anyLong());
        return statement;
      });
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
    return new VectorReindexService(cp, new VectorOutboxService.TransactionBoundary() {
      @Override
      public void commit() {
        log.add("commit");
      }

      @Override
      public void rollback() {
        log.add("rollback");
      }
    });
  }

  private static Object capture(List<Object> captured, org.mockito.invocation.InvocationOnMock i) {
    int index = i.getArgument(0);
    while (captured.size() < index) {
      captured.add(null);
    }
    captured.set(index - 1, i.getArgument(1));
    return null;
  }

  private ResultSet rowsFor(String sql, boolean filtered, int[] chunk) throws Exception {
    ResultSet rs = mock(ResultSet.class);
    if (sql.startsWith("SELECT r.etarc_vector_reindex_req_id") || sql.contains("FROM etarc_vector_reindex_req r")) {
      when(rs.next()).thenReturn(true);
      when(rs.getString(1)).thenReturn("REQ1");
      when(rs.getString(2)).thenReturn("SRC1");
      when(rs.getString(3)).thenReturn(null);
      when(rs.getLong(4)).thenReturn(0L);
      when(rs.getObject(5)).thenReturn(totalCount);
      when(rs.getLong(5)).thenReturn(totalCount == null ? 0L : totalCount);
      when(rs.getLong(6)).thenReturn(1L);
      when(rs.getString(7)).thenReturn("C_BPartner");
      when(rs.getString(8)).thenReturn("C_BPartner_ID");
      when(rs.getString(9)).thenReturn("AD_Client_ID");
      when(rs.getString(10)).thenReturn("AD_Org_ID");
      when(rs.getString(11)).thenReturn(filtered ? "IsActive" : null);
      when(rs.getString(12)).thenReturn(filtered ? "Y" : null);
    } else if (sql.startsWith("WITH page AS")) {
      int size = chunk[0] < chunkSizes.length ? chunkSizes[chunk[0]] : 0;
      chunk[0]++;
      if (size < 0) {
        when(rs.next()).thenThrow(new java.sql.SQLException("page failed"));
      } else {
        when(rs.next()).thenReturn(true);
        when(rs.getInt(1)).thenReturn(size);
        when(rs.getString(2)).thenReturn("key-" + chunk[0]);
      }
    } else if (sql.contains("count(*)") || sql.contains("COUNT(*)")) {
      when(rs.next()).thenReturn(true);
      when(rs.getLong(1)).thenReturn(backlog);
    } else {
      when(rs.next()).thenReturn(false);
    }
    return rs;
  }

  // --- reading the log ------------------------------------------------------------------------

  private int indexOf(String prefix) {
    for (int i = 0; i < log.size(); i++) {
      if (log.get(i).startsWith(prefix)) {
        return i;
      }
    }
    return -1;
  }

  private int lastIndexOf(String prefix) {
    for (int i = log.size() - 1; i >= 0; i--) {
      if (log.get(i).startsWith(prefix)) {
        return i;
      }
    }
    return -1;
  }

  private int count(String prefix) {
    return (int) log.stream().filter(s -> s.startsWith(prefix)).count();
  }

  private String first(String prefix) {
    int index = indexOf(prefix);
    assertTrue(index >= 0, "no statement starts with " + prefix);
    return log.get(index);
  }

  /** The parameters of every statement starting with the prefix, in order. */
  private List<List<Object>> parametersOf(String prefix) {
    List<List<Object>> found = new ArrayList<>();
    int statementIndex = 0;
    for (String entry : log) {
      if ("commit".equals(entry) || "rollback".equals(entry)) {
        continue;
      }
      if (entry.startsWith(prefix)) {
        found.add(parameters.get(statementIndex));
      }
      statementIndex++;
    }
    return found;
  }
}
