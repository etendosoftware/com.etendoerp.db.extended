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
 * Exercises how the dispatcher groups events and where it closes transactions.
 *
 * <p>These are the guarantees the delivery path rests on and none of them is visible in the data
 * afterwards: a chunk that was split wrongly, a claim that was never made durable or a failure that
 * could not be recorded all look the same once the run is over.</p>
 */
class VectorOutboxServiceTest {

  /** Records every step in order, so assertions can be about sequence and not only about counts. */
  private final List<String> log = new ArrayList<>();

  @Test
  void deliversTheWholeGroupInOneChunkWhenTheBatchIsLargeEnough() {
    RecordingConsumer consumer = new RecordingConsumer(25);

    int processed = service(5, consumer).processPending(100);

    assertEquals(5, processed);
    assertEquals(List.of(5), consumer.preparedSizes, "five events of one source must be resolved in one call");
  }

  @Test
  void splitsTheGroupIntoChunksOfTheConsumerBatchSize() {
    RecordingConsumer consumer = new RecordingConsumer(2);

    int processed = service(5, consumer).processPending(100);

    assertEquals(5, processed);
    assertEquals(List.of(2, 2, 1), consumer.preparedSizes, "the chunk must follow what the consumer can resolve at once");
  }

  @Test
  void makesTheClaimDurableBeforeTheConsumerRuns() {
    RecordingConsumer consumer = new RecordingConsumer(25);

    service(2, consumer).processPending(100);

    assertEquals("commit", log.get(0),
        "the PROCESSING marker has to be committed before a consumer that reaches an external provider");
    assertTrue(log.indexOf("commit") < log.indexOf("prepare"));
  }

  @Test
  void rollsBackBeforeRecordingAConsumerFailure() {
    RecordingConsumer consumer = new RecordingConsumer(25);
    consumer.failOn = "event-1";

    int processed = service(2, consumer).processPending(100);

    assertEquals(1, processed, "the healthy event of the chunk still has to be delivered");
    int failure = log.indexOf("consume:event-1");
    assertEquals("rollback", log.get(failure + 1),
        "a consumer failing on a database error leaves the transaction aborted, so it must be recovered "
            + "before the failure can be written");
  }

  @Test
  void failsTheWholeChunkWhenTheSharedResolutionFails() {
    RecordingConsumer consumer = new RecordingConsumer(25);
    consumer.failPrepare = true;

    int processed = service(3, consumer).processPending(100);

    assertEquals(0, processed);
    assertTrue(consumer.consumed.isEmpty(), "nothing can be delivered when what the chunk shares could not be resolved");
    assertTrue(log.contains("rollback"));
  }

  // ---------------------------------------------------------------- fixtures

  private VectorOutboxService service(int pendingEvents, VectorOutboxConsumer consumer) {
    try {
      ConnectionProvider connectionProvider = mock(ConnectionProvider.class);
      PreparedStatement statement = mock(PreparedStatement.class);
      ResultSet pending = mock(ResultSet.class);
      when(connectionProvider.getPreparedStatement(anyString())).thenReturn(statement);
      when(statement.executeQuery()).thenReturn(pending);
      when(statement.executeUpdate()).thenReturn(1);

      Boolean[] hasNext = new Boolean[pendingEvents + 1];
      String[] ids = new String[pendingEvents];
      for (int i = 0; i < pendingEvents; i++) {
        hasNext[i] = Boolean.TRUE;
        ids[i] = "event-" + i;
      }
      hasNext[pendingEvents] = Boolean.FALSE;
      when(pending.next()).thenReturn(hasNext[0], java.util.Arrays.copyOfRange(hasNext, 1, hasNext.length));
      when(pending.getString(1)).thenReturn(ids[0], java.util.Arrays.copyOfRange(ids, 1, ids.length));
      when(pending.getString(2)).thenReturn("source");
      when(pending.getLong(3)).thenReturn(1L);
      when(pending.getString(4)).thenReturn("namespace");
      when(pending.getString(5)).thenReturn("record");
      when(pending.getString(6)).thenReturn("UPDATE");

      return new VectorOutboxService(connectionProvider, List.of(consumer), new VectorOutboxService.TransactionBoundary() {
        @Override public void commit() { log.add("commit"); }

        @Override public void rollback() { log.add("rollback"); }
      });
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private final class RecordingConsumer implements VectorOutboxConsumer {
    private final int batchSize;
    private final List<Integer> preparedSizes = new ArrayList<>();
    private final List<String> consumed = new ArrayList<>();
    private String failOn;
    private boolean failPrepare;

    private RecordingConsumer(int batchSize) { this.batchSize = batchSize; }

    @Override public String namespace() { return "namespace"; }

    @Override public int batchSize(VectorOutboxEvent event) { return batchSize; }

    @Override public void prepare(List<VectorOutboxEvent> events) throws Exception {
      log.add("prepare");
      if (failPrepare) {
        throw new IllegalStateException("the provider is unreachable");
      }
      preparedSizes.add(events.size());
    }

    @Override public void consume(VectorOutboxEvent event) throws Exception {
      log.add("consume:" + event.getId());
      if (event.getId().equals(failOn)) {
        throw new IllegalStateException("could not store the vector");
      }
      consumed.add(event.getId());
    }
  }
}
