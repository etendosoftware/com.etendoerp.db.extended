package com.etendoerp.db.extended.vector;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.ConnectException;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;
import org.openbravo.database.ConnectionProvider;

/**
 * Exercises how the dispatcher groups events and where it closes transactions.
 *
 * <p>These are the guarantees the delivery path rests on and none of them is visible in the data
 * afterwards: a chunk that was split wrongly, a claim that was never made durable or a failure that
 * could not be recorded all look the same once the run is over.</p>
 *
 * <p>The bounds of {@link VectorDeliveryRetryPolicy#delay(int, Throwable)} are asserted directly,
 * since a run through the service only ever reaches the first few attempts.</p>
 *
 * @covers com.etendoerp.db.extended.vector.VectorOutboxService
 * @covers com.etendoerp.db.extended.vector.VectorDeliveryRetryPolicy
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

  @Test
  void retiresEventsQueuedUnderAnOlderConfiguration() {
    RecordingConsumer consumer = new RecordingConsumer(25);

    // The source moved to version 2 after these events were queued under version 1.
    int processed = service(3, consumer, 2L).processPending(100);

    assertEquals(0, processed);
    assertTrue(consumer.consumed.isEmpty(), "content built under rules that no longer apply must not be indexed");
    assertTrue(log.isEmpty(), "nothing is claimed, so no transaction is opened for them either");
  }

  // ---------------------------------------------------------------- retry of transient failures

  @Test
  void requeuesAThrottledEventWithBackoffInsteadOfFailingIt() {
    RecordingConsumer consumer = new RecordingConsumer(25);
    consumer.prepareFailure = new VectorProviderException(429, null, "throttled");

    int processed = service(1, consumer).processPending(100);

    assertEquals(0, processed);
    Executed retry = single(RETRY_UPDATE);
    assertEquals(60L, retry.parameters.get(2), "the first retry waits the base backoff");
    assertEquals("event-0", retry.parameters.get(3));
    assertTrue(executed(FAILED_UPDATE).isEmpty(), "a 429 is the provider asking to slow down, not a lost event");
    assertTrue(single(CLAIM).sql.contains("attempt_count = attempt_count + 1"),
        "the attempt is counted when the event is claimed, so the retry spends budget");
    assertTrue(!retry.sql.contains("attempt_count = 0"), "a retry must not give the budget back");
  }

  @Test
  void honoursRetryAfterWhenItIsLongerThanTheBackoff() {
    RecordingConsumer consumer = new RecordingConsumer(25);
    consumer.prepareFailure = new VectorProviderException(429, Duration.ofSeconds(300), "throttled");

    service(1, consumer).processPending(100);

    assertEquals(300L, single(RETRY_UPDATE).parameters.get(2),
        "retrying before the provider said it would accept again only spends another attempt");
  }

  @Test
  void doublesTheBackoffOnEveryAttempt() {
    RecordingConsumer consumer = new RecordingConsumer(25);
    consumer.prepareFailure = new VectorProviderException(503, null, "unavailable");
    attempts = 2;

    service(1, consumer).processPending(100);

    assertEquals(120L, single(RETRY_UPDATE).parameters.get(2));
  }

  @Test
  void treatsATimeoutAsTransient() {
    RecordingConsumer consumer = new RecordingConsumer(25);
    consumer.prepareFailure = new VectorException(VectorErrorCode.VECTOR_EMBEDDING_OPERATION_FAILED,
        "Could not request an OpenAI embedding.", new SocketTimeoutException("Read timed out"));

    service(1, consumer).processPending(100);

    single(RETRY_UPDATE);
    assertTrue(executed(FAILED_UPDATE).isEmpty());
  }

  @Test
  void retriesATransientFailureOfASingleEventToo() {
    RecordingConsumer consumer = new RecordingConsumer(25);
    consumer.failOn = "event-1";
    consumer.consumeFailure = new VectorProviderException(500, null, "server error");

    int processed = service(2, consumer).processPending(100);

    assertEquals(1, processed);
    assertEquals("event-1", single(RETRY_UPDATE).parameters.get(3));
  }

  @Test
  void treatsARequestTimeoutAnswerAsTransient() {
    RecordingConsumer consumer = new RecordingConsumer(25);
    consumer.prepareFailure = new VectorProviderException(408, null, "request timeout");

    service(1, consumer).processPending(100);

    single(RETRY_UPDATE);
    assertTrue(executed(FAILED_UPDATE).isEmpty(), "a 408 is the provider giving up on a slow request, not a bad one");
  }

  @Test
  void treatsAMalformedEndpointAsPermanentAlthoughItIsAnIoFailure() {
    RecordingConsumer consumer = new RecordingConsumer(25);
    consumer.prepareFailure = new VectorException(VectorErrorCode.VECTOR_EMBEDDING_OPERATION_FAILED,
        "Could not request an OpenAI embedding.", new MalformedURLException("no protocol: api.example"));

    service(1, consumer).processPending(100);

    single(FAILED_UPDATE);
    assertTrue(executed(RETRY_UPDATE).isEmpty(), "a misconfigured endpoint does not fix itself between runs");
  }

  @Test
  void retriesEveryEventOfAThrottledChunkWithItsOwnBudget() {
    RecordingConsumer consumer = new RecordingConsumer(25);
    consumer.prepareFailure = new VectorProviderException(429, null, "throttled");

    service(3, consumer).processPending(100);

    List<Object> retried = new ArrayList<>();
    for (Executed retry : executed(RETRY_UPDATE)) {
      retried.add(retry.parameters.get(3));
    }
    assertEquals(List.of("event-0", "event-1", "event-2"), retried,
        "the chunk shares one request, so one 429 sends every event of it back to the queue");
    List<Object> budgets = new ArrayList<>();
    for (Executed budget : executed(BUDGET_QUERY)) {
      budgets.add(budget.parameters.get(2));
    }
    assertEquals(List.of("event-0", "event-1", "event-2"), budgets,
        "each event is retried against its own attempt count, not the chunk's");
  }

  @Test
  void onlyRecordsTheOutcomeOfAnEventItStillHoldsInProcessing() {
    RecordingConsumer consumer = new RecordingConsumer(25);
    consumer.failOn = "event-1";
    consumer.consumeFailure = new VectorProviderException(503, null, "unavailable");

    service(3, consumer).processPending(100);
    consumer.failOn = "event-2";
    consumer.consumeFailure = new VectorProviderException(400, null, "bad payload");
    service(3, consumer).processPending(100);

    // A stale-recovery can hand the event to another node; overwriting its outcome would lose it.
    String guard = "WHERE etarc_vector_outbox_id = ? AND status = 'PROCESSING'";
    assertTrue(executed(RETRY_UPDATE).get(0).sql.contains(guard));
    assertTrue(executed(FAILED_UPDATE).get(0).sql.contains(guard));
    assertTrue(executed(DONE_UPDATE).get(0).sql.contains(guard));
  }

  @Test
  void failsTheEventOnceTheRetryLimitIsExhausted() {
    RecordingConsumer consumer = new RecordingConsumer(25);
    consumer.prepareFailure = new VectorProviderException(429, null, "throttled");
    attempts = 3;
    retryLimit = 3;

    service(1, consumer).processPending(100);

    assertTrue(executed(RETRY_UPDATE).isEmpty(), "the provider retry limit bounds transient failures too");
    Executed failed = single(FAILED_UPDATE);
    assertTrue(((String) failed.parameters.get(1)).contains("throttled"),
        "the administrator still has to see what kept failing");
  }

  @Test
  void failsARejectedRequestImmediately() {
    RecordingConsumer consumer = new RecordingConsumer(25);
    consumer.prepareFailure = new VectorProviderException(400, null, "bad payload");

    service(1, consumer).processPending(100);

    single(FAILED_UPDATE);
    assertTrue(executed(RETRY_UPDATE).isEmpty(), "sending the same payload again cannot succeed");
    assertTrue(executed(BUDGET_QUERY).isEmpty(), "a permanent failure needs no retry budget");
  }

  @Test
  void treatsAnAuthenticationFailureAsPermanent() {
    RecordingConsumer consumer = new RecordingConsumer(25);
    consumer.prepareFailure = new VectorProviderException(401, null, "unauthorized");

    service(1, consumer).processPending(100);

    single(FAILED_UPDATE);
    assertTrue(executed(RETRY_UPDATE).isEmpty(),
        "a wrong API key does not fix itself; it is requeued once the key is corrected");
  }

  @Test
  void treatsAForbiddenAnswerAsPermanent() {
    RecordingConsumer consumer = new RecordingConsumer(25);
    consumer.prepareFailure = new VectorProviderException(403, null, "forbidden");

    service(1, consumer).processPending(100);

    single(FAILED_UPDATE);
    assertTrue(executed(RETRY_UPDATE).isEmpty(), "a key without permission does not gain it by waiting");
    assertTrue(executed(BUDGET_QUERY).isEmpty(), "a permanent failure needs no retry budget");
  }

  @Test
  void classifiesTheEdgesOfTheClientAndServerErrorRanges() {
    assertFalse(VectorDeliveryRetryPolicy.isTransient(new VectorProviderException(404, null, "not found")));
    assertFalse(VectorDeliveryRetryPolicy.isTransient(new VectorProviderException(499, null, "client closed")),
        "the last 4xx is still the request's fault");
    assertTrue(VectorDeliveryRetryPolicy.isTransient(new VectorProviderException(500, null, "server error")),
        "the first 5xx is the provider's");
    assertTrue(VectorDeliveryRetryPolicy.isTransient(new VectorProviderException(599, null, "network timeout")));
  }

  @Test
  void treatsAnUnreachableProviderAsTransientWhereverItSitsInTheCauseChain() {
    assertTrue(VectorDeliveryRetryPolicy.isTransient(wrapped(new ConnectException("refused"))),
        "a refused connection is the network, not the request");
    assertTrue(VectorDeliveryRetryPolicy.isTransient(wrapped(new UnknownHostException("api.example"))),
        "a DNS hiccup passes on its own");
  }

  @Test
  void treatsAFailureThatIsNotAProviderCallAsPermanent() {
    assertFalse(VectorDeliveryRetryPolicy.isTransient(
        new VectorException(VectorErrorCode.VECTOR_EMBEDDING_OPERATION_FAILED, "no content column")),
        "a configuration problem carries neither a status nor an I/O cause");
    assertFalse(VectorDeliveryRetryPolicy.isTransient(wrapped(new SQLException("relation does not exist"))),
        "a database error is not something a later attempt repairs");
  }

  @Test
  void failsATransientFailureWhoseRetryBudgetCannotBeFound() {
    RecordingConsumer consumer = new RecordingConsumer(25);
    consumer.prepareFailure = new VectorProviderException(503, null, "unavailable");
    budgetRowMissing = true;

    assertDoesNotThrow(() -> service(1, consumer).processPending(100));

    assertTrue(executed(RETRY_UPDATE).isEmpty(), "an event that is gone has nothing to retry");
    assertTrue(((String) single(FAILED_UPDATE).parameters.get(1)).startsWith("Retry limit of 3 attempts exhausted"),
        "with no row the default limit is taken as spent");
  }

  @Test
  void reportsAnUnreadableRetryBudgetAsAnOutboxFailure() {
    RecordingConsumer consumer = new RecordingConsumer(25);
    consumer.prepareFailure = new VectorProviderException(503, null, "unavailable");
    budgetFailure = new SQLException("connection lost");

    VectorException failure = assertThrows(VectorException.class,
        () -> service(1, consumer).processPending(100));

    assertEquals(VectorErrorCode.VECTOR_OUTBOX_OPERATION_FAILED, failure.getCode());
    assertTrue(executed(RETRY_UPDATE).isEmpty() && executed(FAILED_UPDATE).isEmpty(),
        "without the budget no outcome may be guessed");
  }

  @Test
  void onlyLoadsEventsWhoseBackoffHasElapsed() {
    service(1, new RecordingConsumer(25)).processPending(100);

    assertTrue(single(PENDING_QUERY).sql.contains(
        "(o.next_attempt_at IS NULL OR o.next_attempt_at <= now() AT TIME ZONE 'UTC')"),
        "an event waiting for its backoff must not be delivered early, and NULL means now");
  }

  // ---------------------------------------------------------------- requeue

  @Test
  void requeuesEveryFailedEventWhenNoScopeIsGiven() {
    service(0, new RecordingConsumer(25)).requeueFailed(10);

    Executed requeue = single(REQUEUE_UPDATE);
    assertTrue(requeue.sql.contains("next_attempt_at = NULL"), "a requeued event is eligible at once");
    assertTrue(!requeue.sql.contains("ad_client_id = ?") && !requeue.sql.contains("etarc_vector_source_id = ?"));
    assertEquals("FAILED", requeue.parameters.get(1));
    assertEquals(10, requeue.parameters.get(2));
  }

  @Test
  void scopesTheRequeueToAClientAndASource() {
    service(0, new RecordingConsumer(25)).requeueFailed(10, "client-1", "source-1");

    Executed requeue = single(REQUEUE_UPDATE);
    assertTrue(requeue.sql.contains("ad_client_id = ?") && requeue.sql.contains("etarc_vector_source_id = ?"));
    assertEquals(List.of("FAILED", "client-1", "source-1", 10), List.copyOf(requeue.parameters.values()));
  }

  @Test
  void scopesTheRequeueToASourceAlone() {
    service(0, new RecordingConsumer(25)).requeueFailed(10, null, "source-1");

    Executed requeue = single(REQUEUE_UPDATE);
    assertTrue(!requeue.sql.contains("ad_client_id = ?"));
    assertEquals(List.of("FAILED", "source-1", 10), List.copyOf(requeue.parameters.values()));
  }

  @Test
  void scopesTheRequeueToAClientAlone() {
    service(0, new RecordingConsumer(25)).requeueFailed(10, "client-1", null);

    Executed requeue = single(REQUEUE_UPDATE);
    assertTrue(requeue.sql.contains("ad_client_id = ?"));
    assertTrue(!requeue.sql.contains("etarc_vector_source_id = ?"), "no source was selected");
    assertEquals(List.of("FAILED", "client-1", 10), List.copyOf(requeue.parameters.values()));
  }

  @Test
  void requeuesStaleProcessingEventsWithinTheirBudgetWithoutResettingIt() {
    service(0, new RecordingConsumer(25)).requeueStaleProcessing(Duration.ofMinutes(15), 50);

    Executed requeue = single(REQUEUE_UPDATE);
    assertEquals(List.of("PROCESSING", 900L, VectorOutboxService.DEFAULT_RETRY_LIMIT, 50),
        List.copyOf(requeue.parameters.values()),
        "the age and the retry limit have to bind after the status, in the order the SQL reads them");
    assertTrue(requeue.sql.contains("next_attempt_at = NULL"), "a recovered event is eligible at once");
    assertTrue(!requeue.sql.contains("attempt_count = 0"),
        "recovery must not hand back the budget, or an event that kills the run loops forever");
    assertTrue(requeue.sql.contains("attempt_count < "), "only events with attempts left are recovered");
  }

  @Test
  void leavesAnEventAloneWhenItsOutcomeFindsItNoLongerProcessing() {
    // Stale recovery or another node took the event over, so every guarded outcome matches no row.
    outcomeRowCount = 0;
    RecordingConsumer consumer = new RecordingConsumer(25);
    consumer.failOn = "event-1";
    consumer.consumeFailure = new VectorProviderException(503, null, "unavailable");
    service(3, consumer).processPending(100);
    consumer.failOn = "event-2";
    consumer.consumeFailure = new VectorProviderException(400, null, "bad payload");

    assertDoesNotThrow(() -> service(3, consumer).processPending(100),
        "losing the event to its new owner is expected, not an error that aborts the run");

    assertEquals(List.of("event-0", "event-2", "event-0", "event-1"), consumer.consumed,
        "the rest of the chunk is still delivered after an outcome that matched nothing");
    assertEquals(4, executed(DONE_UPDATE).size());
    assertEquals(1, executed(RETRY_UPDATE).size(), "an unmatched retry is not written a second time");
    assertEquals(1, executed(FAILED_UPDATE).size(),
        "an unmatched retry must not fall back to failing the event either");
    assertEquals(RETRY_UPDATE, lastWriteFor("event-1", 0), "nothing is written for event-1 after its retry");
    assertEquals(FAILED_UPDATE, lastWriteFor("event-2", 1), "nothing is written for event-2 after its failure");
  }

  // ---------------------------------------------------------------- retry delay bounds

  @Test
  void delayDoublesTheBaseBackoffPerAttempt() {
    Throwable io = new SocketTimeoutException("timeout");

    assertEquals(Duration.ofSeconds(60), VectorDeliveryRetryPolicy.delay(1, io));
    assertEquals(Duration.ofSeconds(120), VectorDeliveryRetryPolicy.delay(2, io));
    assertEquals(Duration.ofSeconds(480), VectorDeliveryRetryPolicy.delay(4, io));
  }

  @Test
  void delayCapsTheBackoffAtOneHour() {
    assertEquals(Duration.ofHours(1), VectorDeliveryRetryPolicy.delay(7, null),
        "60s * 2^6 = 64 minutes, which is past the one-hour ceiling");
  }

  @Test
  void delayCapsTheExponentSoAHugeAttemptDoesNotOverflow() {
    assertEquals(Duration.ofHours(1), VectorDeliveryRetryPolicy.delay(Integer.MAX_VALUE, null),
        "an unbounded shift would overflow into a negative or zero delay");
  }

  @Test
  void delayKeepsTheBackoffWhenRetryAfterIsShorter() {
    assertEquals(Duration.ofSeconds(120), VectorDeliveryRetryPolicy.delay(2,
        new VectorProviderException(429, Duration.ofSeconds(10), "throttled")),
        "the provider's minimum never shortens the backoff");
  }

  @Test
  void delayHonoursARetryAfterLongerThanTheBackoff() {
    assertEquals(Duration.ofMinutes(90), VectorDeliveryRetryPolicy.delay(1,
        new VectorProviderException(429, Duration.ofMinutes(90), "throttled")),
        "Retry-After may exceed the backoff ceiling: only its own 24h cap applies");
  }

  @Test
  void delayCapsRetryAfterAtTwentyFourHours() {
    assertEquals(Duration.ofHours(24), VectorDeliveryRetryPolicy.delay(1,
        new VectorProviderException(429, Duration.ofDays(30), "throttled")),
        "a hostile or malformed Retry-After must not park an event for a month");
  }

  @Test
  void delayReadsRetryAfterFromDeeperInTheCauseChain() {
    Throwable wrapped = new VectorException(VectorErrorCode.VECTOR_EMBEDDING_OPERATION_FAILED, "wrapped",
        new VectorProviderException(503, Duration.ofSeconds(900), "unavailable"));

    assertEquals(Duration.ofSeconds(900), VectorDeliveryRetryPolicy.delay(1, wrapped));
  }

  // ---------------------------------------------------------------- fixtures

  private static final String PENDING_QUERY = "o.event_type";
  private static final String CLAIM = "SET status = 'PROCESSING'";
  private static final String BUDGET_QUERY = "SELECT attempt_count";
  private static final String RETRY_UPDATE = "SET status = 'PENDING', last_error = ?, next_attempt_at";
  private static final String FAILED_UPDATE = "SET status = 'FAILED', last_error = ?";
  private static final String DONE_UPDATE = "SET status = 'DONE'";
  private static final String REQUEUE_UPDATE = "SET status = 'PENDING', last_error = NULL";

  /** Rows matched by an outcome guarded on PROCESSING; zero means the event changed hands. */
  private int outcomeRowCount = 1;
  /** The retry-budget query finds no row, as when the event was deleted meanwhile. */
  private boolean budgetRowMissing;
  /** What the retry-budget query throws, or {@code null} when it answers. */
  private SQLException budgetFailure;
  /** Attempts the event has used, counting the one that just failed. */
  private int attempts = 1;
  private int retryLimit = 3;
  /** Every statement the service prepared, in order, with the parameters bound to it. */
  private final List<Executed> statements = new ArrayList<>();

  private static final class Executed {
    private final String sql;
    private final Map<Integer, Object> parameters = new TreeMap<>();

    private Executed(String sql) { this.sql = sql; }
  }

  private List<Executed> executed(String fragment) {
    List<Executed> matches = new ArrayList<>();
    for (Executed statement : statements) {
      if (statement.sql.contains(fragment)) {
        matches.add(statement);
      }
    }
    return matches;
  }

  private Executed single(String fragment) {
    List<Executed> matches = executed(fragment);
    assertEquals(1, matches.size(), "expected exactly one statement containing: " + fragment);
    return matches.get(0);
  }

  /**
   * The outcome fragment of the last write that bound the event, within its {@code run}-th delivery
   * (each run starts at its pending-events query).
   */
  private String lastWriteFor(String eventId, int run) {
    int seen = -1;
    String last = null;
    for (Executed statement : statements) {
      if (statement.sql.contains(PENDING_QUERY)) {
        seen++;
      } else if (seen == run && !statement.sql.contains(BUDGET_QUERY)
          && statement.parameters.containsValue(eventId)) {
        last = statement.sql;
      }
    }
    for (String outcome : List.of(DONE_UPDATE, RETRY_UPDATE, FAILED_UPDATE, CLAIM)) {
      if (last != null && last.contains(outcome)) {
        return outcome;
      }
    }
    return last;
  }

  private static VectorException wrapped(Throwable cause) {
    return new VectorException(VectorErrorCode.VECTOR_EMBEDDING_OPERATION_FAILED, "wrapped", cause);
  }

  private VectorOutboxService service(int pendingEvents, VectorOutboxConsumer consumer) {
    return service(pendingEvents, consumer, 1L);
  }

  private VectorOutboxService service(int pendingEvents, VectorOutboxConsumer consumer, long sourceVersion) {
    try {
      ConnectionProvider connectionProvider = mock(ConnectionProvider.class);
      ResultSet pending = pending(pendingEvents, sourceVersion);
      when(connectionProvider.getPreparedStatement(anyString())).thenAnswer(
          invocation -> statement(invocation.getArgument(0), pending));

      return new VectorOutboxService(connectionProvider, List.of(consumer), new VectorOutboxService.TransactionBoundary() {
        @Override public void commit() { log.add("commit"); }

        @Override public void rollback() { log.add("rollback"); }
      });
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private PreparedStatement statement(String sql, ResultSet pending) throws Exception {
    Executed executed = new Executed(sql);
    statements.add(executed);
    PreparedStatement statement = mock(PreparedStatement.class);
    Answer<Void> bind = invocation -> {
      executed.parameters.put(invocation.getArgument(0), invocation.getArgument(1));
      return null;
    };
    doAnswer(bind).when(statement).setString(anyInt(), any());
    doAnswer(bind).when(statement).setInt(anyInt(), anyInt());
    doAnswer(bind).when(statement).setLong(anyInt(), anyLong());
    when(statement.executeUpdate()).thenReturn(sql.contains("AND status = 'PROCESSING'") ? outcomeRowCount : 1);
    if (sql.contains(BUDGET_QUERY) && budgetFailure != null) {
      when(statement.executeQuery()).thenThrow(budgetFailure);
    } else if (sql.contains(BUDGET_QUERY)) {
      ResultSet budget = mock(ResultSet.class);
      when(budget.next()).thenReturn(!budgetRowMissing, false);
      when(budget.getInt(1)).thenReturn(attempts);
      when(budget.getInt(2)).thenReturn(retryLimit);
      when(statement.executeQuery()).thenReturn(budget);
    } else {
      when(statement.executeQuery()).thenReturn(pending);
    }
    return statement;
  }

  private ResultSet pending(int pendingEvents, long sourceVersion) throws Exception {
    ResultSet pending = mock(ResultSet.class);
    if (pendingEvents == 0) {
      when(pending.next()).thenReturn(false);
      return pending;
    }
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
    when(pending.getLong(10)).thenReturn(sourceVersion);
    when(pending.getString(4)).thenReturn("namespace");
    when(pending.getString(5)).thenReturn("record");
    when(pending.getString(6)).thenReturn("UPDATE");
    return pending;
  }

  private final class RecordingConsumer implements VectorOutboxConsumer {
    private final int batchSize;
    private final List<Integer> preparedSizes = new ArrayList<>();
    private final List<String> consumed = new ArrayList<>();
    private String failOn;
    private boolean failPrepare;
    private RuntimeException prepareFailure;
    private RuntimeException consumeFailure;

    private RecordingConsumer(int batchSize) { this.batchSize = batchSize; }

    @Override public String namespace() { return "namespace"; }

    @Override public int batchSize(VectorOutboxEvent event) { return batchSize; }

    @Override public void prepare(List<VectorOutboxEvent> events) {
      log.add("prepare");
      if (prepareFailure != null) {
        throw prepareFailure;
      }
      if (failPrepare) {
        throw new IllegalStateException("the provider is unreachable");
      }
      preparedSizes.add(events.size());
    }

    @Override public void consume(VectorOutboxEvent event) throws Exception {
      log.add("consume:" + event.getId());
      if (event.getId().equals(failOn)) {
        throw consumeFailure != null ? consumeFailure : new IllegalStateException("could not store the vector");
      }
      consumed.add(event.getId());
    }
  }
}
