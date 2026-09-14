/*
 *************************************************************************
 * The contents of this file are subject to the Etendo License
 * (the "License"), you may not use this file except in compliance with
 * the License.
 * You may obtain a copy of the License at
 * https://github.com/etendosoftware/etendo_core/blob/main/legal/Etendo_license.txt
 * Software distributed under the License is distributed on an
 * "AS IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing rights
 * and limitations under the License.
 * All portions are Copyright © 2026 FUTIT SERVICES, S.L
 * All Rights Reserved.
 * Contributor(s): Futit Services S.L.
 *************************************************************************
 */
package com.etendoerp.db.extended.vector;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.openbravo.database.ConnectionProvider;

/**
 * Delivers pending vector source events to namespace-owned consumers.
 *
 * <p>The service makes delivery at-least-once. Consumers must therefore make indexing idempotent,
 * normally by upserting with {@link VectorOutboxEvent#getRecordId()} as their external key.</p>
 */
public class VectorOutboxService {
  private static final int MAX_ERROR_LENGTH = 2000;

  private static final String PENDING_EVENTS_SQL =
      "SELECT o.etarc_vector_outbox_id, o.etarc_vector_source_id, o.config_version, s.namespace, o.record_id, "
          + "o.event_type, o.ad_column_id, o.ad_client_id, o.ad_org_id, s.config_version "
          + "FROM etarc_vector_outbox o "
          + "JOIN etarc_vector_source s ON s.etarc_vector_source_id = o.etarc_vector_source_id "
          + "WHERE o.isactive = 'Y' AND o.status = 'PENDING' "
          + "AND NOT EXISTS (SELECT 1 FROM etarc_vector_outbox newer WHERE newer.etarc_vector_source_id = o.etarc_vector_source_id "
          + "AND newer.record_id = o.record_id AND newer.status = 'PENDING' AND (newer.created > o.created OR (newer.created = o.created AND newer.etarc_vector_outbox_id > o.etarc_vector_outbox_id))) "
          + "AND NOT EXISTS (SELECT 1 FROM etarc_vector_outbox processing WHERE processing.etarc_vector_source_id = o.etarc_vector_source_id "
          + "AND processing.record_id = o.record_id AND processing.status = 'PROCESSING') "
          // SKIP LOCKED keeps concurrent nodes from all fetching the same rows: without it every
          // node but one wastes its whole batch losing the claim compare-and-swap. The locks last
          // until the first chunk commits, after which correctness rests on that compare-and-swap
          // and on the PROCESSING guard above, which is what actually prevents double delivery.
          + "ORDER BY o.created, o.etarc_vector_outbox_id LIMIT ? FOR UPDATE OF o SKIP LOCKED";

  /** Retry budget used when the source has no provider, matching the AD default of RETRY_LIMIT. */
  static final int DEFAULT_RETRY_LIMIT = 3;
  /** Resolves the retry budget of the event's source provider. Takes one parameter: the fallback. */
  private static final String RETRY_LIMIT_SQL =
      "COALESCE((SELECT p.retry_limit FROM etarc_vector_source s "
          + "JOIN etarc_vector_embed_provider p "
          + "ON p.etarc_vector_embed_provider_id = s.etarc_vector_embed_provider_id "
          + "WHERE s.etarc_vector_source_id = etarc_vector_outbox.etarc_vector_source_id), ?)";

  private final ConnectionProvider connectionProvider;
  private final VectorOutboxConsumerResolver consumerResolver;
  private final TransactionBoundary transactionBoundary;

  /**
   * Closes the unit of work around a single event.
   *
   * <p>A rollback path is required, not optional: when a consumer fails with a database error the
   * JDBC transaction is left aborted, and every later statement on that connection is rejected with
   * {@code current transaction is aborted}. Without recovering first, the service cannot even record
   * why the event failed.</p>
   */
  public interface TransactionBoundary {
    /** Makes the work accumulated so far durable. */
    void commit();

    /** Discards it and leaves the connection usable again. */
    void rollback();
  }

  /**
   * Creates a service whose caller owns the transaction: no intermediate commit is issued and the
   * whole batch lands in a single unit of work. Use
   * {@link #VectorOutboxService(ConnectionProvider, Collection, Runnable)} whenever the consumers
   * perform remote calls.
   */
  public VectorOutboxService(ConnectionProvider connectionProvider,
      Collection<VectorOutboxConsumer> consumers) {
    this(connectionProvider, consumers, new TransactionBoundary() {
      @Override public void commit() {
        // No transaction boundary: the caller commits the whole batch.
      }

      @Override public void rollback() {
        // Idem: recovering the connection is the caller's responsibility.
      }
    });
  }

  /**
   * Creates a service that closes a transaction after claiming each event and again after its
   * terminal state is written.
   *
   * @param transactionBoundary
   *     commits the work accumulated so far. It runs twice per event: once after the claim, so the
   *     PROCESSING marker becomes visible to other nodes before the consumer starts, and once after
   *     the event reaches DONE or FAILED, so an interrupted run does not discard the deliveries
   *     already made.
   */
  public VectorOutboxService(ConnectionProvider connectionProvider,
      Collection<VectorOutboxConsumer> consumers, TransactionBoundary transactionBoundary) {
    this.connectionProvider = connectionProvider;
    this.consumerResolver = new VectorOutboxConsumerResolver(consumers);
    this.transactionBoundary = transactionBoundary;
  }

  /** Processes at most {@code maxEvents} events and returns the number successfully delivered. */
  public int processPending(int maxEvents) {
    if (maxEvents < 1) {
      throw new IllegalArgumentException("maxEvents must be positive");
    }
    // Events are grouped by source so that every chunk shares one provider, and the chunk size is
    // whatever that provider resolves in a single round trip. The chunk is also the transaction the
    // dispatcher holds, so the two stay aligned instead of being tuned independently.
    Map<String, List<VectorOutboxEvent>> bySource = new LinkedHashMap<>();
    for (VectorOutboxEvent event : loadPending(maxEvents)) {
      if (event.getConfigVersion() != event.getSourceConfigVersion()) {
        // The source was reconfigured after this event was queued. It used to be marked DONE, so a
        // configuration change in the middle of a large backfill silently reported success while
        // indexing nothing. SUPERSEDED is what actually happened, and it shows in the window.
        markSuperseded(event.getId());
        continue;
      }
      bySource.computeIfAbsent(event.getSourceId(), key -> new ArrayList<>()).add(event);
    }
    int processed = 0;
    for (List<VectorOutboxEvent> group : bySource.values()) {
      VectorOutboxConsumer consumer = consumerResolver.resolve(group.get(0).getNamespace());
      if (consumer == null) {
        continue;
      }
      int chunkSize = chunkSize(consumer, group.get(0));
      for (int start = 0; start < group.size(); start += chunkSize) {
        processed += deliver(consumer, group.subList(start, Math.min(group.size(), start + chunkSize)));
      }
    }
    return processed;
  }

  /**
   * Asks the consumer how many events it can resolve at once, falling back to one at a time.
   *
   * <p>Sizing the chunk can itself fail, typically because the source has no provider configured.
   * Delivering one by one then lets each event record that failure on its own instead of losing the
   * whole group to an exception raised before anything was even claimed.</p>
   */
  private int chunkSize(VectorOutboxConsumer consumer, VectorOutboxEvent event) {
    try {
      return Math.max(1, consumer.batchSize(event));
    } catch (Exception e) {
      return 1;
    }
  }

  /** Claims a chunk, resolves it in one go and then records the outcome of each event. */
  private int deliver(VectorOutboxConsumer consumer, List<VectorOutboxEvent> chunk) {
    List<VectorOutboxEvent> claimed = new ArrayList<>();
    for (VectorOutboxEvent event : chunk) {
      supersedeOlderPending(event);
      if (claim(event.getId())) {
        claimed.add(event);
      }
    }
    // The claims have to be durable before the consumer runs: consumers reach external providers,
    // so holding them in the same transaction would keep PROCESSING invisible to other nodes and
    // would roll back every delivery already made if the run is interrupted.
    transactionBoundary.commit();
    if (claimed.isEmpty()) {
      return 0;
    }
    try {
      consumer.prepare(claimed);
    } catch (Exception e) {
      // What prepare resolves is shared by the chunk, so its failure is every event's failure.
      transactionBoundary.rollback();
      for (VectorOutboxEvent event : claimed) {
        markFailed(event.getId(), e);
      }
      transactionBoundary.commit();
      return 0;
    }
    int processed = 0;
    for (VectorOutboxEvent event : claimed) {
      try {
        consumer.consume(event);
        markDone(event.getId());
        processed++;
      } catch (Exception e) {
        // A consumer failing on a database error leaves the transaction aborted, so markFailed
        // could not write and the event stayed PROCESSING with no trace of the cause.
        transactionBoundary.rollback();
        markFailed(event.getId(), e);
      }
      transactionBoundary.commit();
    }
    return processed;
  }

  /**
   * Requeues failed events, allowing an administrator to choose when a retry is attempted.
   *
   * <p>The attempt counter is reset: this is an explicit decision taken after correcting the
   * provider or source configuration, so the event is entitled to a full budget again.</p>
   */
  public int requeueFailed(int maxEvents) {
    return requeue("FAILED", maxEvents, null, true, false);
  }

  /**
   * Requeues events abandoned while processing for at least the supplied duration, as long as they
   * have attempts left.
   *
   * <p>Recovery used to be unconditional, so an event whose delivery kills the transaction was
   * resurrected every fifteen minutes forever. Events that exhausted their budget are retired by
   * {@link #exhaustStaleProcessing(Duration, int)} instead.</p>
   */
  public int requeueStaleProcessing(Duration minimumAge, int maxEvents) {
    if (minimumAge == null || minimumAge.isNegative() || minimumAge.isZero()) {
      throw new IllegalArgumentException("minimumAge must be positive");
    }
    return requeue("PROCESSING", maxEvents, minimumAge, false, true);
  }

  /**
   * Retires abandoned events that already used every delivery attempt allowed by their provider.
   *
   * <p>They are marked FAILED so they leave the recovery loop and become visible to an
   * administrator, who can correct the configuration and requeue them explicitly.</p>
   *
   * @return the number of events retired
   */
  public int exhaustStaleProcessing(Duration minimumAge, int maxEvents) {
    if (minimumAge == null || minimumAge.isNegative() || minimumAge.isZero()) {
      throw new IllegalArgumentException("minimumAge must be positive");
    }
    if (maxEvents < 1) {
      throw new IllegalArgumentException("maxEvents must be positive");
    }
    String sql = "UPDATE etarc_vector_outbox SET status = 'FAILED', updated = now() AT TIME ZONE 'UTC', updatedby = '0', "
        + "last_error = 'Delivery abandoned after exhausting the provider retry limit.' "
        + "WHERE etarc_vector_outbox_id IN (SELECT etarc_vector_outbox_id FROM etarc_vector_outbox "
        + "WHERE status = 'PROCESSING' AND updated < now() AT TIME ZONE 'UTC' - (? * interval '1 second') "
        + "AND attempt_count >= "
        + RETRY_LIMIT_SQL
        + " ORDER BY updated, etarc_vector_outbox_id LIMIT ?)";
    try (PreparedStatement statement = connectionProvider.getPreparedStatement(sql)) {
      statement.setLong(1, minimumAge.getSeconds());
      statement.setInt(2, DEFAULT_RETRY_LIMIT);
      statement.setInt(3, maxEvents);
      return statement.executeUpdate();
    } catch (Exception e) {
      throw new VectorException(VectorErrorCode.VECTOR_OUTBOX_OPERATION_FAILED,
          "Could not retire exhausted vector outbox events.", e);
    }
  }

  private List<VectorOutboxEvent> loadPending(int maxEvents) {
    List<VectorOutboxEvent> events = new ArrayList<>();
    try (PreparedStatement statement = connectionProvider.getPreparedStatement(PENDING_EVENTS_SQL)) {
      statement.setInt(1, maxEvents);
      try (ResultSet result = statement.executeQuery()) {
        while (result.next()) {
          events.add(new VectorOutboxEvent(result.getString(1), result.getString(2), result.getLong(3),
              result.getString(4), result.getString(5), result.getString(6), result.getString(7),
              result.getString(8), result.getString(9), result.getLong(10)));
        }
      }
      return events;
    } catch (Exception e) {
      throw new VectorException(VectorErrorCode.VECTOR_OUTBOX_OPERATION_FAILED,
          "Could not load pending vector outbox events.", e);
    }
  }

  private boolean claim(String eventId) {
    return update("UPDATE etarc_vector_outbox SET status = 'PROCESSING', attempt_count = attempt_count + 1, "
        + "updated = now() AT TIME ZONE 'UTC', updatedby = '0' WHERE etarc_vector_outbox_id = ? AND status = 'PENDING'",
        eventId) == 1;
  }

  private void supersedeOlderPending(VectorOutboxEvent event) {
    String sql = "UPDATE etarc_vector_outbox SET status = 'SUPERSEDED', last_error = 'Superseded by a newer event', "
        + "processed_at = now() AT TIME ZONE 'UTC', updated = now() AT TIME ZONE 'UTC', updatedby = '0' WHERE etarc_vector_source_id = ? "
        + "AND record_id = ? AND status = 'PENDING' AND etarc_vector_outbox_id <> ?";
    try (PreparedStatement statement = connectionProvider.getPreparedStatement(sql)) {
      statement.setString(1, event.getSourceId()); statement.setString(2, event.getRecordId());
      statement.setString(3, event.getId()); statement.executeUpdate();
    } catch (Exception e) { throw new VectorException(VectorErrorCode.VECTOR_OUTBOX_OPERATION_FAILED,
        "Could not supersede obsolete vector outbox events.", e); }
  }

  private void markSuperseded(String eventId) {
    update("UPDATE etarc_vector_outbox SET status = 'SUPERSEDED', processed_at = now() AT TIME ZONE 'UTC', "
        + "last_error = 'Discarded: the source configuration changed after the event was queued', "
        + "updated = now() AT TIME ZONE 'UTC', updatedby = '0' WHERE etarc_vector_outbox_id = ?", eventId);
  }

  private void markDone(String eventId) {
    update("UPDATE etarc_vector_outbox SET status = 'DONE', processed_at = now() AT TIME ZONE 'UTC', last_error = NULL, "
        + "updated = now() AT TIME ZONE 'UTC', updatedby = '0' WHERE etarc_vector_outbox_id = ?", eventId);
  }

  private void markFailed(String eventId, Exception error) {
    try (PreparedStatement statement = connectionProvider.getPreparedStatement(
        "UPDATE etarc_vector_outbox SET status = 'FAILED', last_error = ?, updated = now() AT TIME ZONE 'UTC', "
            + "updatedby = '0' WHERE etarc_vector_outbox_id = ?")) {
      statement.setString(1, errorMessage(error));
      statement.setString(2, eventId);
      statement.executeUpdate();
    } catch (Exception updateFailure) {
      throw new VectorException(VectorErrorCode.VECTOR_OUTBOX_OPERATION_FAILED,
          "Could not record a vector outbox consumer failure.", updateFailure);
    }
  }

  /**
   * Deletes events that already reached a terminal state and are older than {@code retention}.
   *
   * <p>The outbox is append-only: a trigger fires per watched column, so a single update of three
   * watched columns deposits three rows, and nothing ever removed the {@code DONE} and
   * {@code SUPERSEDED} ones. Left alone the table grows without bound and {@code loadPending}
   * degrades with it.</p>
   *
   * <p>Rows are removed oldest first and in a bounded amount, so the delete never turns into a long
   * lock. The status predicate is index-backed by both outbox indexes; filtering on {@code created}
   * rather than {@code processed_at} keeps the ordering aligned with {@code ETARC_VOUT_PENDING_IDX}
   * so the planner can avoid a sort, although which index it picks depends on the table's
   * statistics.</p>
   *
   * @param retention
   *     how long a terminal event is kept
   * @param maxEvents
   *     upper bound of rows removed in this call
   * @return the number of events removed
   */
  public int purgeTerminal(Duration retention, int maxEvents) {
    if (maxEvents < 1) {
      throw new IllegalArgumentException("maxEvents must be positive");
    }
    if (retention == null || retention.isNegative()) {
      throw new IllegalArgumentException("retention must not be negative");
    }
    String sql = "DELETE FROM etarc_vector_outbox WHERE etarc_vector_outbox_id IN ("
        + "SELECT etarc_vector_outbox_id FROM etarc_vector_outbox "
        + "WHERE status IN ('DONE', 'SUPERSEDED') AND created < now() AT TIME ZONE 'UTC' - (? * interval '1 second') "
        + "ORDER BY created, etarc_vector_outbox_id LIMIT ?)";
    try (PreparedStatement statement = connectionProvider.getPreparedStatement(sql)) {
      statement.setLong(1, retention.getSeconds());
      statement.setInt(2, maxEvents);
      return statement.executeUpdate();
    } catch (Exception e) {
      throw new VectorException(VectorErrorCode.VECTOR_OUTBOX_OPERATION_FAILED,
          "Could not purge vector outbox events.", e);
    }
  }

  private int requeue(String status, int maxEvents, Duration minimumAge, boolean resetAttempts,
      boolean withinAttemptLimit) {
    if (maxEvents < 1) {
      throw new IllegalArgumentException("maxEvents must be positive");
    }
    String ageCondition = minimumAge == null ? "" : " AND updated < now() AT TIME ZONE 'UTC' - (? * interval '1 second')";
    String attemptReset = resetAttempts ? ", attempt_count = 0" : "";
    String attemptCondition = withinAttemptLimit ? " AND attempt_count < " + RETRY_LIMIT_SQL : "";
    String sql = "UPDATE etarc_vector_outbox SET status = 'PENDING', last_error = NULL, updated = now() AT TIME ZONE 'UTC', "
        + "updatedby = '0'" + attemptReset + " WHERE etarc_vector_outbox_id IN (SELECT etarc_vector_outbox_id "
        + "FROM etarc_vector_outbox WHERE status = ?" + ageCondition + attemptCondition
        + " ORDER BY updated, etarc_vector_outbox_id LIMIT ?)";
    try (PreparedStatement statement = connectionProvider.getPreparedStatement(sql)) {
      statement.setString(1, status);
      int parameter = 2;
      if (minimumAge != null) {
        statement.setLong(parameter++, minimumAge.getSeconds());
      }
      if (withinAttemptLimit) {
        statement.setInt(parameter++, DEFAULT_RETRY_LIMIT);
      }
      statement.setInt(parameter, maxEvents);
      return statement.executeUpdate();
    } catch (Exception e) {
      throw new VectorException(VectorErrorCode.VECTOR_OUTBOX_OPERATION_FAILED,
          "Could not requeue vector outbox events.", e);
    }
  }

  private int update(String sql, String eventId) {
    try (PreparedStatement statement = connectionProvider.getPreparedStatement(sql)) {
      statement.setString(1, eventId);
      return statement.executeUpdate();
    } catch (Exception e) {
      throw new VectorException(VectorErrorCode.VECTOR_OUTBOX_OPERATION_FAILED,
          "Could not update a vector outbox event.", e);
    }
  }

  private static String errorMessage(Exception error) {
    String message = error.getMessage() == null ? error.getClass().getSimpleName() : error.getMessage();
    return message.length() <= MAX_ERROR_LENGTH ? message : message.substring(0, MAX_ERROR_LENGTH);
  }
}
