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
import java.util.List;

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
          + "o.event_type, o.ad_column_id, o.ad_client_id, o.ad_org_id "
          + "FROM etarc_vector_outbox o "
          + "JOIN etarc_vector_source s ON s.etarc_vector_source_id = o.etarc_vector_source_id "
          + "WHERE o.isactive = 'Y' AND o.status = 'PENDING' "
          + "AND NOT EXISTS (SELECT 1 FROM etarc_vector_outbox newer WHERE newer.etarc_vector_source_id = o.etarc_vector_source_id "
          + "AND newer.record_id = o.record_id AND newer.status = 'PENDING' AND (newer.created > o.created OR (newer.created = o.created AND newer.etarc_vector_outbox_id > o.etarc_vector_outbox_id))) "
          + "AND NOT EXISTS (SELECT 1 FROM etarc_vector_outbox processing WHERE processing.etarc_vector_source_id = o.etarc_vector_source_id "
          + "AND processing.record_id = o.record_id AND processing.status = 'PROCESSING') "
          + "ORDER BY o.created, o.etarc_vector_outbox_id LIMIT ?";

  private final ConnectionProvider connectionProvider;
  private final VectorOutboxConsumerResolver consumerResolver;

  public VectorOutboxService(ConnectionProvider connectionProvider,
      Collection<VectorOutboxConsumer> consumers) {
    this.connectionProvider = connectionProvider;
    this.consumerResolver = new VectorOutboxConsumerResolver(consumers);
  }

  /** Processes at most {@code maxEvents} events and returns the number successfully delivered. */
  public int processPending(int maxEvents) {
    if (maxEvents < 1) {
      throw new IllegalArgumentException("maxEvents must be positive");
    }
    int processed = 0;
    for (VectorOutboxEvent event : loadPending(maxEvents)) {
      VectorOutboxConsumer consumer = consumerResolver.resolve(event.getNamespace());
      if (consumer == null) {
        continue;
      }
      supersedeOlderPending(event);
      if (!claim(event.getId())) continue;
      try {
        consumer.consume(event);
        markDone(event.getId());
        processed++;
      } catch (Exception e) {
        markFailed(event.getId(), e);
      }
    }
    return processed;
  }

  /** Requeues failed events, allowing an administrator to choose when a retry is attempted. */
  public int requeueFailed(int maxEvents) {
    return requeue("FAILED", maxEvents, null);
  }

  /** Requeues events abandoned while processing for at least the supplied duration. */
  public int requeueStaleProcessing(Duration minimumAge, int maxEvents) {
    if (minimumAge == null || minimumAge.isNegative() || minimumAge.isZero()) {
      throw new IllegalArgumentException("minimumAge must be positive");
    }
    return requeue("PROCESSING", maxEvents, minimumAge);
  }

  private List<VectorOutboxEvent> loadPending(int maxEvents) {
    List<VectorOutboxEvent> events = new ArrayList<>();
    try (PreparedStatement statement = connectionProvider.getPreparedStatement(PENDING_EVENTS_SQL)) {
      statement.setInt(1, maxEvents);
      try (ResultSet result = statement.executeQuery()) {
        while (result.next()) {
          events.add(new VectorOutboxEvent(result.getString(1), result.getString(2), result.getLong(3),
              result.getString(4), result.getString(5), result.getString(6), result.getString(7),
              result.getString(8), result.getString(9)));
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
        + "updated = now(), updatedby = '0' WHERE etarc_vector_outbox_id = ? AND status = 'PENDING'",
        eventId) == 1;
  }

  private void supersedeOlderPending(VectorOutboxEvent event) {
    String sql = "UPDATE etarc_vector_outbox SET status = 'SUPERSEDED', last_error = 'Superseded by a newer event', "
        + "processed_at = now(), updated = now(), updatedby = '0' WHERE etarc_vector_source_id = ? "
        + "AND record_id = ? AND status = 'PENDING' AND etarc_vector_outbox_id <> ?";
    try (PreparedStatement statement = connectionProvider.getPreparedStatement(sql)) {
      statement.setString(1, event.getSourceId()); statement.setString(2, event.getRecordId());
      statement.setString(3, event.getId()); statement.executeUpdate();
    } catch (Exception e) { throw new VectorException(VectorErrorCode.VECTOR_OUTBOX_OPERATION_FAILED,
        "Could not supersede obsolete vector outbox events.", e); }
  }

  private void markDone(String eventId) {
    update("UPDATE etarc_vector_outbox SET status = 'DONE', processed_at = now(), last_error = NULL, "
        + "updated = now(), updatedby = '0' WHERE etarc_vector_outbox_id = ?", eventId);
  }

  private void markFailed(String eventId, Exception error) {
    try (PreparedStatement statement = connectionProvider.getPreparedStatement(
        "UPDATE etarc_vector_outbox SET status = 'FAILED', last_error = ?, updated = now(), "
            + "updatedby = '0' WHERE etarc_vector_outbox_id = ?")) {
      statement.setString(1, errorMessage(error));
      statement.setString(2, eventId);
      statement.executeUpdate();
    } catch (Exception updateFailure) {
      throw new VectorException(VectorErrorCode.VECTOR_OUTBOX_OPERATION_FAILED,
          "Could not record a vector outbox consumer failure.", updateFailure);
    }
  }

  private int requeue(String status, int maxEvents, Duration minimumAge) {
    if (maxEvents < 1) {
      throw new IllegalArgumentException("maxEvents must be positive");
    }
    String ageCondition = minimumAge == null ? "" : " AND updated < now() - (? * interval '1 second')";
    String sql = "UPDATE etarc_vector_outbox SET status = 'PENDING', last_error = NULL, updated = now(), "
        + "updatedby = '0' WHERE etarc_vector_outbox_id IN (SELECT etarc_vector_outbox_id "
        + "FROM etarc_vector_outbox WHERE status = ?" + ageCondition
        + " ORDER BY updated, etarc_vector_outbox_id LIMIT ?)";
    try (PreparedStatement statement = connectionProvider.getPreparedStatement(sql)) {
      statement.setString(1, status);
      int parameter = 2;
      if (minimumAge != null) {
        statement.setLong(parameter++, minimumAge.getSeconds());
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
