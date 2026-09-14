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

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.openbravo.database.ConnectionProvider;

/**
 * Enqueues the records a source already held before it was instrumented.
 *
 * <p>The generated triggers only capture changes made from the moment they exist, so a source
 * configured over a table with history starts out empty and only fills with whatever is touched
 * afterwards. This service walks the source table and appends an event per record, which is the
 * same shape of work the triggers produce.</p>
 *
 * <p>It advances by keyset, storing how far it got on the request itself: a run interrupted halfway
 * continues from there instead of starting over, and a table with millions of rows never becomes a
 * single long transaction. It also refuses to run ahead of the consumer, because enqueueing faster
 * than the outbox drains only grows a backlog that has to be paid for one provider call at a
 * time.</p>
 */
public class VectorReindexService {

  /** Events are appended as updates: the record exists and its embedding has to be rebuilt. */
  private static final String EVENT_TYPE = "UPDATE";

  private static final String CLAIM_SQL =
      "UPDATE etarc_vector_reindex_req SET status = 'PROCESSING', updated = now() AT TIME ZONE 'UTC', updatedby = '0' "
          + "WHERE etarc_vector_reindex_req_id = (SELECT etarc_vector_reindex_req_id "
          + "FROM etarc_vector_reindex_req WHERE isactive = 'Y' AND status IN ('PENDING', 'PROCESSING') "
          + "ORDER BY status DESC, updated LIMIT 1 FOR UPDATE SKIP LOCKED)";

  private static final String REQUEST_SQL =
      "SELECT r.etarc_vector_reindex_req_id, r.etarc_vector_source_id, r.last_record_id, "
          + "r.enqueued_count, r.total_count, s.config_version, t.tablename, k.columnname AS key_column, "
          + "(SELECT c.columnname FROM ad_column c WHERE c.ad_table_id = t.ad_table_id "
          + " AND lower(c.columnname) = 'ad_client_id' AND c.isactive = 'Y') AS client_column, "
          + "(SELECT c.columnname FROM ad_column c WHERE c.ad_table_id = t.ad_table_id "
          + " AND lower(c.columnname) = 'ad_org_id' AND c.isactive = 'Y') AS organization_column, "
          + "f.columnname AS filter_column, s.filter_value "
          + "FROM etarc_vector_reindex_req r "
          + "JOIN etarc_vector_source s ON s.etarc_vector_source_id = r.etarc_vector_source_id "
          + "JOIN ad_table t ON t.ad_table_id = s.ad_table_id "
          + "LEFT JOIN ad_column f ON f.ad_column_id = s.ad_filter_column_id "
          + "  AND f.ad_table_id = s.ad_table_id AND f.isactive = 'Y' "
          + "LEFT JOIN ad_column k ON k.ad_table_id = t.ad_table_id "
          + "  AND k.iskey = 'Y' AND k.isactive = 'Y' "
          + "WHERE r.status = 'PROCESSING' AND r.isactive = 'Y'";

  private static final String BACKLOG_SQL =
      "SELECT count(*) FROM etarc_vector_outbox WHERE etarc_vector_source_id = ? AND status = 'PENDING'";

  private final ConnectionProvider connectionProvider;
  private final VectorOutboxService.TransactionBoundary transactionBoundary;

  public VectorReindexService(ConnectionProvider connectionProvider,
      VectorOutboxService.TransactionBoundary transactionBoundary) {
    this.connectionProvider = connectionProvider;
    this.transactionBoundary = transactionBoundary;
  }

  /**
   * Advances the pending reindex request, if there is one.
   *
   * @param chunkSize
   *     records enqueued per statement, which is also the transaction held while doing it
   * @param maxChunks
   *     upper bound of chunks for this run, so one run cannot monopolise the scheduler
   * @param backlogLimit
   *     stop when the source already has this many events waiting, so the reindex never runs
   *     further ahead of the consumer than it can absorb
   * @return the number of records enqueued in this run
   */
  public int process(int chunkSize, int maxChunks, int backlogLimit) {
    if (chunkSize < 1 || maxChunks < 1 || backlogLimit < 1) {
      throw new IllegalArgumentException("chunkSize, maxChunks and backlogLimit must be positive");
    }
    if (claim() == 0) {
      return 0;
    }
    transactionBoundary.commit();
    Request request = load();
    if (request == null) {
      return 0;
    }
    int enqueued = 0;
    try {
      if (request.totalCount == null) {
        estimateTotal(request);
        transactionBoundary.commit();
      }
      for (int chunk = 0; chunk < maxChunks; chunk++) {
        if (backlog(request.sourceId) >= backlogLimit) {
          // Leave it PROCESSING: the next run continues from the stored cursor.
          break;
        }
        int added = enqueueChunk(request, chunkSize);
        if (added == 0) {
          complete(request.id);
          transactionBoundary.commit();
          break;
        }
        enqueued += added;
        transactionBoundary.commit();
      }
      return enqueued;
    } catch (Exception e) {
      transactionBoundary.rollback();
      fail(request.id, e);
      transactionBoundary.commit();
      throw new VectorException(VectorErrorCode.VECTOR_OUTBOX_OPERATION_FAILED,
          "Could not enqueue the records of a vector reindex request.", e);
    }
  }

  private int claim() {
    try (PreparedStatement statement = connectionProvider.getPreparedStatement(CLAIM_SQL)) {
      return statement.executeUpdate();
    } catch (Exception e) {
      throw new VectorException(VectorErrorCode.VECTOR_OUTBOX_OPERATION_FAILED,
          "Could not claim a vector reindex request.", e);
    }
  }

  private Request load() {
    try (PreparedStatement statement = connectionProvider.getPreparedStatement(REQUEST_SQL);
        ResultSet result = statement.executeQuery()) {
      if (!result.next()) {
        return null;
      }
      Request request = new Request();
      request.id = result.getString(1);
      request.sourceId = result.getString(2);
      request.cursor = result.getString(3);
      request.enqueuedCount = result.getLong(4);
      request.totalCount = result.getObject(5) == null ? null : result.getLong(5);
      request.configVersion = result.getLong(6);
      request.table = result.getString(7);
      request.keyColumn = result.getString(8);
      request.clientColumn = result.getString(9);
      request.organizationColumn = result.getString(10);
      request.filterColumn = result.getString(11);
      request.filterValue = result.getString(12);
      if (request.keyColumn == null) {
        throw new VectorException(VectorErrorCode.VECTOR_OUTBOX_OPERATION_FAILED,
            "The source table has no active key column, so its records cannot be enqueued.");
      }
      return request;
    } catch (VectorException e) {
      throw e;
    } catch (Exception e) {
      throw new VectorException(VectorErrorCode.VECTOR_OUTBOX_OPERATION_FAILED,
          "Could not read the vector reindex request.", e);
    }
  }

  /**
   * Stores an approximate row count taken from the table statistics.
   *
   * <p>An exact count would scan the whole table, which is precisely what this service exists to
   * avoid doing in one go. The number is only there to give the operator a sense of progress.</p>
   */
  private void estimateTotal(Request request) throws Exception {
    String sql = "UPDATE etarc_vector_reindex_req SET total_count = "
        + "GREATEST(COALESCE((SELECT reltuples::bigint FROM pg_class WHERE oid = ?::regclass), 0), 0) "
        + "WHERE etarc_vector_reindex_req_id = ?";
    try (PreparedStatement statement = connectionProvider.getPreparedStatement(sql)) {
      statement.setString(1, request.table.toLowerCase());
      statement.setString(2, request.id);
      statement.executeUpdate();
    }
  }

  private long backlog(String sourceId) throws Exception {
    try (PreparedStatement statement = connectionProvider.getPreparedStatement(BACKLOG_SQL)) {
      statement.setString(1, sourceId);
      try (ResultSet result = statement.executeQuery()) {
        return result.next() ? result.getLong(1) : 0;
      }
    }
  }

  /**
   * Appends one page of records and moves the cursor in the same statement.
   *
   * <p>Reading the page, inserting from it and computing the new cursor happen together so the
   * three cannot disagree: the cursor always reflects exactly what was enqueued.</p>
   *
   * @return how many records were enqueued, zero when the table is exhausted
   */
  private int enqueueChunk(Request request, int chunkSize) throws Exception {
    String key = quote(request.keyColumn);
    String client = request.clientColumn == null ? "'0'" : "page." + quote(request.clientColumn);
    String organization = request.organizationColumn == null ? "'0'" : "page." + quote(request.organizationColumn);
    StringBuilder page = new StringBuilder("SELECT ").append(key);
    if (request.clientColumn != null) {
      page.append(", ").append(quote(request.clientColumn));
    }
    if (request.organizationColumn != null) {
      page.append(", ").append(quote(request.organizationColumn));
    }
    page.append(" FROM ").append(quote(request.table)).append(" WHERE (? IS NULL OR ").append(key).append(" > ?)");
    boolean filtered = request.filterColumn != null && request.filterValue != null;
    if (filtered) {
      // The same predicate the generated trigger applies, so a reindex enqueues exactly the records
      // that ongoing changes would.
      page.append(" AND ").append(quote(request.filterColumn)).append(" IS NOT DISTINCT FROM ?");
    }
    page.append(" ORDER BY ").append(key).append(" LIMIT ?");

    String sql = "WITH page AS (" + page + "), inserted AS ("
        + "INSERT INTO etarc_vector_outbox (etarc_vector_outbox_id, ad_client_id, ad_org_id, isactive, "
        + "created, createdby, updated, updatedby, etarc_vector_source_id, config_version, record_id, "
        + "event_type, ad_column_id, status, attempt_count) "
        + "SELECT get_uuid(), " + client + ", " + organization + ", 'Y', now() AT TIME ZONE 'UTC', '0', now() AT TIME ZONE 'UTC', '0', ?, ?, "
        + "page." + key + ", '" + EVENT_TYPE + "', NULL, 'PENDING', 0 FROM page) "
        + "SELECT count(*), max(page." + key + ") FROM page";

    try (PreparedStatement statement = connectionProvider.getPreparedStatement(sql)) {
      int parameter = 1;
      statement.setString(parameter++, request.cursor);
      statement.setString(parameter++, request.cursor);
      if (filtered) {
        statement.setString(parameter++, request.filterValue);
      }
      statement.setInt(parameter++, chunkSize);
      statement.setString(parameter++, request.sourceId);
      statement.setLong(parameter, request.configVersion);
      try (ResultSet result = statement.executeQuery()) {
        if (!result.next()) {
          return 0;
        }
        int added = result.getInt(1);
        if (added == 0) {
          return 0;
        }
        request.cursor = result.getString(2);
        request.enqueuedCount += added;
        advance(request);
        return added;
      }
    }
  }

  private void advance(Request request) throws Exception {
    String sql = "UPDATE etarc_vector_reindex_req SET last_record_id = ?, enqueued_count = ?, "
        + "updated = now() AT TIME ZONE 'UTC', updatedby = '0' WHERE etarc_vector_reindex_req_id = ?";
    try (PreparedStatement statement = connectionProvider.getPreparedStatement(sql)) {
      statement.setString(1, request.cursor);
      statement.setLong(2, request.enqueuedCount);
      statement.setString(3, request.id);
      statement.executeUpdate();
    }
  }

  private void complete(String requestId) {
    update("UPDATE etarc_vector_reindex_req SET status = 'DONE', last_error = NULL, updated = now() AT TIME ZONE 'UTC', "
        + "updatedby = '0' WHERE etarc_vector_reindex_req_id = ?", requestId, null);
  }

  private void fail(String requestId, Exception error) {
    String message = error.getMessage() == null ? error.getClass().getSimpleName() : error.getMessage();
    update("UPDATE etarc_vector_reindex_req SET status = 'FAILED', last_error = ?, updated = now() AT TIME ZONE 'UTC', "
        + "updatedby = '0' WHERE etarc_vector_reindex_req_id = ?", requestId,
        message.length() > 2000 ? message.substring(0, 2000) : message);
  }

  private void update(String sql, String requestId, String message) {
    try (PreparedStatement statement = connectionProvider.getPreparedStatement(sql)) {
      if (message == null) {
        statement.setString(1, requestId);
      } else {
        statement.setString(1, message);
        statement.setString(2, requestId);
      }
      statement.executeUpdate();
    } catch (Exception e) {
      throw new VectorException(VectorErrorCode.VECTOR_OUTBOX_OPERATION_FAILED,
          "Could not update the vector reindex request.", e);
    }
  }

  private static String quote(String identifier) {
    return "\"" + identifier.toLowerCase().replace("\"", "\"\"") + "\"";
  }

  private static final class Request {
    private String id;
    private String sourceId;
    private String cursor;
    private long enqueuedCount;
    private Long totalCount;
    private long configVersion;
    private String table;
    private String keyColumn;
    private String clientColumn;
    private String organizationColumn;
    private String filterColumn;
    private String filterValue;
  }
}
