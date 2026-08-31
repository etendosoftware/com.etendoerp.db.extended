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
package com.etendoerp.db.extended.modulescript;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openbravo.database.ConnectionProvider;
import org.openbravo.modulescript.PostUpdateModuleScript;

/**
 * Materializes PostgreSQL outbox triggers for the enabled generic vector sources.
 *
 * <p>The Application Dictionary is the source of truth: a source selects an AD table and the
 * selected child rows determine the columns that enqueue an UPDATE event. The generated trigger
 * never creates embeddings or calls external services; it only inserts a PENDING event in
 * {@code ETARC_VECTOR_OUTBOX}. This script runs after dictionary data is installed so it can see
 * the complete source configuration.</p>
 */
public class GenerateVectorSourceTriggers extends PostUpdateModuleScript {

  private static final Logger log = LogManager.getLogger();

  private static final String SOURCES_SQL =
      "SELECT s.etarc_vector_source_id, t.tablename, k.columnname AS key_column, "
          + "(SELECT c.columnname FROM ad_column c WHERE c.ad_table_id = t.ad_table_id "
          + " AND lower(c.columnname) = 'ad_client_id' AND c.isactive = 'Y') AS client_column, "
          + "(SELECT c.columnname FROM ad_column c WHERE c.ad_table_id = t.ad_table_id "
          + " AND lower(c.columnname) = 'ad_org_id' AND c.isactive = 'Y') AS organization_column, "
          + "s.isinsertenabled, s.isupdateenabled, "
          + "s.isdeleteenabled "
          + "FROM etarc_vector_source s "
          + "JOIN ad_table t ON t.ad_table_id = s.ad_table_id "
          + "LEFT JOIN ad_column k ON k.ad_table_id = t.ad_table_id "
          + "  AND k.iskey = 'Y' AND k.isactive = 'Y' "
          + "WHERE s.isactive = 'Y' AND s.isenabled = 'Y' "
          + "ORDER BY s.etarc_vector_source_id";

  private static final String WATCHED_COLUMNS_SQL =
      "SELECT c.ad_column_id, c.columnname "
          + "FROM etarc_vector_source_column sc "
          + "JOIN ad_column c ON c.ad_column_id = sc.ad_column_id "
          + "WHERE sc.etarc_vector_source_id = ? "
          + "  AND sc.isactive = 'Y' AND sc.isreindexonchange = 'Y' "
          + "  AND c.isactive = 'Y' "
          + "ORDER BY sc.seqno, sc.etarc_vector_source_column_id";

  @Override
  public void execute() {
    try {
      ConnectionProvider connectionProvider = getConnectionProvider();
      if (!"POSTGRE".equals(connectionProvider.getRDBMS())) {
        log.info("Vector source triggers are only generated for PostgreSQL.");
        return;
      }

      List<Source> sources = loadSources(connectionProvider);
      Set<String> activeFunctions = new HashSet<>();
      Set<String> activeTriggers = new HashSet<>();
      for (Source source : sources) {
        if (source.keyColumn == null) {
          log.warn("Skipping vector source {} because its table has no active key column.", source.id);
          continue;
        }
        deploySource(connectionProvider, source, activeFunctions, activeTriggers);
      }
      dropOrphans(connectionProvider, activeFunctions, activeTriggers);
      log.info("Generated vector outbox triggers for {} enabled source(s).", sources.size());
    } catch (Exception e) {
      handleError(e);
    }
  }

  private List<Source> loadSources(ConnectionProvider connectionProvider) throws Exception {
    List<Source> sources = new ArrayList<>();
    try (PreparedStatement statement = connectionProvider.getPreparedStatement(SOURCES_SQL);
        ResultSet result = statement.executeQuery()) {
      while (result.next()) {
        Source source = new Source();
        source.id = result.getString("etarc_vector_source_id");
        source.tableName = result.getString("tablename");
        source.keyColumn = result.getString("key_column");
        source.clientColumn = result.getString("client_column");
        source.organizationColumn = result.getString("organization_column");
        source.insertEnabled = "Y".equals(result.getString("isinsertenabled"));
        source.updateEnabled = "Y".equals(result.getString("isupdateenabled"));
        source.deleteEnabled = "Y".equals(result.getString("isdeleteenabled"));
        sources.add(source);
      }
    }
    return sources;
  }

  private void deploySource(ConnectionProvider connectionProvider, Source source,
      Set<String> activeFunctions, Set<String> activeTriggers) throws Exception {
    String functionName = functionName(source.id);
    activeFunctions.add(functionName);
    execute(connectionProvider, createFunctionSql(functionName, source));

    if (source.insertEnabled) {
      String trigger = triggerName(source.id, "ai");
      activeTriggers.add(trigger);
      recreateTrigger(connectionProvider, trigger, source.tableName,
          "AFTER INSERT", functionName, "");
    }
    if (source.deleteEnabled) {
      String trigger = triggerName(source.id, "ad");
      activeTriggers.add(trigger);
      recreateTrigger(connectionProvider, trigger, source.tableName,
          "AFTER DELETE", functionName, "");
    }
    if (source.updateEnabled) {
      for (WatchedColumn column : loadWatchedColumns(connectionProvider, source.id)) {
        String trigger = triggerName(source.id, "u_" + shortId(column.id).toLowerCase());
        activeTriggers.add(trigger);
        String quotedColumn = quoteIdentifier(column.name);
        recreateTrigger(connectionProvider, trigger, source.tableName,
            "AFTER UPDATE OF " + quotedColumn,
            functionName, column.id,
            " WHEN (OLD." + quotedColumn + " IS DISTINCT FROM NEW." + quotedColumn + ")");
      }
    }
  }

  private List<WatchedColumn> loadWatchedColumns(ConnectionProvider connectionProvider, String sourceId)
      throws Exception {
    List<WatchedColumn> columns = new ArrayList<>();
    try (PreparedStatement statement = connectionProvider.getPreparedStatement(WATCHED_COLUMNS_SQL)) {
      statement.setString(1, sourceId);
      try (ResultSet result = statement.executeQuery()) {
        while (result.next()) {
          columns.add(new WatchedColumn(result.getString("ad_column_id"),
              result.getString("columnname")));
        }
      }
    }
    return columns;
  }

  private String createFunctionSql(String functionName, Source source) {
    String keyColumn = quoteIdentifier(source.keyColumn);
    return "CREATE OR REPLACE FUNCTION " + quoteIdentifier(functionName) + "() RETURNS trigger "
        + "LANGUAGE plpgsql AS $$ BEGIN "
        + "INSERT INTO etarc_vector_outbox (etarc_vector_outbox_id, ad_client_id, ad_org_id, "
        + "isactive, created, createdby, updated, updatedby, etarc_vector_source_id, config_version, record_id, "
        + "event_type, ad_column_id, status, attempt_count) VALUES (get_uuid(), "
        + sourceScopeExpression(source.clientColumn) + ", "
        + sourceScopeExpression(source.organizationColumn)
        + ", 'Y', now(), '0', now(), '0', " + quoteLiteral(source.id) + ", "
        + "(SELECT config_version FROM etarc_vector_source WHERE etarc_vector_source_id = " + quoteLiteral(source.id) + "), "
        + "CASE WHEN TG_OP = 'DELETE' THEN OLD." + keyColumn + " ELSE NEW." + keyColumn + " END, "
        + "TG_OP, NULLIF(TG_ARGV[0], ''), 'PENDING', 0); "
        + "IF TG_OP = 'DELETE' THEN RETURN OLD; END IF; RETURN NEW; END; $$";
  }

  private void recreateTrigger(ConnectionProvider connectionProvider, String triggerName, String tableName,
      String event, String functionName, String columnId) throws Exception {
    recreateTrigger(connectionProvider, triggerName, tableName, event, functionName, columnId, "");
  }

  private void recreateTrigger(ConnectionProvider connectionProvider, String triggerName, String tableName,
      String event, String functionName, String columnId, String whenClause) throws Exception {
    String quotedTable = quoteIdentifier(tableName);
    execute(connectionProvider, "DROP TRIGGER IF EXISTS " + quoteIdentifier(triggerName) + " ON "
        + quotedTable);
    execute(connectionProvider, "CREATE TRIGGER " + quoteIdentifier(triggerName) + " " + event + " ON "
        + quotedTable + " FOR EACH ROW" + whenClause + " EXECUTE FUNCTION "
        + quoteIdentifier(functionName) + "(" + quoteLiteral(columnId) + ")");
  }

  private void dropOrphans(ConnectionProvider connectionProvider, Set<String> activeFunctions,
      Set<String> activeTriggers) throws Exception {
    try (PreparedStatement statement = connectionProvider.getPreparedStatement(
        "SELECT t.tgname, c.relname FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid "
            + "WHERE NOT t.tgisinternal AND t.tgname LIKE 'etarc_vsrc_%'" );
        ResultSet result = statement.executeQuery()) {
      while (result.next()) {
        String trigger = result.getString(1);
        if (!activeTriggers.contains(trigger)) {
          execute(connectionProvider, "DROP TRIGGER IF EXISTS " + quoteIdentifier(trigger) + " ON "
              + quoteIdentifier(result.getString(2)));
        }
      }
    }
    try (PreparedStatement statement = connectionProvider.getPreparedStatement(
        "SELECT proname FROM pg_proc WHERE proname LIKE 'etarc_vsrc_%_fn'");
        ResultSet result = statement.executeQuery()) {
      while (result.next()) {
        String function = result.getString(1);
        if (!activeFunctions.contains(function)) {
          execute(connectionProvider, "DROP FUNCTION IF EXISTS " + quoteIdentifier(function) + "()");
        }
      }
    }
  }

  private void execute(ConnectionProvider connectionProvider, String sql) throws Exception {
    try (PreparedStatement statement = connectionProvider.getPreparedStatement(sql)) {
      statement.executeUpdate();
    }
  }

  private static String functionName(String sourceId) {
    return "etarc_vsrc_" + sourceId.toLowerCase() + "_fn";
  }

  private static String triggerName(String sourceId, String suffix) {
    return "etarc_vsrc_" + sourceId.toLowerCase() + "_" + suffix;
  }

  private static String quoteIdentifier(String value) {
    return "\"" + value.toLowerCase().replace("\"", "\"\"") + "\"";
  }

  private static String shortId(String id) {
    return id.substring(0, Math.min(8, id.length()));
  }

  private static String quoteLiteral(String value) {
    return "'" + value.replace("'", "''") + "'";
  }

  private static String sourceScopeExpression(String column) {
    if (column == null) {
      return "'0'";
    }
    String quotedColumn = quoteIdentifier(column);
    return "CASE WHEN TG_OP = 'DELETE' THEN OLD." + quotedColumn + " ELSE NEW."
        + quotedColumn + " END";
  }

  private static class Source {
    private String id;
    private String tableName;
    private String keyColumn;
    private String clientColumn;
    private String organizationColumn;
    private boolean insertEnabled;
    private boolean updateEnabled;
    private boolean deleteEnabled;
  }

  private static class WatchedColumn {
    private final String id;
    private final String name;

    private WatchedColumn(String id, String name) {
      this.id = id;
      this.name = name;
    }
  }
}
