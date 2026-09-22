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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openbravo.database.ConnectionProvider;

/**
 * Materializes the PostgreSQL outbox triggers of the generic vector sources.
 *
 * <p>The Application Dictionary is the source of truth: a source selects an AD table and its
 * selected child rows determine the columns that enqueue an UPDATE event. A generated trigger
 * never creates embeddings or calls an external service; it only inserts a PENDING row in
 * {@code ETARC_VECTOR_OUTBOX}.</p>
 *
 * <p>This lives in the runtime source tree rather than beside the module script because both
 * entry points need it and only one of them is a module script. {@code
 * GenerateVectorSourceTriggers} calls {@link #deployAll()} after an update installs dictionary
 * data, when every source has to be rebuilt; {@code ActivateVectorSource} calls {@link
 * #deploy(String)} when an administrator changes a source through the window, which must not
 * disturb the sources they did not select. Keeping one implementation matters more than usual
 * here: the trigger names it produces have to agree with the ones the build validation excludes
 * from the DBSM model, and a second copy is how those two drift apart.</p>
 */
public class VectorTriggerService {

  private static final Logger log = LogManager.getLogger();

  /**
   * What it takes for a source to be worth instrumenting at all.
   *
   * <p>Both entry points share this one expression on purpose. Instrumenting a source that cannot
   * be delivered is not a harmless half-measure: the triggers keep enqueueing events, every one of
   * them fails, and they retry until the limit gives up. A source with no content column is the
   * case that showed it -- the dictionary accepts it, and only delivery rejects it.</p>
   */
  private static final String READY_EXPR =
      "(s.isactive = 'Y' AND s.isenabled = 'Y' AND k.columnname IS NOT NULL "
          + " AND ep.etarc_vector_embed_provider_id IS NOT NULL "
          + " AND EXISTS (SELECT 1 FROM etarc_vector_source_column sc "
          + "             WHERE sc.etarc_vector_source_id = s.etarc_vector_source_id "
          + "               AND sc.isactive = 'Y' AND sc.iscontent = 'Y'))";

  private static final String SOURCE_COLUMNS =
      "s.etarc_vector_source_id, t.tablename, k.columnname AS key_column, "
          + "(SELECT c.columnname FROM ad_column c WHERE c.ad_table_id = t.ad_table_id "
          + " AND lower(c.columnname) = 'ad_client_id' AND c.isactive = 'Y') AS client_column, "
          + "(SELECT c.columnname FROM ad_column c WHERE c.ad_table_id = t.ad_table_id "
          + " AND lower(c.columnname) = 'ad_org_id' AND c.isactive = 'Y') AS organization_column, "
          + "f.columnname AS filter_column, s.filter_value, "
          + "s.isinsertenabled, s.isupdateenabled, s.isdeleteenabled, " + READY_EXPR + " AS ready ";


  private static final String SOURCE_JOINS =
      "FROM etarc_vector_source s "
          + "JOIN ad_table t ON t.ad_table_id = s.ad_table_id "
          + "LEFT JOIN ad_column f ON f.ad_column_id = s.ad_filter_column_id "
          + "  AND f.ad_table_id = s.ad_table_id AND f.isactive = 'Y' "
          + "LEFT JOIN ad_column k ON k.ad_table_id = t.ad_table_id "
          + "  AND k.iskey = 'Y' AND k.isactive = 'Y' "
          // Joined on isactive, not merely on the foreign key: delivery resolves the provider with
          // that same condition, so a source pointing at an inactive one has no provider at all.
          // Instrumenting it anyway would fill the outbox with events nothing can ever deliver.
          + "LEFT JOIN etarc_vector_embed_provider ep "
          + "  ON ep.etarc_vector_embed_provider_id = s.etarc_vector_embed_provider_id "
          + " AND ep.isactive = 'Y' ";

  private static final String READY_SOURCES_SQL =
      "SELECT " + SOURCE_COLUMNS + SOURCE_JOINS
          + "WHERE " + READY_EXPR + " ORDER BY s.etarc_vector_source_id";

  /** Deliberately unfiltered: turning a source off has to be able to remove its triggers. */
  private static final String ONE_SOURCE_SQL =
      "SELECT " + SOURCE_COLUMNS + SOURCE_JOINS + "WHERE s.etarc_vector_source_id = ?";

  private static final String WATCHED_COLUMNS_SQL =
      "SELECT c.ad_column_id, c.columnname "
          + "FROM etarc_vector_source_column sc "
          + "JOIN ad_column c ON c.ad_column_id = sc.ad_column_id "
          + "WHERE sc.etarc_vector_source_id = ? "
          + "  AND sc.isactive = 'Y' AND sc.isreindexonchange = 'Y' "
          + "  AND c.isactive = 'Y' "
          + "ORDER BY sc.seqno, sc.etarc_vector_source_column_id";

  private static final String TRIGGERS_SQL =
      "SELECT t.tgname, c.relname FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid "
          + "WHERE NOT t.tgisinternal AND ";

  private static final String FUNCTIONS_SQL =
      "SELECT proname FROM pg_proc WHERE ";

  private final ConnectionProvider cp;

  /**
   * Creates a service that installs and removes the change-capture triggers of a source.
   *
   * @param cp
   *     connection the triggers and functions are created and dropped with
   */
  public VectorTriggerService(ConnectionProvider cp) {
    this.cp = cp;
  }

  /**
   * Rebuilds the triggers of every enabled source and removes everything else this module owns.
   *
   * <p>The sweep is global because the caller is an update: a source that was deleted or disabled
   * since the previous run leaves triggers nobody would otherwise remove.</p>
   *
   * @return the number of sources deployed
   * @throws Exception
   *     if the sources cannot be read or their triggers cannot be written
   */
  public int deployAll() throws Exception {
    if (!isPostgres()) {
      log.info("Vector source triggers are only generated for PostgreSQL.");
      return 0;
    }
    List<Source> sources = loadSources(READY_SOURCES_SQL, null);
    Set<String> activeFunctions = new HashSet<>();
    Set<String> activeTriggers = new HashSet<>();
    for (Source source : sources) {
      deploySource(source, activeFunctions, activeTriggers);
    }
    int deployed = sources.size();
    dropOrphans("t.tgname LIKE 'etarc_vsrc_%'", "proname LIKE 'etarc_vsrc_%_fn'",
        activeFunctions, activeTriggers);
    log.info("Generated vector outbox triggers for {} enabled source(s).", deployed);
    return deployed;
  }

  /**
   * Rebuilds the triggers of one source and removes the ones it no longer needs.
   *
   * <p>The sweep is narrowed to this source's own objects. An administrator activating a handful
   * of sources is not saying anything about the rest, and a global sweep here would silently tear
   * down the triggers of a source that merely happens to be disabled at the moment.</p>
   *
   * <p>A source that is inactive or disabled loses every trigger it has. That is the point: the
   * alternative is a table that keeps enqueueing events for a source nobody is going to deliver.
   * </p>
   *
   * @param sourceId the source to rebuild
   * @return what was installed and what was removed
   * @throws Exception
   *     if the source cannot be read or its triggers cannot be written
   */
  public Deployment deploy(String sourceId) throws Exception {
    return deploy(sourceId, true);
  }

  /**
   * Removes every trigger and function of one source, whatever the dictionary says about it.
   *
   * <p>{@link #deploy(String)} decides from the dictionary alone, which is all a module script can
   * know at update time: a collection legitimately does not exist yet, because activation is a
   * later and deliberate step. A caller that can see the runtime state knows more, and can refuse
   * to instrument a source whose collection no longer matches what its provider produces. Writing
   * into such a collection fails on every row, so capturing the changes only manufactures events
   * that cannot succeed.</p>
   *
   * @param sourceId the source to tear down
   * @return what was removed
   * @throws Exception
   *     if the triggers of the source cannot be removed
   */
  public Deployment teardown(String sourceId) throws Exception {
    return deploy(sourceId, false);
  }

  private Deployment deploy(String sourceId, boolean allowed) throws Exception {
    if (!isPostgres()) {
      return new Deployment(0, 0, false);
    }
    List<Source> sources = loadSources(ONE_SOURCE_SQL, sourceId);
    if (sources.isEmpty()) {
      throw new VectorException(VectorErrorCode.VECTOR_OUTBOX_OPERATION_FAILED,
          "Vector source was not found.");
    }
    Source source = sources.get(0);

    Set<String> activeFunctions = new HashSet<>();
    Set<String> activeTriggers = new HashSet<>();
    boolean instrumented = allowed && source.ready;
    if (instrumented) {
      deploySource(source, activeFunctions, activeTriggers);
    }
    int removed = dropOrphans("starts_with(t.tgname, " + quoteLiteral(namePrefix(source.id)) + ")",
        "starts_with(proname, " + quoteLiteral(namePrefix(source.id)) + ")",
        activeFunctions, activeTriggers);
    return new Deployment(activeTriggers.size(), removed, instrumented);
  }

  private boolean isPostgres() {
    return "POSTGRE".equals(cp.getRDBMS());
  }

  private List<Source> loadSources(String sql, String sourceId) throws Exception {
    List<Source> sources = new ArrayList<>();
    try (PreparedStatement statement = cp.getPreparedStatement(sql)) {
      if (sourceId != null) {
        statement.setString(1, sourceId);
      }
      try (ResultSet result = statement.executeQuery()) {
        while (result.next()) {
          Source source = new Source();
          source.id = result.getString("etarc_vector_source_id");
          source.tableName = result.getString("tablename");
          source.keyColumn = result.getString("key_column");
          source.clientColumn = result.getString("client_column");
          source.organizationColumn = result.getString("organization_column");
          source.filterColumn = result.getString("filter_column");
          source.filterValue = result.getString("filter_value");
          source.ready = result.getBoolean("ready");
          source.insertEnabled = "Y".equals(result.getString("isinsertenabled"));
          source.updateEnabled = "Y".equals(result.getString("isupdateenabled"));
          source.deleteEnabled = "Y".equals(result.getString("isdeleteenabled"));
          sources.add(source);
        }
      }
    }
    return sources;
  }

  private void deploySource(Source source, Set<String> activeFunctions, Set<String> activeTriggers)
      throws Exception {
    String functionName = functionName(source.id);
    activeFunctions.add(functionName);
    execute(createFunctionSql(functionName, source));

    if (source.insertEnabled) {
      String trigger = triggerName(source.id, "ai");
      activeTriggers.add(trigger);
      recreateTrigger(trigger, source.tableName, "AFTER INSERT", functionName, "", "");
    }
    if (source.deleteEnabled) {
      String trigger = triggerName(source.id, "ad");
      activeTriggers.add(trigger);
      recreateTrigger(trigger, source.tableName, "AFTER DELETE", functionName, "", "");
    }
    if (source.updateEnabled) {
      for (WatchedColumn column : loadWatchedColumns(source.id)) {
        String trigger = triggerName(source.id, "u_" + columnKey(column.id));
        activeTriggers.add(trigger);
        String quotedColumn = quoteIdentifier(column.name);
        recreateTrigger(trigger, source.tableName, "AFTER UPDATE OF " + quotedColumn, functionName,
            column.id, " WHEN (OLD." + quotedColumn + " IS DISTINCT FROM NEW." + quotedColumn + ")");
      }
    }
  }

  private List<WatchedColumn> loadWatchedColumns(String sourceId) throws Exception {
    List<WatchedColumn> columns = new ArrayList<>();
    try (PreparedStatement statement = cp.getPreparedStatement(WATCHED_COLUMNS_SQL)) {
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
    String filterGuard = filterGuard(source);
    return "CREATE OR REPLACE FUNCTION " + quoteIdentifier(functionName) + "() RETURNS trigger "
        + "LANGUAGE plpgsql AS $$ BEGIN " + filterGuard
        + "INSERT INTO etarc_vector_outbox (etarc_vector_outbox_id, ad_client_id, ad_org_id, "
        + "isactive, created, createdby, updated, updatedby, etarc_vector_source_id, config_version, record_id, "
        + "event_type, ad_column_id, status, attempt_count) VALUES (get_uuid(), "
        + sourceScopeExpression(source.clientColumn) + ", "
        + sourceScopeExpression(source.organizationColumn)
        + ", 'Y', now() AT TIME ZONE 'UTC', '0', now() AT TIME ZONE 'UTC', '0', " + quoteLiteral(source.id) + ", "
        + "(SELECT config_version FROM etarc_vector_source WHERE etarc_vector_source_id = " + quoteLiteral(source.id) + "), "
        + "CASE WHEN TG_OP = 'DELETE' THEN OLD." + keyColumn + " ELSE NEW." + keyColumn + " END, "
        + "TG_OP, NULLIF(TG_ARGV[0], ''), 'PENDING', 0); "
        + "IF TG_OP = 'DELETE' THEN RETURN OLD; END IF; RETURN NEW; END; $$";
  }

  private static String filterGuard(Source source) {
    if (source.filterColumn == null || source.filterValue == null) {
      return "";
    }
    String column = quoteIdentifier(source.filterColumn);
    String value = quoteLiteral(source.filterValue);
    String newMatches = "(NEW." + column + " IS NOT DISTINCT FROM " + value + ")";
    String oldMatches = "(OLD." + column + " IS NOT DISTINCT FROM " + value + ")";
    return "IF TG_OP = 'INSERT' AND NOT " + newMatches + " THEN RETURN NEW; END IF; "
        + "IF TG_OP = 'DELETE' AND NOT " + oldMatches + " THEN RETURN OLD; END IF; "
        + "IF TG_OP = 'UPDATE' AND NOT (" + newMatches + " OR " + oldMatches
        + ") THEN RETURN NEW; END IF; ";
  }

  private void recreateTrigger(String triggerName, String tableName, String event,
      String functionName, String columnId, String whenClause) throws Exception {
    String quotedTable = quoteIdentifier(tableName);
    execute("DROP TRIGGER IF EXISTS " + quoteIdentifier(triggerName) + " ON " + quotedTable);
    execute("CREATE TRIGGER " + quoteIdentifier(triggerName) + " " + event + " ON " + quotedTable
        + " FOR EACH ROW" + whenClause + " EXECUTE FUNCTION " + quoteIdentifier(functionName)
        + "(" + quoteLiteral(columnId) + ")");
  }

  /**
   * Removes the triggers and functions matched by the given predicates that are not in the active
   * set, so the caller decides whether the sweep covers every source or only one.
   *
   * @return the number of triggers dropped
   */
  private int dropOrphans(String triggerPredicate, String functionPredicate,
      Set<String> activeFunctions, Set<String> activeTriggers) throws Exception {
    int removed = 0;
    List<String[]> staleTriggers = new ArrayList<>();
    try (PreparedStatement statement = cp.getPreparedStatement(TRIGGERS_SQL + triggerPredicate);
        ResultSet result = statement.executeQuery()) {
      while (result.next()) {
        if (!activeTriggers.contains(result.getString(1))) {
          staleTriggers.add(new String[] { result.getString(1), result.getString(2) });
        }
      }
    }
    for (String[] trigger : staleTriggers) {
      execute("DROP TRIGGER IF EXISTS " + quoteIdentifier(trigger[0]) + " ON "
          + quoteIdentifier(trigger[1]));
      removed++;
    }

    List<String> staleFunctions = new ArrayList<>();
    try (PreparedStatement statement = cp.getPreparedStatement(FUNCTIONS_SQL + functionPredicate);
        ResultSet result = statement.executeQuery()) {
      while (result.next()) {
        if (!activeFunctions.contains(result.getString(1))) {
          staleFunctions.add(result.getString(1));
        }
      }
    }
    for (String function : staleFunctions) {
      execute("DROP FUNCTION IF EXISTS " + quoteIdentifier(function) + "()");
    }
    return removed;
  }

  private void execute(String sql) throws Exception {
    try (PreparedStatement statement = cp.getPreparedStatement(sql)) {
      statement.executeUpdate();
    }
  }

  private static String namePrefix(String sourceId) {
    return "etarc_vsrc_" + sourceId.toLowerCase();
  }

  private static String functionName(String sourceId) {
    return namePrefix(sourceId) + "_fn";
  }

  private static String triggerName(String sourceId, String suffix) {
    return namePrefix(sourceId) + "_" + suffix;
  }

  private static String quoteIdentifier(String value) {
    return "\"" + value.toLowerCase().replace("\"", "\"\"") + "\"";
  }

  /**
   * Builds the per-column suffix of a watched-column trigger name.
   * <p>
   * The original implementation took the first 8 characters of the column id, which collides
   * systematically: entire tables share the same 8-character prefix across all their columns
   * (AD_MODULE, for instance, has 33 columns starting with the same 8 characters). Two watched
   * columns colliding produce the same trigger name, so the second CREATE silently replaces the
   * first and that column stops enqueueing events, with no warning.
   * <p>
   * A hash keeps the name the same length -- the full 32-character id would push the trigger name
   * to 78 characters, past PostgreSQL's 63-byte limit -- while making collisions random rather
   * than systematic.
   * <p>
   * <b>Must stay identical to {@code CreateExcludeFilter.columnKey}</b>: the build validation
   * derives the excluded trigger names with the same rule, and if the two drift apart DBSM stops
   * recognising the generated triggers as excluded. It cannot be shared with this class because
   * the build validation runs from its own source tree, on its own classpath, before this one is
   * available.
   *
   * @param id the AD_COLUMN_ID of the watched column
   * @return an 8-character lowercase hexadecimal suffix
   */
  private static String columnKey(String id) {
    return String.format("%08x", id.hashCode());
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

  /** What one run changed for a single source. */
  public static final class Deployment {
    private final int installed;
    private final int removed;
    private final boolean instrumented;

    private Deployment(int installed, int removed, boolean instrumented) {
      this.installed = installed;
      this.removed = removed;
      this.instrumented = instrumented;
    }

    public int getInstalled() {
      return installed;
    }

    public int getRemoved() {
      return removed;
    }

    /** Whether the source is now instrumented at all, as opposed to having been torn down. */
    public boolean isInstrumented() {
      return instrumented;
    }
  }

  private static class Source {
    private String id;
    private String tableName;
    private String keyColumn;
    private String clientColumn;
    private String organizationColumn;
    private String filterColumn;
    private String filterValue;
    private boolean ready;
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
