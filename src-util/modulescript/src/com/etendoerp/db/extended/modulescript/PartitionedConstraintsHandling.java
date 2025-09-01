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
 * All portions are Copyright © 2021–2025 FUTIT SERVICES, S.L
 * All Rights Reserved.
 * Contributor(s): Futit Services S.L.
 *************************************************************************
 */

package com.etendoerp.db.extended.modulescript;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openbravo.base.exception.OBException;
import org.openbravo.database.ConnectionProvider;
import org.openbravo.modulescript.ModuleScript;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.etendoerp.db.extended.utils.TableDefinitionComparator;

public class PartitionedConstraintsHandling extends ModuleScript {

  public static final String MODULES_JAR = "build/etendo/modules";
  public static final String MODULES_BASE = "modules";
  public static final String MODULES_CORE = "modules_core";
  public static final String SEPARATOR = "=======================================================";
  public static final String TABLE_NAME = "tableName";
  public static final String TABLE = "table";
  private static final String SRC_DB_DATABASE_MODEL_TABLES = "src-db/database/model/tables";
  private static final String SRC_DB_DATABASE_MODEL_MODIFIED_TABLES = "src-db/database/model/modifiedTables";
  private static final Logger log4j = LogManager.getLogger();
  private static final String[] moduleDirs = new String[]{ MODULES_BASE, MODULES_CORE, MODULES_JAR };

  // Backup infrastructure
  private static final String BACKUP_SCHEMA = "etarc_backups";
  private static final String BACKUP_METADATA_TABLE = "backup_metadata"; // in BACKUP_SCHEMA
  private static final int MAX_BACKUPS_PER_TABLE = 5;
  private static final int BACKUP_RETENTION_DAYS = 7;

  // SQL templates
  private static final String DROP_PK =
      "ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s CASCADE;\n";
  private static final String ADD_PK_PARTITIONED =
      "ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY (%s, %s);\n";
  private static final String ADD_PK_SIMPLE =
      "ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY (%s);\n";
  private static final String DROP_FK =
      "ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s;\n";
  private static final String ADD_COL_IF_NOT_EXISTS =
      "ALTER TABLE %s\nADD COLUMN IF NOT EXISTS %s TIMESTAMP WITHOUT TIME ZONE;\n";
  private static final String UPDATE_HELPER_COL =
      "UPDATE %s SET %s = F.%s FROM %s F WHERE F.%s = %s.%s AND %s.%s IS NULL;\n";
  private static final String ADD_FK_PARTITIONED =
      "ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s, %s) " +
          "REFERENCES PUBLIC.%s (%s, %s) MATCH SIMPLE ON UPDATE CASCADE ON DELETE NO ACTION;\n";
  private static final String ADD_FK_SIMPLE =
      "ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) " +
          "REFERENCES PUBLIC.%s (%s) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION;\n";

  /**
   * Null/blank convenience, kept for readability in callers.
   */
  public static boolean isBlank(String str) {
    return StringUtils.isBlank(str);
  }

  /**
   * Logs a separator line at INFO, useful to visually split log sections.
   */
  private static void logSeparator() {
    log4j.info(SEPARATOR);
  }

  /**
   * Parses an XML file into a normalized DOM {@link Document} with XXE protections enabled.
   *
   * <p>The parser is configured to:</p>
   * <ul>
   *   <li>Disallow DOCTYPE declarations</li>
   *   <li>Disable external entities (general and parameter)</li>
   *   <li>Enable secure processing</li>
   *   <li>Disable XInclude and entity expansion</li>
   * </ul>
   */
  private static Document getDocument(File xml)
      throws ParserConfigurationException, SAXException, IOException {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
    factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
    factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
    factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
    factory.setXIncludeAware(false);
    factory.setExpandEntityReferences(false);

    DocumentBuilder builder = factory.newDocumentBuilder();
    Document doc = builder.parse(xml);
    doc.getDocumentElement().normalize();
    return doc;
  }

  /**
   * Attempts to resolve the primary key constraint name from the given table XML files.
   *
   * <p>For each XML:</p>
   * <ul>
   *   <li>Skips files under {@code modifiedTables}.</li>
   *   <li>Requires exactly one {@code <table>} element.</li>
   *   <li>Returns the {@code primaryKey} attribute if present; logs otherwise.</li>
   * </ul>
   *
   * @return the primary key name if found; {@code null} otherwise
   */
  public static String findPrimaryKey(List<File> xmlFiles) {
    try {
      for (File xml : xmlFiles) {
        if (StringUtils.contains(xml.getAbsolutePath(), "modifiedTables")) {
          continue;
        }
        if (!xml.exists()) {
          log4j.error("Error: XML file does not exist: {}", xml.getAbsolutePath());
          return null;
        }
        Document doc = getDocument(xml);
        NodeList tableList = doc.getElementsByTagName(TABLE);

        if (tableList.getLength() == 1) {
          Element tableEl = (Element) tableList.item(0);
          if (tableEl.hasAttribute("primaryKey")) {
            return tableEl.getAttribute("primaryKey");
          } else {
            log4j.warn("Warning: Missing 'primaryKey' attribute in: {}", xml.getAbsolutePath());
            return null;
          }
        } else if (tableList.getLength() == 0) {
          log4j.error("Error: No <table> tag found in: {}", xml.getAbsolutePath());
          return null;
        } else {
          log4j.error("Error: Found {} <table> tags in: {}", tableList.getLength(), xml.getAbsolutePath());
          return null;
        }
      }
    } catch (Exception e) {
      log4j.error("Error processing XML: {}", e.getMessage(), e);
    }
    return null;
  }

  /**
   * Logs that an XML file was skipped due to parsing issues. This is best-effort and non-blocking.
   */
  private static void logUnparseableXML(File sourceXmlFile, Exception e) {
    log4j.info("Skipping unparsable XML while scanning for FK refs: {} -> {}",
        sourceXmlFile.getAbsolutePath(), e.getMessage());
  }

  @Override
  public void execute() {
    try {
      ConnectionProvider cp = getConnectionProvider();
      List<Map<String, String>> tableConfigs = loadTableConfigs(cp);

      if (!tableConfigs.isEmpty()) {
        logSeparator();
        log4j.info("============== Partitioning process info ==============");
        logSeparator();
      }

      // Ensure backup infra and clear old backups before making changes
      ensureBackupInfrastructure(cp);
      cleanupExcessBackups(cp);

      Map<String, Long> timings = new LinkedHashMap<>();
      boolean hasPartitioned = quickCheckPartitioned(cp, tableConfigs);

      for (Map<String, String> cfg : tableConfigs) {
        String tName = cfg.get(TABLE_NAME);
        long elapsed = processTableSafely(cp, cfg);
        timings.put(tName, elapsed);
      }

      boolean anyProcessed = timings.values().stream().anyMatch(v -> v != null && v >= 0L);
      if (anyProcessed || hasPartitioned) {
        logSeparator();
        log4j.info("Partitioning processing summary (ms):");
        for (Map.Entry<String, Long> e : timings.entrySet()) {
          log4j.info(" - {} => {}", e.getKey(), e.getValue());
        }
      }
      if (!tableConfigs.isEmpty()) {
        logSeparator();
      }

    } catch (Exception e) {
      handleError(e);
    }
  }

  /**
   * Quickly checks whether any configured table is already partitioned.
   * Errors are swallowed to avoid failing the whole execution.
   */
  private boolean quickCheckPartitioned(ConnectionProvider cp, List<Map<String, String>> tableConfigs) {
    try {
      for (Map<String, String> cfg : tableConfigs) {
        String tName = cfg.get(TABLE_NAME);
        if (safeIsTablePartitioned(cp, tName)) {
          return true;
        }
      }
    } catch (Exception ignore) {
      // ignore global failures
    }
    return false;
  }

  /**
   * Wrapper around {@link #isTablePartitioned(ConnectionProvider, String)} that returns {@code false}
   * on failure instead of throwing.
   */
  private boolean safeIsTablePartitioned(ConnectionProvider cp, String tableName) {
    try {
      return isTablePartitioned(cp, tableName);
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Processes one table configuration and returns elapsed time in milliseconds,
   * or {@code -1} if not processed or on error.
   */
  private long processTableSafely(ConnectionProvider cp, Map<String, String> cfg) {
    long start = System.currentTimeMillis();
    try {
      boolean processed = processTableConfig(cp, cfg);
      return processed ? (System.currentTimeMillis() - start) : -1L;
    } catch (Exception e) {
      return -1L;
    }
  }

  /**
   * Loads table configuration from {@code ETARC_TABLE_CONFIG}:
   * table name, partition column, and PK column.
   */
  private List<Map<String, String>> loadTableConfigs(ConnectionProvider cp) throws Exception {
    String configSql = "SELECT UPPER(TBL.TABLENAME) TABLENAME, "
        + "UPPER(COL.COLUMNNAME) COLUMNNAME, "
        + "UPPER(COL_PK.COLUMNNAME) PK_COLUMNNAME "
        + "FROM ETARC_TABLE_CONFIG CFG "
        + "JOIN AD_TABLE TBL ON TBL.AD_TABLE_ID = CFG.AD_TABLE_ID "
        + "JOIN AD_COLUMN COL ON COL.AD_COLUMN_ID = CFG.AD_COLUMN_ID "
        + "JOIN AD_COLUMN COL_PK ON COL_PK.AD_TABLE_ID = TBL.AD_TABLE_ID AND COL_PK.ISKEY = 'Y'";

    List<Map<String, String>> tableConfigs = new ArrayList<>();
    try (PreparedStatement ps = cp.getPreparedStatement(configSql);
         ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        Map<String, String> cfg = new HashMap<>();
        cfg.put(TABLE_NAME, rs.getString("TABLENAME"));
        cfg.put("columnName", rs.getString("COLUMNNAME"));
        cfg.put("pkColumnName", rs.getString("PK_COLUMNNAME"));
        tableConfigs.add(cfg);
      }
    }
    return tableConfigs;
  }

  /**
   * Decides whether constraints must be (re)created for a table by checking:
   * configuration completeness, XML changes, and partitioning state.
   * Executes schema diffs and FK/PK SQL with backup/restore safety.
   */
  private boolean processTableConfig(ConnectionProvider cp, Map<String, String> cfg) throws Exception {
    String tableName = cfg.get(TABLE_NAME);
    String partitionCol = cfg.get("columnName");
    String pkCol = cfg.get("pkColumnName");

    log4j.info("DATA FROM ETARC_TABLE_CONFIG: tableName: {} - partitionCol: {} - pkCol: {}",
        tableName, partitionCol, pkCol);

    boolean isIncomplete = isBlank(tableName) || isBlank(partitionCol) || isBlank(pkCol);
    List<File> xmlFiles = isIncomplete ? Collections.emptyList() : findTableXmlFiles(tableName);

    boolean isUnchanged = !isIncomplete &&
        !(new TableDefinitionComparator()).isTableDefinitionChanged(tableName, cp, xmlFiles);

    // If XML for this table didn't change but other XMLs now reference it, still process.
    if (isUnchanged) {
      try {
        if (hasForeignReferencesInXml(tableName)) {
          log4j.info("Detected external XML foreign-key references to {} — forcing processing.", tableName);
          isUnchanged = false;
        }
      } catch (Exception e) {
        // Be conservative on errors: if detection fails, do not block processing
        log4j.info("Failed to scan for external foreign-key references for {}: {}", tableName, e.getMessage());
      }
    }

    boolean isPartitioned = isTablePartitioned(cp, tableName);
    List<String> pkCols = getPrimaryKeyColumns(cp, tableName);
    boolean firstPartitionRun = isPartitioned && pkCols.isEmpty();

    log4j.info("Table {} partitioned = {} existing PK cols = {}", tableName, isPartitioned, pkCols);

    if (shouldSkipTable(isIncomplete, firstPartitionRun, isUnchanged)) {
      logSkipReason(isIncomplete, tableName, pkCol, partitionCol);
      return false;
    }

    log4j.info("Recreating constraints for {} (firstRun = {}, xmlChanged = {})",
        tableName, firstPartitionRun, !isUnchanged);

    String tableSql = buildConstraintSql(tableName, cp, pkCol, partitionCol);

    try {
      TableDefinitionComparator.ColumnDiff diff =
          (new TableDefinitionComparator()).diffTableDefinition(tableName, cp, xmlFiles);

      StringBuilder alterSql = getAlterSql(diff, tableName);

      if (!alterSql.isEmpty()) {
        executeSqlWithBackup(cp, tableName, isPartitioned, alterSql.toString());
      }
      if (!isBlank(tableSql)) {
        executeSqlWithBackup(cp, tableName, isPartitioned, tableSql);
      }
      return true;
    } catch (Exception e) {
      log4j.error("Failed to persist XML schema changes for {}: {}", tableName, e.getMessage(), e);
      // Fallback: still try constraint SQL
      try {
        executeSqlWithBackup(cp, tableName, isPartitioned, tableSql);
      } catch (Exception e2) {
        log4j.error("Failed executing fallback constraint SQL for {}: {}", tableName, e2.getMessage(), e2);
      }
      return false;
    }
  }

  /**
   * Builds ALTER statements from XML diff: adds missing columns and drops removed ones.
   */
  private StringBuilder getAlterSql(TableDefinitionComparator.ColumnDiff diff, String tableName) {
    StringBuilder alterSql = new StringBuilder();
    for (Map.Entry<String, TableDefinitionComparator.ColumnDefinition> e : diff.added.entrySet()) {
      String col = e.getKey();
      TableDefinitionComparator.ColumnDefinition def = e.getValue();
      String sqlType = mapXmlTypeToSql(def.getDataType(), def.getLength()); // best-effort
      alterSql.append(String.format(
          "ALTER TABLE public.%s ADD COLUMN IF NOT EXISTS %s %s %s;\n",
          tableName, col, sqlType, def.isNullable() ? "" : "NOT NULL"));
    }
    for (Map.Entry<String, TableDefinitionComparator.ColumnDefinition> e : diff.removed.entrySet()) {
      String col = e.getKey();
      alterSql.append(String.format(
          "ALTER TABLE public.%s DROP COLUMN IF EXISTS %s CASCADE;\n",
          tableName, col));
    }
    return alterSql;
  }

  /**
   * Scans table XMLs for any {@code <foreign-key foreignTable="...">} referencing {@code tableName}.
   * Uses a lightweight "scan only new/modified" heuristic via {@code lastProcessed}.
   */
  private boolean hasForeignReferencesInXml(String tableName) throws NoSuchFileException {
    Timestamp lastProcessed = getLastProcessed(getConnectionProvider(), tableName);
    for (File xml : collectAllXmlFiles()) {
      if (!shouldScan(xml, lastProcessed)) continue;
      if (fileReferencesTable(xml, tableName)) {
        setLastProcessed(getConnectionProvider(), tableName); // avoid repeated forced runs
        return true;
      }
    }
    return false;
  }

  /**
   * True if the file should be scanned (always for paths containing {@code modifiedTables};
   * otherwise only if its mtime is newer than {@code lastProcessed}).
   */
  private boolean shouldScan(File xml, Timestamp lastProcessed) {
    boolean inModified = StringUtils.contains(xml.getAbsolutePath(), "modifiedTables");
    if (inModified) return true;
    if (lastProcessed == null) return true;
    return xml.lastModified() > lastProcessed.getTime();
  }

  /**
   * Returns {@code true} if the XML contains a {@code <foreign-key>} that references {@code tableName}.
   * Parsing errors are logged and treated as "no reference".
   */
  private boolean fileReferencesTable(File xml, String tableName) {
    try {
      Document doc = getDocument(xml);
      return hasForeignKeyRef(doc, tableName, xml);
    } catch (Exception e) {
      logUnparseableXML(xml, e);
      return false;
    }
  }

  /**
   * DOM scan for any matching {@code <foreign-key>} element; logs the number scanned at DEBUG.
   */
  private boolean hasForeignKeyRef(Document doc, String tableName, File sourceXml) {
    NodeList fkList = doc.getElementsByTagName("foreign-key");
    for (int i = 0; i < fkList.getLength(); i++) {
      Element fkEl = (Element) fkList.item(i);
      if (foreignKeyElementMatches(fkEl, tableName, sourceXml)) {
        return true;
      }
    }
    log4j.debug("Scanned {} and found {} foreign-key elements",
        sourceXml.getAbsolutePath(), fkList.getLength());
    return false;
  }

  /**
   * Returns {@code true} if the given {@code <foreign-key>} element references {@code tableName}.
   * Checks the fast path attribute {@code foreignTable} first; then inspects all attributes.
   */
  private boolean foreignKeyElementMatches(Element fkEl, String tableName, File sourceXml) {
    String foreignTable = fkEl.getAttribute("foreignTable");
    if (tableName.equalsIgnoreCase(foreignTable)) {
      log4j.debug("Found external FK reference to {} in file {} (attribute foreignTable)",
          tableName, sourceXml.getAbsolutePath());
      return true;
    }
    NamedNodeMap attrs = fkEl.getAttributes();
    for (int a = 0; a < attrs.getLength(); a++) {
      String attrName = attrs.item(a).getNodeName();
      String attrVal = attrs.item(a).getNodeValue();
      if (tableName.equalsIgnoreCase(attrVal)) {
        log4j.debug("Found external FK reference to {} in file {} (attribute {})",
            tableName, sourceXml.getAbsolutePath(), attrName);
        return true;
      }
    }
    return false;
  }

  // --- Backup helpers ---

  /**
   * Creates backup schema/tables if missing. Best-effort, non-throwing.
   */
  private void ensureBackupInfrastructure(ConnectionProvider cp) {
    String createSchema = "CREATE SCHEMA IF NOT EXISTS " + BACKUP_SCHEMA + ";";
    String createMeta = "CREATE TABLE IF NOT EXISTS " + BACKUP_SCHEMA + "." + BACKUP_METADATA_TABLE
        + " (backup_name TEXT PRIMARY KEY, table_name TEXT, created_at TIMESTAMP);";
    String createProc = "CREATE TABLE IF NOT EXISTS " + BACKUP_SCHEMA + ".processing_metadata"
        + " (table_name TEXT PRIMARY KEY, last_processed TIMESTAMP);";
    try (PreparedStatement ps = cp.getPreparedStatement(createSchema + " " + createMeta + " " + createProc)) {
      ps.executeUpdate();
    } catch (Exception e) {
      log4j.warn("Could not ensure backup infrastructure: {}", e.getMessage());
    }
  }

  /**
   * Returns the last processed timestamp for a table, or {@code null} if unknown.
   */
  private Timestamp getLastProcessed(ConnectionProvider cp, String tableName) {
    String sql = "SELECT last_processed FROM " + BACKUP_SCHEMA + ".processing_metadata WHERE lower(table_name) = lower(?)";
    try (PreparedStatement ps = cp.getPreparedStatement(sql)) {
      ps.setString(1, tableName);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          return rs.getTimestamp(1);
        }
      }
    } catch (Exception e) {
      log4j.warn("Could not read last_processed for {}: {}", tableName, e.getMessage());
    }
    return null;
  }

  /**
   * Sets {@code last_processed = now()} for the given table. Best-effort.
   */
  private void setLastProcessed(ConnectionProvider cp, String tableName) {
    String sql = "INSERT INTO " + BACKUP_SCHEMA + ".processing_metadata (table_name, last_processed) VALUES (?, now())"
        + " ON CONFLICT (table_name) DO UPDATE SET last_processed = EXCLUDED.last_processed";
    try (PreparedStatement ps = cp.getPreparedStatement(sql)) {
      ps.setString(1, tableName);
      ps.executeUpdate();
    } catch (Exception e) {
      log4j.warn("Could not set last_processed for {}: {}", tableName, e.getMessage());
    }
  }

  /**
   * Cleans old/excess backups across all tables and logs a summary.
   */
  private void cleanupExcessBackups(ConnectionProvider cp) {
    logStartCleanup();

    List<String> tableNames = fetchDistinctTableNames(cp);
    if (tableNames.isEmpty()) {
      log4j.info("No tables found in {}.{}", BACKUP_SCHEMA, BACKUP_METADATA_TABLE);
      return;
    }

    int totalDeleted = 0;
    for (String tableName : tableNames) {
      totalDeleted += cleanupBackupsForTable(cp, tableName);
    }
    logSummary(totalDeleted);
  }

  /**
   * Writes initial retention policy line.
   */
  private void logStartCleanup() {
    log4j.info(
        "Starting backup cleanup - will keep {} most recent backups per table and delete backups older than {} days",
        MAX_BACKUPS_PER_TABLE, BACKUP_RETENTION_DAYS);
  }

  /**
   * Lists distinct logical table names with backups recorded.
   */
  private List<String> fetchDistinctTableNames(ConnectionProvider cp) {
    String sql = "SELECT DISTINCT table_name FROM " + BACKUP_SCHEMA + "." + BACKUP_METADATA_TABLE;
    List<String> tables = new ArrayList<>();
    try (PreparedStatement ps = cp.getPreparedStatement(sql);
         ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        tables.add(rs.getString(1));
      }
    } catch (Exception e) {
      log4j.warn("Failed to list tables in backup metadata: {}", e.getMessage());
    }
    return tables;
  }

  /**
   * Deletes backups for a single logical table according to both policies:
   * keep only the N most recent; delete any older than the retention window.
   *
   * @return number of backups selected for deletion (not necessarily successfully dropped)
   */
  private int cleanupBackupsForTable(ConnectionProvider cp, String tableName) {
    List<String> backupsToDelete = findBackupsToDelete(cp, tableName);
    if (backupsToDelete.isEmpty()) {
      return 0;
    }
    for (String backupName : backupsToDelete) {
      dropBackupTableQuietly(cp, backupName);
      deleteBackupMetadataQuietly(cp, backupName);
    }
    log4j.info("Cleaned up {} backups for table {} (excess count + older than {} days)",
        backupsToDelete.size(), tableName, BACKUP_RETENTION_DAYS);
    return backupsToDelete.size();
  }

  /**
   * Computes which backup tables to delete for a logical table.
   */
  private List<String> findBackupsToDelete(ConnectionProvider cp, String tableName) {
    String sql =
        "SELECT backup_name FROM (" +
            "  SELECT backup_name, " +
            "         ROW_NUMBER() OVER (ORDER BY created_at DESC) AS rn, " +
            "         created_at " +
            "  FROM " + BACKUP_SCHEMA + "." + BACKUP_METADATA_TABLE + " " +
            "  WHERE table_name = ?" +
            ") ranked " +
            "WHERE rn > " + MAX_BACKUPS_PER_TABLE + " " +
            "   OR created_at < now() - interval '" + BACKUP_RETENTION_DAYS + " days'";

    List<String> backupNames = new ArrayList<>();
    try (PreparedStatement ps = cp.getPreparedStatement(sql)) {
      ps.setString(1, tableName);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          backupNames.add(rs.getString(1));
        }
      }
    } catch (Exception e) {
      log4j.warn("Failed to compute backups to delete for {}: {}", tableName, e.getMessage());
    }
    return backupNames;
  }

  /**
   * Drops a backup table; logs and continues on failure.
   */
  private void dropBackupTableQuietly(ConnectionProvider cp, String backupName) {
    String dropSql = "DROP TABLE IF EXISTS " + BACKUP_SCHEMA + "." + backupName;
    try (PreparedStatement drop = cp.getPreparedStatement(dropSql)) {
      drop.executeUpdate();
      log4j.debug("Dropped backup table: {}", backupName);
    } catch (Exception e) {
      log4j.warn("Failed to drop backup table {}: {}", backupName, e.getMessage());
    }
  }

  /**
   * Deletes the metadata row for a backup; logs and continues on failure.
   */
  private void deleteBackupMetadataQuietly(ConnectionProvider cp, String backupName) {
    String deleteMetaSql =
        "DELETE FROM " + BACKUP_SCHEMA + "." + BACKUP_METADATA_TABLE + " WHERE backup_name = ?";
    try (PreparedStatement del = cp.getPreparedStatement(deleteMetaSql)) {
      del.setString(1, backupName);
      del.executeUpdate();
    } catch (Exception e) {
      log4j.warn("Failed to delete metadata for backup {}: {}", backupName, e.getMessage());
    }
  }

  /**
   * Logs the cleanup summary.
   */
  private void logSummary(int totalDeleted) {
    if (totalDeleted > 0) {
      log4j.info("Total cleanup: removed {} old/excess backup tables", totalDeleted);
    } else {
      log4j.info("No excess or old backups found to clean up");
    }
  }

  /**
   * Cleans old/excess backups for a specific logical table only.
   */
  private void cleanupExcessBackupsForTable(ConnectionProvider cp, String tableName) {
    String deleteMetaSql = "DELETE FROM " + BACKUP_SCHEMA + "." + BACKUP_METADATA_TABLE + " WHERE backup_name = ?";

    try {
      String selectBackupsToDeleteSql =
          "SELECT backup_name FROM (" +
              "  SELECT backup_name, " +
              "         ROW_NUMBER() OVER (ORDER BY created_at DESC) as rn, " +
              "         created_at " +
              "  FROM " + BACKUP_SCHEMA + "." + BACKUP_METADATA_TABLE +
              "  WHERE table_name = ?" +
              ") ranked " +
              "WHERE rn > " + MAX_BACKUPS_PER_TABLE +
              "   OR created_at < now() - interval '" + BACKUP_RETENTION_DAYS + " days'";

      try (PreparedStatement psBackups = cp.getPreparedStatement(selectBackupsToDeleteSql)) {
        psBackups.setString(1, tableName);
        try (ResultSet rsBackups = psBackups.executeQuery()) {
          List<String> backupsToDelete = new ArrayList<>();
          while (rsBackups.next()) {
            backupsToDelete.add(rsBackups.getString(1));
          }

          for (String backupName : backupsToDelete) {
            try (PreparedStatement drop = cp.getPreparedStatement(
                "DROP TABLE IF EXISTS " + BACKUP_SCHEMA + "." + backupName)) {
              drop.executeUpdate();
              log4j.info("Dropped backup table for {}: {} (excess or > {} days old)",
                  tableName, backupName, BACKUP_RETENTION_DAYS);
            } catch (Exception de) {
              log4j.warn("Failed to drop backup table {}: {}", backupName, de.getMessage());
            }

            try (PreparedStatement del = cp.getPreparedStatement(deleteMetaSql)) {
              del.setString(1, backupName);
              del.executeUpdate();
            } catch (Exception de2) {
              log4j.warn("Failed to delete metadata for backup {}: {}", backupName, de2.getMessage());
            }
          }

          if (!backupsToDelete.isEmpty()) {
            log4j.info(
                "Cleaned up {} backups for table {} after creating new backup (excess count + older than {} days)",
                backupsToDelete.size(), tableName, BACKUP_RETENTION_DAYS);
          }
        }
      }
    } catch (Exception e) {
      log4j.warn("Failed to cleanup excess backups for table {}: {}", tableName, e.getMessage());
    }
  }

  /**
   * Creates a snapshot table in {@code BACKUP_SCHEMA} and records it in metadata.
   */
  private String createTableBackup(ConnectionProvider cp, String tableName) throws Exception {
    String backupName = tableName.toLowerCase() + "_backup_" + System.currentTimeMillis();
    String sql = "CREATE TABLE " + BACKUP_SCHEMA + "." + backupName + " AS TABLE public." + tableName + ";";
    String metaSql = "INSERT INTO " + BACKUP_SCHEMA + "." + BACKUP_METADATA_TABLE
        + " (backup_name, table_name, created_at) VALUES (?, ?, ?)";
    try (PreparedStatement ps = cp.getPreparedStatement(sql)) {
      ps.executeUpdate();
    }
    try (PreparedStatement ps = cp.getPreparedStatement(metaSql)) {
      ps.setString(1, backupName);
      ps.setString(2, tableName);
      ps.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
      ps.executeUpdate();
    }
    cleanupExcessBackupsForTable(cp, tableName);
    return backupName;
  }

  /**
   * Best-effort restore: truncates target table and inserts from the backup.
   */
  private void restoreBackup(ConnectionProvider cp, String tableName, String backupName) throws Exception {
    String truncate = "TRUNCATE TABLE public." + tableName + " RESTART IDENTITY CASCADE;";
    String insert = "INSERT INTO public." + tableName + " SELECT * FROM " + BACKUP_SCHEMA + "." + backupName + ";";
    try (PreparedStatement ps = cp.getPreparedStatement(truncate + " " + insert)) {
      ps.executeUpdate();
    }
  }

  /**
   * Executes the given SQL against the table. If the table is partitioned, creates a backup
   * beforehand and restores it on failure.
   */
  private void executeSqlWithBackup(ConnectionProvider cp, String tableName, boolean isPartitioned, String sql)
      throws Exception {
    String backupName = null;
    try {
      if (isPartitioned) {
        backupName = createTableBackup(cp, tableName);
        log4j.info("Created backup {} for table {}", backupName, tableName);
      }
      if (isBlank(sql)) {
        log4j.info("No SQL to execute for table {}", tableName);
        return;
      }
      try (PreparedStatement ps = cp.getPreparedStatement(sql)) {
        ps.executeUpdate();
      }
    } catch (Exception e) {
      log4j.error("Error executing SQL for table {}: {}", tableName, e.getMessage(), e);
      if (backupName != null) {
        try {
          restoreBackup(cp, tableName, backupName);
          log4j.info("Restored backup {} to table {} after failure", backupName, tableName);
        } catch (Exception re) {
          log4j.error("Failed to restore backup {} for table {}: {}", backupName, tableName, re.getMessage(), re);
        }
      }
      throw e;
    }
  }

  /**
   * Best-effort mapping from XML datatype to PostgreSQL type.
   */
  private String mapXmlTypeToSql(String xmlType, Integer length) {
    if (xmlType == null) return "text";
    String t = xmlType.toLowerCase();
    return switch (t) {
      case "varchar", "character varying", "string" ->
          length != null && length > 0 ? "varchar(" + length + ")" : "text";
      case "int", "integer" -> "integer";
      case "bigint" -> "bigint";
      case "timestamp", "datetime" -> "timestamp without time zone";
      case "boolean", "bool" -> "boolean";
      case "numeric", "decimal" -> "numeric";
      default -> t;
    };
  }

  /**
   * Skip logic used by {@link #processTableConfig(ConnectionProvider, Map)}.
   */
  private boolean shouldSkipTable(boolean isIncomplete, boolean firstPartitionRun, boolean isUnchanged) {
    return isIncomplete || (!firstPartitionRun && isUnchanged);
  }

  /**
   * Logs the reason a table was skipped.
   */
  private void logSkipReason(boolean isIncomplete, String tableName, String pkCol, String partitionCol) {
    if (isIncomplete) {
      log4j.warn("Skipping incomplete configuration for table {} (pk = {}, partition = {})",
          tableName, pkCol, partitionCol);
    } else {
      log4j.info("Skipping {}: already processed and no XML changes", tableName);
    }
  }

  /**
   * True if the table is partitioned in PostgreSQL.
   */
  private boolean isTablePartitioned(ConnectionProvider cp, String tableName) throws Exception {
    try (PreparedStatement ps = cp.getPreparedStatement(
        "SELECT 1 FROM pg_partitioned_table WHERE partrelid = to_regclass(?)")) {
      ps.setString(1, tableName);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    }
  }

  /**
   * Returns the list of PK columns for the given table.
   */
  private List<String> getPrimaryKeyColumns(ConnectionProvider cp, String tableName) throws Exception {
    List<String> pkCols = new ArrayList<>();
    String sql = "SELECT a.attname FROM pg_index i JOIN pg_attribute a "
        + "ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
        + "WHERE i.indrelid = to_regclass(?) AND i.indisprimary";
    try (PreparedStatement ps = cp.getPreparedStatement(sql)) {
      ps.setString(1, tableName);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          pkCols.add(rs.getString(1));
        }
      }
    }
    return pkCols;
  }

  /**
   * True if a column exists on the given table.
   */
  private boolean columnExists(ConnectionProvider cp, String tableName, String columnName) throws Exception {
    String sql = "SELECT 1 FROM information_schema.columns " +
        "WHERE lower(table_name) = lower(?) AND lower(column_name) = lower(?)";
    try (PreparedStatement ps = cp.getPreparedStatement(sql)) {
      ps.setString(1, tableName);
      ps.setString(2, columnName);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    }
  }

  /**
   * True if a constraint with the given name exists on the table.
   */
  private boolean constraintExists(ConnectionProvider cp, String tableName, String constraintName) throws Exception {
    String sql = "SELECT 1 FROM information_schema.table_constraints " +
        "WHERE lower(table_name) = lower(?) AND lower(constraint_name) = lower(?)";
    try (PreparedStatement ps = cp.getPreparedStatement(sql)) {
      ps.setString(1, tableName);
      ps.setString(2, constraintName);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    }
  }

  /**
   * Collects directories that may contain table XMLs.
   * Returns only existing directories; never {@code null}.
   */
  private List<File> collectTableDirs() throws NoSuchFileException {
    List<File> dirs = new ArrayList<>();
    File root = new File(getSourcePath());
    for (String mod : moduleDirs) {
      File modBase = new File(root, mod);
      if (!modBase.isDirectory()) continue;
      dirs.add(new File(modBase, SRC_DB_DATABASE_MODEL_TABLES));
      File[] subDirs = modBase.listFiles(File::isDirectory);
      if (subDirs != null) {
        for (File sd : subDirs) {
          dirs.add(new File(sd, SRC_DB_DATABASE_MODEL_TABLES));
        }
      }
      dirs.add(new File(modBase, SRC_DB_DATABASE_MODEL_MODIFIED_TABLES));
      if (subDirs != null) {
        for (File sd : subDirs) {
          dirs.add(new File(sd, SRC_DB_DATABASE_MODEL_MODIFIED_TABLES));
        }
      }
    }
    dirs.add(new File(root, SRC_DB_DATABASE_MODEL_TABLES));
    return dirs.stream().filter(File::isDirectory).collect(Collectors.toList());
  }

  /* =========================
     Primary table helpers
     ========================= */

  /**
   * Finds XML files that define {@code tableName} either by filename (fast path)
   * or by containing a {@code <table name="...">} with the same name.
   * Deduplicates while preserving first-seen order.
   */
  public List<File> findTableXmlFiles(String tableName) throws NoSuchFileException {
    String targetLower = normalizeLower(tableName);
    Set<File> matches = new LinkedHashSet<>();
    for (File xml : collectAllXmlFiles()) {
      if (fileNameMatches(xml, targetLower) || containsTableDefinition(xml, tableName)) {
        matches.add(xml);
      }
    }
    return new ArrayList<>(matches);
  }

  /**
   * Returns a flattened list of all {@code .xml} files under the collected directories.
   */
  private List<File> collectAllXmlFiles() throws NoSuchFileException {
    List<File> xmls = new ArrayList<>();
    for (File dir : collectTableDirs()) {
      Collections.addAll(xmls, listXmlFiles(dir));
    }
    return xmls;
  }

  /**
   * Lists {@code .xml} files inside {@code dir}. Returns an empty array if none/inaccessible.
   */
  private File[] listXmlFiles(File dir) {
    File[] files = dir.listFiles(f -> f.isFile() && f.getName().endsWith(".xml"));
    return files != null ? files : new File[0];
  }

  /* =========================
     FK scanning & generation
     ========================= */

  /**
   * Filename fast path: equals {@code <table>.xml} (case-insensitive).
   */
  private boolean fileNameMatches(File xml, String targetLower) {
    return (xml != null) && xml.getName().equalsIgnoreCase(targetLower + ".xml");
  }

  /**
   * True if the XML contains a {@code <table name="...">} for {@code tableName}.
   */
  private boolean containsTableDefinition(File xml, String tableName) {
    try {
      Document doc = getDocument(xml);
      NodeList tableNodes = doc.getElementsByTagName(TABLE);
      return nodeListHasTableName(tableNodes, tableName);
    } catch (Exception e) {
      logUnparseableXML(xml, e);
      return false;
    }
  }

  /**
   * Scans a {@link NodeList} of {@code <table>} elements for a matching {@code name}.
   */
  private boolean nodeListHasTableName(NodeList tableNodes, String tableName) {
    for (int i = 0; i < tableNodes.getLength(); i++) {
      Element el = (Element) tableNodes.item(i);
      if (tableName.equalsIgnoreCase(el.getAttribute("name"))) {
        return true;
      }
    }
    return false;
  }

  /* =========================
     XML utils
     ========================= */

  /**
   * Lowercases with {@link Locale#ROOT} to avoid locale-specific surprises.
   */
  private String normalizeLower(String s) {
    return s == null ? null : s.toLowerCase(Locale.ROOT);
  }

  /**
   * Builds SQL to (re)create PK/FK constraints for {@code tableName}, handling partitioned vs non-partitioned.
   */
  public String buildConstraintSql(String tableName, ConnectionProvider cp, String pkField,
      String partitionField) throws Exception {

    boolean isPartitioned = isPartitioned(cp, tableName);
    String pkName = resolvePrimaryKeyName(tableName); // throws if not found

    StringBuilder sql = new StringBuilder(512);
    appendPrimaryTableSql(sql, tableName, pkName, pkField, partitionField, isPartitioned);
    FkContext ctx = new FkContext(cp, tableName, pkField, partitionField, isPartitioned);
    appendAllForeignKeySql(sql, ctx);
    return sql.toString();
  }

  /**
   * Appends DROP/ADD PK for the target table.
   */
  private void appendPrimaryTableSql(StringBuilder sql, String tableName, String pkName,
      String pkField, String partitionField, boolean isPartitioned) {
    sql.append(String.format(DROP_PK, tableName, pkName));
    if (isPartitioned) {
      sql.append(String.format(ADD_PK_PARTITIONED, tableName, pkName, pkField, partitionField));
    } else {
      sql.append(String.format(ADD_PK_SIMPLE, tableName, pkName, pkField));
    }
  }

  /**
   * Resolves the PK constraint name from table XML or throws {@link OBException}.
   */
  private String resolvePrimaryKeyName(String tableName) throws OBException, NoSuchFileException {
    List<File> xmls = findTableXmlFiles(tableName);
    if (xmls.isEmpty()) {
      throw new OBException("Entity XML file for " + tableName + " not found.");
    }
    String pkName = findPrimaryKey(xmls);
    if (pkName == null) {
      throw new OBException("Primary Key for entity " + tableName + " not found in XML.");
    }
    return pkName;
  }

  /**
   * Safe partitioned-table check used by {@link #buildConstraintSql}.
   */
  private boolean isPartitioned(ConnectionProvider cp, String tableName) {
    String sql = "SELECT 1 FROM pg_partitioned_table WHERE partrelid = to_regclass(?)";
    try (PreparedStatement ps = cp.getPreparedStatement(sql)) {
      ps.setString(1, tableName);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    } catch (Exception e) {
      log4j.warn("Could not check partitioning for {}: {}", tableName, e.getMessage());
      return false;
    }
  }

  /**
   * Scans all known table XMLs and appends FK SQL for children referencing the parent in {@code ctx}.
   */
  private void appendAllForeignKeySql(StringBuilder sql, FkContext ctx) {
    for (File dir : collectTableDirsSafe()) {
      for (File xml : listXmlFiles(dir)) {
        processXmlForForeignKeys(sql, ctx, xml);
      }
    }
  }

  /* =========================
     Existence checks (safe)
     ========================= */

  /**
   * Processes one XML file: detects child table and appends FK SQL if it references {@code ctx.parentTable}.
   */
  private void processXmlForForeignKeys(StringBuilder sql, FkContext ctx, File xml) {
    try {
      Document doc = getDocument(xml);
      Element tableEl = singleTableElementOrNull(doc);
      if (tableEl == null || shouldSkipTableElement(tableEl, ctx.parentTable)) return;

      String childTable = tableEl.getAttribute("name").toUpperCase();
      NodeList fkList = tableEl.getElementsByTagName("foreign-key");

      for (int i = 0; i < fkList.getLength(); i++) {
        Element fkEl = (Element) fkList.item(i);
        if (!referencesTarget(fkEl, ctx.parentTable)) continue;

        String fkName = fkEl.getAttribute("name");
        String localCol = firstLocalColumn(fkEl);
        if (StringUtils.isBlank(fkName) || StringUtils.isBlank(localCol)) continue;

        appendFkSqlForChild(sql, ctx, new ChildRef(childTable, fkName, localCol));
      }
    } catch (Exception e) {
      log4j.error("Error processing XML file: {}", xml.getAbsolutePath(), e);
    }
  }

  /**
   * Appends SQL for a single child table referencing the target.
   * If the parent is partitioned, conditionally adds the helper column and recreates FK only if missing.
   * If not partitioned, always drops & recreates the simple FK.
   */
  private void appendFkSqlForChild(StringBuilder sql, FkContext ctx, ChildRef child) {
    if (ctx.parentIsPartitioned) {
      String helperCol = "etarc_" + ctx.partitionField + "__" + child.fkName;

      boolean colExists = existsSafe(() -> columnExists(ctx.cp, child.childTable, helperCol),
          "columnExists", helperCol, child.childTable);
      boolean fkExists = existsSafe(() -> constraintExists(ctx.cp, child.childTable, child.fkName),
          "constraintExists", child.fkName, child.childTable);

      if (colExists && fkExists) {
        log4j.debug("Skipping creation of helper column {} and FK {} on {} because both already exist",
            helperCol, child.fkName, child.childTable);
        return;
      }
      if (!colExists) {
        sql.append(String.format(ADD_COL_IF_NOT_EXISTS, child.childTable, helperCol));
        sql.append(String.format(UPDATE_HELPER_COL, child.childTable, helperCol,
            ctx.partitionField, ctx.parentTable, ctx.pkField,
            child.childTable, child.localCol, child.childTable, helperCol));
      }
      if (!fkExists) {
        sql.append(String.format(DROP_FK, child.childTable, child.fkName));
        sql.append(String.format(ADD_FK_PARTITIONED, child.childTable, child.fkName, child.localCol,
            helperCol, ctx.parentTable, ctx.pkField, ctx.partitionField));
      }
    } else {
      sql.append(String.format(DROP_FK, child.childTable, child.fkName));
      sql.append(String.format(ADD_FK_SIMPLE, child.childTable, child.fkName,
          child.localCol, ctx.parentTable, ctx.pkField));
    }
  }

  /* =========================
     SQL templates helpers
     ========================= */

  /**
   * Returns the single {@code <table>} element if exactly one exists; otherwise {@code null}.
   */
  private Element singleTableElementOrNull(Document doc) {
    NodeList tables = doc.getElementsByTagName(TABLE);
    if (tables.getLength() != 1) return null;
    return (Element) tables.item(0);
  }

  /**
   * True if the element represents a view or is the same as the target.
   */
  private boolean shouldSkipTableElement(Element tableEl, String targetTable) {
    if (Boolean.parseBoolean(tableEl.getAttribute("isView"))) return true;
    return targetTable.equalsIgnoreCase(tableEl.getAttribute("name"));
  }

  /**
   * True if the {@code <foreign-key>} points to {@code targetTable}.
   */
  private boolean referencesTarget(Element fkEl, String targetTable) {
    return targetTable.equalsIgnoreCase(fkEl.getAttribute("foreignTable"));
  }

  /**
   * Returns the first {@code local} attribute of a {@code <reference>} child (single-column FK).
   */
  private String firstLocalColumn(Element fkEl) {
    NodeList refList = fkEl.getElementsByTagName("reference");
    if (refList.getLength() == 0) return null;
    Element refEl = (Element) refList.item(0);
    return refEl.getAttribute("local");
  }

  /**
   * Collects table directories; returns empty list (not null) on failure and logs a warning.
   */
  private List<File> collectTableDirsSafe() {
    try {
      return collectTableDirs();
    } catch (Exception e) {
      log4j.warn("Could not collect table dirs: {}", e.getMessage());
      return List.of();
    }
  }

  /**
   * Wraps boolean existence checks that may throw; logs and returns {@code false} on failure.
   */
  private boolean existsSafe(Check op, String label, String name, String onTable) {
    try {
      return op.get();
    } catch (Exception e) {
      log4j.warn("Could not run {} for {} on {}: {}", label, name, onTable, e.getMessage());
      return false;
    }
  }

  @FunctionalInterface
  private interface Check {
    boolean get() throws Exception;
  }

  // =========================
  // Parameter objects
  // =========================

  /**
   * Immutable context describing the parent (referenced) table and DB access.
   */
  private static final class FkContext {
    final ConnectionProvider cp;
    final String parentTable;
    final String pkField;
    final String partitionField;
    final boolean parentIsPartitioned;

    FkContext(ConnectionProvider cp, String parentTable, String pkField,
        String partitionField, boolean parentIsPartitioned) {
      this.cp = cp;
      this.parentTable = parentTable;
      this.pkField = pkField;
      this.partitionField = partitionField;
      this.parentIsPartitioned = parentIsPartitioned;
    }
  }

  /**
   * Immutable holder for a single child-table FK reference.
   */
  private static final class ChildRef {
    final String childTable;
    final String fkName;
    final String localCol;

    ChildRef(String childTable, String fkName, String localCol) {
      this.childTable = childTable;
      this.fkName = fkName;
      this.localCol = localCol;
    }
  }
}
