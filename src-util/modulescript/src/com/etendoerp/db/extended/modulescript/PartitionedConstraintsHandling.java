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

import java.nio.file.NoSuchFileException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openbravo.database.ConnectionProvider;
import org.openbravo.modulescript.ModuleScript;

import com.etendoerp.db.extended.utils.BackupManager;
import com.etendoerp.db.extended.utils.ConstraintProcessor;
import com.etendoerp.db.extended.utils.SqlBuilder;
import com.etendoerp.db.extended.utils.TableDefinitionComparator;
import com.etendoerp.db.extended.utils.TriggerManager;
import com.etendoerp.db.extended.utils.XmlTableProcessor;

/**
 * Main module script for handling partitioned table constraints in PostgreSQL.
 *
 * <p>This class coordinates the management of database constraints (primary keys and foreign keys)
 * for partitioned tables. It ensures that constraints are properly recreated when table definitions
 * change and handles the complex scenarios that arise when working with PostgreSQL table partitioning.
 *
 * <h3>Key Responsibilities:</h3>
 * <ul>
 *   <li>Load table configurations from {@code ETARC_TABLE_CONFIG}</li>
 *   <li>Detect when table definitions have changed in XML files</li>
 *   <li>Manage database backups before making schema changes</li>
 *   <li>Coordinate constraint recreation through specialized utility classes</li>
 *   <li>Handle both partitioned and non-partitioned table scenarios</li>
 * </ul>
 *
 * <h3>Architecture:</h3>
 * <p>This class has been refactored to follow the Single Responsibility Principle,
 * delegating specific tasks to specialized utility classes in the {@code utils} package:
 * <ul>
 *   <li>{@link BackupManager} - Database backup operations</li>
 *   <li>{@link XmlTableProcessor} - XML table definition processing</li>
 *   <li>{@link ConstraintProcessor} - Constraint validation and SQL generation</li>
 *   <li>{@link SqlBuilder} - SQL statement construction</li>
 *   <li>{@link TriggerManager} - Database trigger management</li>
 * </ul>
 *
 * <h3>Processing Flow:</h3>
 * <ol>
 *   <li>Initialize component instances</li>
 *   <li>Load table configurations from database</li>
 *   <li>Setup backup infrastructure</li>
 *   <li>For each configured table:
 *     <ul>
 *       <li>Check if table definition has changed</li>
 *       <li>Determine if constraint recreation is needed</li>
 *       <li>Generate and execute necessary SQL statements</li>
 *       <li>Create backups before making changes</li>
 *     </ul>
 *   </li>
 *   <li>Report processing summary</li>
 * </ol>
 *
 * @author Futit Services S.L.
 * @see BackupManager
 * @see XmlTableProcessor
 * @see ConstraintProcessor
 * @see SqlBuilder
 * @see TriggerManager
 * @since ETP-2450
 */
public class PartitionedConstraintsHandling extends ModuleScript {

  public static final String SEPARATOR = "=======================================================";
  public static final String TABLE_NAME = "tableName";
  private static final Logger log4j = LogManager.getLogger();

  // Component instances
  private BackupManager backupManager;
  private XmlTableProcessor xmlProcessor;

  private SqlBuilder sqlBuilder;
  private ConstraintProcessor constraintProcessor;

  /**
   * Logs a visual separator line to help organize log output.
   *
   * <p>This method outputs a standardized separator line that makes it easier
   * to visually distinguish different sections of processing in the logs.
   */
  private static void logSeparator() {
    log4j.info(SEPARATOR);
  }

  /**
   * Initializes all component instances required for constraint processing.
   *
   * <p>This method creates and configures the specialized utility classes that handle
   * different aspects of the partition constraint management process. The components
   * are initialized with proper dependencies and configuration.
   *
   * <h4>Component Dependencies:</h4>
   * <ul>
   *   <li>{@code XmlTableProcessor} depends on {@code BackupManager} and source path</li>
   *   <li>{@code TriggerManager} depends on {@code XmlTableProcessor}</li>
   *   <li>{@code ConstraintProcessor} depends on all other components</li>
   * </ul>
   *
   * @throws NoSuchFileException
   *     if the source path for XML files cannot be found
   */
  private void initializeComponents() throws NoSuchFileException {
    TriggerManager triggerManager;
    this.backupManager = new BackupManager();
    this.xmlProcessor = new XmlTableProcessor(backupManager, getSourcePath());
    this.sqlBuilder = new SqlBuilder();
    triggerManager = new TriggerManager(xmlProcessor);
    this.constraintProcessor = new ConstraintProcessor(xmlProcessor, sqlBuilder, triggerManager);
  }

  @Override
  public void execute() {
    try {
      initializeComponents();
      ConnectionProvider cp = getConnectionProvider();
      List<Map<String, String>> tableConfigs = loadTableConfigs(cp);

      if (!tableConfigs.isEmpty()) {
        logSeparator();
        log4j.info("============== Partitioning process info ==============");
        logSeparator();
      }

      // Ensure backup infra and clear old backups before making changes
      backupManager.ensureBackupInfrastructure(cp);
      backupManager.cleanupExcessBackups(cp);

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
   * Performs a quick check to determine if any configured table is already partitioned.
   *
   * <p>This method provides an optimization by checking if any tables in the configuration
   * are already partitioned in PostgreSQL. This information is used later to determine
   * whether to show processing summaries and perform certain optimizations.
   *
   * @param cp
   *     the database connection provider
   * @param tableConfigs
   *     list of table configurations to check
   * @return {@code true} if at least one configured table is partitioned, {@code false} otherwise
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
   * Safe wrapper for checking if a specific table is partitioned.
   *
   * <p>This method provides exception-safe checking of table partitioning status.
   * If any error occurs during the check (e.g., table doesn't exist, connection issues),
   * it returns {@code false} rather than propagating the exception.
   *
   * @param cp
   *     the database connection provider
   * @param tableName
   *     the name of the table to check
   * @return {@code true} if the table is partitioned, {@code false} if not partitioned or on error
   */
  private boolean safeIsTablePartitioned(ConnectionProvider cp, String tableName) {
    try {
      return constraintProcessor.isTablePartitioned(cp, tableName);
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Safely processes a single table configuration with timing measurement.
   *
   * <p>This method wraps the main table processing logic with exception handling and
   * performance timing. It ensures that processing failures for one table don't affect
   * the processing of other tables.
   *
   * @param cp
   *     the database connection provider
   * @param cfg
   *     the table configuration map containing table name, partition column, and PK column
   * @return the elapsed processing time in milliseconds, or -1 if processing failed or was skipped
   */
  private long processTableSafely(ConnectionProvider cp, Map<String, String> cfg) throws TableProcessingException {
    long start = System.currentTimeMillis();
    try {
      boolean processed = processTableConfig(cp, cfg);
      return processed ? (System.currentTimeMillis() - start) : -1L;
    } catch (Exception e) {
      throw new TableProcessingException(e);
    }
  }

  /**
   * Loads table configurations from the {@code ETARC_TABLE_CONFIG} table.
   *
   * <p>This method queries the database to retrieve all configured tables that should
   * be processed for partition constraint management. Each configuration includes:
   * <ul>
   *   <li>Table name (from AD_TABLE)</li>
   *   <li>Partition column name (from AD_COLUMN)</li>
   *   <li>Primary key column name (from AD_COLUMN where ISKEY='Y')</li>
   * </ul>
   *
   * <p>The query joins several Etendo metadata tables to resolve the actual database
   * names from the configuration IDs stored in ETARC_TABLE_CONFIG.
   *
   * @param cp
   *     the database connection provider
   * @return a list of configuration maps, each containing "tableName", "columnName", and "pkColumnName"
   * @throws Exception
   *     if the database query fails or connection issues occur
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
   * Processes a single table configuration to determine and execute necessary constraint changes.
   *
   * <p>This is the core processing method that handles the complex logic of determining
   * whether constraints need to be recreated for a specific table. The decision process considers:
   *
   * <h4>Decision Factors:</h4>
   * <ul>
   *   <li><strong>Configuration completeness:</strong> All required fields (table, partition column, PK) must be present</li>
   *   <li><strong>XML changes:</strong> Whether the table definition has changed in XML files</li>
   *   <li><strong>External references:</strong> Whether other tables reference this table via foreign keys</li>
   *   <li><strong>Partitioning status:</strong> Whether the table is currently partitioned in PostgreSQL</li>
   *   <li><strong>First partition run:</strong> Special handling for newly partitioned tables</li>
   * </ul>
   *
   * <h4>Processing Steps:</h4>
   * <ol>
   *   <li>Validate configuration completeness</li>
   *   <li>Check for XML definition changes</li>
   *   <li>Scan for external foreign key references</li>
   *   <li>Determine current partitioning status</li>
   *   <li>Apply skip logic based on above factors</li>
   *   <li>Generate and execute SQL for schema changes</li>
   *   <li>Generate and execute SQL for constraint recreation</li>
   *   <li>Create backups before making changes</li>
   * </ol>
   *
   * @param cp
   *     the database connection provider
   * @param cfg
   *     the table configuration containing "tableName", "columnName", and "pkColumnName"
   * @return {@code true} if the table was processed successfully, {@code false} if skipped or failed
   * @throws Exception
   *     if database operations fail or XML processing encounters errors
   */
  private boolean processTableConfig(ConnectionProvider cp, Map<String, String> cfg) throws Exception {
    String tableName = cfg.get(TABLE_NAME);
    String partitionCol = cfg.get("columnName");
    String pkCol = cfg.get("pkColumnName");

    log4j.info("DATA FROM ETARC_TABLE_CONFIG: tableName: {} - partitionCol: {} - pkCol: {}",
        tableName, partitionCol, pkCol);

    boolean isIncomplete = StringUtils.isBlank(tableName) || StringUtils.isBlank(partitionCol) ||
        StringUtils.isBlank(pkCol);
    List<java.io.File> xmlFiles = isIncomplete ? java.util.Collections.emptyList() : xmlProcessor.findTableXmlFiles(
        tableName);

    boolean isUnchanged = !isIncomplete &&
        !(new TableDefinitionComparator()).isTableDefinitionChanged(tableName, cp, xmlFiles);

    // Check for external foreign key references
    if (isUnchanged) {
      try {
        if (xmlProcessor.hasForeignReferencesInXml(tableName, cp)) {
          log4j.info("Detected external XML foreign-key references to {} — forcing processing.", tableName);
          isUnchanged = false;
        }
      } catch (Exception e) {
        log4j.info("Failed to scan for external foreign-key references for {}: {}", tableName, e.getMessage());
      }
    }

    boolean isPartitioned = constraintProcessor.isTablePartitioned(cp, tableName);
    List<String> pkCols = constraintProcessor.getPrimaryKeyColumns(cp, tableName);
    boolean firstPartitionRun = isPartitioned && pkCols.isEmpty();

    log4j.info("Table {} partitioned = {} existing PK cols = {}", tableName, isPartitioned, pkCols);

    if (shouldSkipTable(isIncomplete, firstPartitionRun, isUnchanged)) {
      logSkipReason(isIncomplete, tableName, pkCol, partitionCol);
      return false;
    }

    log4j.info("Recreating constraints for {} (firstRun = {}, xmlChanged = {})",
        tableName, firstPartitionRun, !isUnchanged);

    String tableSql = constraintProcessor.buildConstraintSql(tableName, cp, pkCol, partitionCol);

    try {
      TableDefinitionComparator.ColumnDiff diff =
          (new TableDefinitionComparator()).diffTableDefinition(tableName, cp, xmlFiles);

      StringBuilder alterSql = sqlBuilder.getAlterSql(diff, tableName);

      if (alterSql.length() > 0) {
        backupManager.executeSqlWithBackup(cp, tableName, isPartitioned, alterSql.toString());
      }
      if (!StringUtils.isBlank(tableSql)) {
        backupManager.executeSqlWithBackup(cp, tableName, isPartitioned, tableSql);
      }
      return true;
    } catch (Exception e) {
      log4j.error("Failed to persist XML schema changes for {}: {}", tableName, e.getMessage(), e);
      // Fallback: still try constraint SQL
      try {
        backupManager.executeSqlWithBackup(cp, tableName, isPartitioned, tableSql);
      } catch (Exception e2) {
        log4j.error("Failed executing fallback constraint SQL for {}: {}", tableName, e2.getMessage(), e2);
        throw new TableProcessingException(e2);
      }
      return false;
    }
  }

  /**
   * Determines whether table processing should be skipped based on various conditions.
   *
   * <p>This method encapsulates the logic for deciding when to skip constraint processing
   * for a table. A table is skipped if:
   * <ul>
   *   <li>The configuration is incomplete (missing table name, partition column, or PK column)</li>
   *   <li>The table definition hasn't changed AND it's not a first partition run</li>
   * </ul>
   *
   * <p><strong>Note:</strong> First partition runs are always processed regardless of XML changes
   * because newly partitioned tables need their constraints recreated.
   *
   * @param isIncomplete
   *     whether the table configuration is missing required information
   * @param firstPartitionRun
   *     whether this is the first run after the table was partitioned
   * @param isUnchanged
   *     whether the table definition hasn't changed in XML files
   * @return {@code true} if processing should be skipped, {@code false} if processing should continue
   */
  private boolean shouldSkipTable(boolean isIncomplete, boolean firstPartitionRun, boolean isUnchanged) {
    return isIncomplete || (!firstPartitionRun && isUnchanged);
  }

  /**
   * Logs an informative message explaining why a table was skipped during processing.
   *
   * <p>This method provides clear feedback in the logs about why specific tables
   * were not processed, helping with debugging and monitoring. Different log levels
   * are used based on the severity of the skip reason:
   * <ul>
   *   <li><strong>WARN:</strong> Used for incomplete configurations (potential data issues)</li>
   *   <li><strong>INFO:</strong> Used for tables that don't need processing (normal operation)</li>
   * </ul>
   *
   * @param isIncomplete
   *     whether the skip was due to incomplete configuration
   * @param tableName
   *     the name of the table that was skipped
   * @param pkCol
   *     the primary key column name (maybe null if incomplete)
   * @param partitionCol
   *     the partition column name (maybe null if incomplete)
   */
  private void logSkipReason(boolean isIncomplete, String tableName, String pkCol, String partitionCol) {
    if (isIncomplete) {
      log4j.warn("Skipping incomplete configuration for table {} (pk = {}, partition = {})",
          tableName, pkCol, partitionCol);
    } else {
      log4j.info("Skipping {}: already processed and no XML changes", tableName);
    }
  }
}
