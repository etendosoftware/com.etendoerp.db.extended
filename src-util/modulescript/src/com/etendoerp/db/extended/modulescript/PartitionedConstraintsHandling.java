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

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openbravo.database.ConnectionProvider;
import org.openbravo.modulescript.ModuleScript;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.nio.file.NoSuchFileException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.etendoerp.db.extended.utils.TableBackupManager;
import com.etendoerp.db.extended.utils.TableDefinitionComparator;
import com.etendoerp.db.extended.utils.DataMigrationService;
import com.etendoerp.db.extended.utils.DatabaseOptimizerUtil;
import com.etendoerp.db.extended.utils.TableAnalyzer;
import com.etendoerp.db.extended.utils.ConstraintManagementUtils;
import com.etendoerp.db.extended.utils.LoggingUtils;
import com.etendoerp.db.extended.utils.XmlParsingUtils;

public class PartitionedConstraintsHandling extends ModuleScript {

  private static final String SRC_DB_DATABASE_MODEL_TABLES = "src-db/database/model/tables";
  private static final String SRC_DB_DATABASE_MODEL_MODIFIED_TABLES = "src-db/database/model/modifiedTables";
  public static final String ALTER_TABLE = "ALTER TABLE IF EXISTS PUBLIC.%s\n";
  private static final Logger log4j = LogManager.getLogger();
  public static final String MODULES_JAR  = "build/etendo/modules";
  public static final String MODULES_BASE = "modules";
  public static final String MODULES_CORE = "modules_core";
  private static final String[] moduleDirs = new String[] {MODULES_BASE, MODULES_CORE, MODULES_JAR};

  // Schema for backup tables to preserve data during structural changes
  private static final String BACKUP_SCHEMA = "etarc_backup";
  
  // Performance optimization constants
  private static final int DEFAULT_BATCH_SIZE = 50000;
  private static final int LARGE_TABLE_THRESHOLD = 1000000;
  
  // Constants for commonly used strings to avoid duplication
  private static final String TABLE_NAME_KEY = "tableName";
  private static final String COLUMN_NAME_KEY = "columnName";
  private static final String PK_COLUMN_NAME_KEY = "pkColumnName";
  private static final String CREATE_SCHEMA_SQL = "CREATE SCHEMA IF NOT EXISTS %s";
  private static final String DROP_TABLE_CASCADE_SQL = "DROP TABLE IF EXISTS public.%s CASCADE";
  private static final String ALTER_TABLE_RENAME_SQL = "ALTER TABLE IF EXISTS public.%s RENAME TO %s";
  private static final String VARCHAR_PREFIX = "VARCHAR(";
  private static final String VARCHAR_255 = "VARCHAR(255)";

  // Utility classes for better code organization
  private final TableBackupManager backupManager;
  private final DataMigrationService migrationService;
  private final DatabaseOptimizerUtil dbOptimizer;
  private final TableAnalyzer tableAnalyzer;
  private final ConstraintManagementUtils constraintUtils;
  
  // Execution controls and metrics
  private boolean dryRun = false;
  private final List<TableMetrics> runMetrics = new ArrayList<>();

  public PartitionedConstraintsHandling() {
    this.backupManager = new TableBackupManager();
    this.migrationService = new DataMigrationService();
    this.dbOptimizer = new DatabaseOptimizerUtil();
    this.tableAnalyzer = new TableAnalyzer();
    this.constraintUtils = new ConstraintManagementUtils();
  }

  // Custom exceptions for better error handling
  public static class TableMigrationException extends Exception {
    public TableMigrationException(String message) {
      super(message);
    }
    
    public TableMigrationException(String message, Throwable cause) {
      super(message, cause);
    }
  }
  
  public static class PartitioningException extends Exception {
    public PartitioningException(String message) {
      super(message);
    }
    
    public PartitioningException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  public static boolean isBlank(String str) {
    return StringUtils.isBlank(str);
  }

  public static boolean isEqualsIgnoreCase(String str1, String str2) {
    return str1 != null && str1.equalsIgnoreCase(str2);
  }

  public void execute() {
    try {
      ConnectionProvider cp = getConnectionProvider();
      // Enable dry-run if system property or environment variable is set
      this.dryRun = isDryRunEnabled();
      long runStart = System.nanoTime();
      if (dryRun) {
        LoggingUtils.logSeparator();
        log4j.info("[DRY-RUN] Enabled. No changes will be applied to the database.");
        LoggingUtils.logSeparator();
      }
      List<Map<String, String>> tableConfigs = loadTableConfigs(cp);

      if (!tableConfigs.isEmpty()) {
        LoggingUtils.logSeparator();
        log4j.info("============== Partitioning process info ==============");
        LoggingUtils.logSeparator();
        
        // STEP 1: Before processing, check for tables that will need structural changes
        // and backup their data if they are currently partitioned
        backupPartitionedTablesData(cp, tableConfigs);
      }
      
      StringBuilder sql = new StringBuilder();
      for (Map<String, String> cfg : tableConfigs) {
        long start = System.nanoTime();
        String tableName = cfg.get(TABLE_NAME_KEY);
        TableMetrics tm = new TableMetrics(tableName);
        try {
          processTableConfig(cp, cfg, sql, tm);
        } finally {
          tm.endNs = System.nanoTime();
          runMetrics.add(tm);
        }
      }
      if (!tableConfigs.isEmpty()) {
        LoggingUtils.logSeparator();
        
        // STEP 2: After processing, clean up old backups (older than 7 days)
        if (dryRun) {
          log4j.info("[DRY-RUN] Skipping cleanup of old backups.");
        } else {
          backupManager.cleanupOldBackups(cp);
        }
      }
      safeExecuteConstraintSqlIfNeeded(cp, sql.toString());

      // Summary
      logRunMetricsSummary(runStart);

    } catch (Exception e) {
      handleError(e);
    }
  }

  /**
   * Backs up data from partitioned tables that will undergo structural changes.
   * This method runs BEFORE update.database to preserve data that would otherwise be lost.
   * 
   * @param cp the connection provider for accessing the database
   * @param tableConfigs list of table configurations to check
   * @throws Exception if backup operations fail
   */
  private void backupPartitionedTablesData(ConnectionProvider cp, List<Map<String, String>> tableConfigs) throws Exception {
    log4j.info("========== CHECKING FOR PARTITIONED TABLES NEEDING BACKUP ==========");
    
    // Ensure backup schema exists
    safeConstraintExecuteUpdate(cp, String.format(CREATE_SCHEMA_SQL, BACKUP_SCHEMA));

    for (Map<String, String> cfg : tableConfigs) {
      if (!isValidTableConfig(cfg)) {
        continue;
      }
      
      String tableName = cfg.get(TABLE_NAME_KEY);
      
      // Check if table is currently partitioned
      boolean isCurrentlyPartitioned = tableAnalyzer.isTablePartitioned(cp, tableName);
      if (!isCurrentlyPartitioned) {
        log4j.info("Table {} is not currently partitioned, skipping backup", tableName);
        continue;
      }
      
      // Check if there will be structural changes
      List<File> xmlFiles = findTableXmlFiles(tableName);
      boolean willHaveStructuralChanges = (new TableDefinitionComparator()).isTableDefinitionChanged(tableName, cp, xmlFiles);
      
      if (willHaveStructuralChanges) {
        if (dryRun) {
          log4j.info("[DRY-RUN] Table {} is partitioned and will have structural changes. Would create backup.", tableName);
        } else {
          log4j.info("Table {} is partitioned and will have structural changes. Creating backup...", tableName);
          backupManager.backupTableData(cp, tableName);
        }
      } else {
        log4j.info("Table {} is partitioned but has no structural changes, no backup needed", tableName);
      }
    }
    
    log4j.info("========== COMPLETED BACKUP CHECK ==========");
  }

  /**
   * Validates if a table configuration has all required fields.
   *
   * @param cfg the table configuration to validate
   * @return true if the configuration is valid, false otherwise
   */
  private boolean isValidTableConfig(Map<String, String> cfg) {
    String tableName = cfg.get(TABLE_NAME_KEY);
    String partitionCol = cfg.get(COLUMN_NAME_KEY);
    String pkCol = cfg.get(PK_COLUMN_NAME_KEY);
    return !isBlank(tableName) && !isBlank(partitionCol) && !isBlank(pkCol);
  }

  /**
   * Loads the table configuration from the `ETARC_TABLE_CONFIG` table.
   * This includes the table name, the partition column, and the primary key column.
   *
   * @param cp the connection provider for accessing the database.
   * @return a list of maps, where each map contains the keys:
   *         "tableName", "columnName", and "pkColumnName".
   * @throws Exception if a database access error occurs.
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
        cfg.put(TABLE_NAME_KEY, rs.getString("TABLENAME"));
        cfg.put(COLUMN_NAME_KEY, rs.getString("COLUMNNAME"));
        cfg.put(PK_COLUMN_NAME_KEY, rs.getString("PK_COLUMNNAME"));
        tableConfigs.add(cfg);
      }
    }
    return tableConfigs;
  }

  /**
   * Processes a single table configuration, determining whether constraints
   * need to be recreated or if a full structural migration is required.
   * If the table is not yet partitioned correctly or its structure has changed,
   * it performs the appropriate action (constraint recreation or full migration).
   *
   * @param cp the connection provider for accessing the database.
   * @param cfg a map containing the table configuration (table name, partition column, primary key).
   * @param sql the SQL builder to which constraint SQL will be appended if necessary.
   * @throws Exception if an error occurs during processing or querying the database.
   */
  private void processTableConfig(ConnectionProvider cp, Map<String, String> cfg, StringBuilder sql, TableMetrics tm) throws Exception {
    String tableName = cfg.get(TABLE_NAME_KEY);
    String partitionCol = cfg.get(COLUMN_NAME_KEY);
    String pkCol = cfg.get(PK_COLUMN_NAME_KEY);

    log4j.info("DATA FROM ETARC_TABLE_CONFIG: tableName: {} - partitionCol: {} - pkCol: {}", tableName, partitionCol, pkCol);

    boolean isIncomplete = isBlank(tableName) || isBlank(partitionCol) || isBlank(pkCol);
    List<File> xmlFiles = isIncomplete ? Collections.emptyList() : findTableXmlFiles(tableName);

    boolean isStructuralChange = !isIncomplete &&
            (new TableDefinitionComparator()).isTableDefinitionChanged(tableName, cp, xmlFiles);

    boolean isPartitioned = tableAnalyzer.isTablePartitioned(cp, tableName);
    List<String> pkCols = tableAnalyzer.getPrimaryKeyColumns(cp, tableName);
    boolean firstPartitionRun = isPartitioned && pkCols.isEmpty();
    
    // Check if this table should be partitioned but isn't (probably due to update.database processing)
    boolean shouldBePartitioned = !isIncomplete; // If we have config, it should be partitioned
    boolean needsPartitionRestoration = shouldBePartitioned && !isPartitioned;

    log4j.info("Table {} partitioned = {} existing PK cols = {} structural changes = {} needs restoration = {}", 
               tableName, isPartitioned, pkCols, isStructuralChange, needsPartitionRestoration);

      if (shouldSkipTable(isIncomplete, firstPartitionRun, !isStructuralChange && !needsPartitionRestoration)) {
        LoggingUtils.logSkipReason(isIncomplete, tableName, pkCol, partitionCol);
        return;
      }    // Check table size for performance optimizations
    long tableSize = 0;
    try {
      tableSize = tableAnalyzer.getApproximateTableRowCount(cp, tableName);
      log4j.info("Table {} estimated size: {} rows", tableName, tableSize);
    } catch (Exception e) {
      log4j.warn("Could not determine table size for {}: {}", tableName, e.getMessage());
    }

    // Apply performance optimizations for large tables
    boolean isLargeTable = tableSize > LARGE_TABLE_THRESHOLD;
    tm.estimatedRows = tableSize;
    tm.largeTable = isLargeTable;
    if (isLargeTable) {
      log4j.info("Large table detected ({}), applying performance optimizations", tableName);
      if (!dryRun) {
        dbOptimizer.optimizeDatabaseForLargeOperations(cp);
        dbOptimizer.logPerformanceMetrics(cp, "Pre-partition " + tableName);
      } else {
        log4j.info("[DRY-RUN] Skipping DB optimizations.");
      }
    }

    try {
      // If table needs partition restoration (was unpartitioned by update.database), re-partition it
      if (needsPartitionRestoration) {
        tm.action = "restore-partitioning";
        log4j.info("Table {} should be partitioned but isn't. Restoring partitioning with current data...", tableName);
        if (dryRun) {
          log4j.info("[DRY-RUN] Would restore partitioning for table {} (partitionCol={}, pkCol={})", tableName, partitionCol, pkCol);
        } else {
          long migrated = restorePartitioningWithData(cp, tableName, partitionCol, pkCol);
          tm.migratedRows = migrated;
        }
      }
      // If the table is partitioned AND has structural changes, perform full migration
      else if (isPartitioned && isStructuralChange) {
        tm.action = "migrate-structural";
        log4j.info("Structural changes detected in partitioned table {}. Performing full migration...", tableName);
        if (dryRun) {
          log4j.info("[DRY-RUN] Would perform full migration for table {} (partitionCol={}, pkCol={})", tableName, partitionCol, pkCol);
        } else {
          long migrated = performPartitionedTableMigration(cp, tableName, partitionCol, pkCol);
          tm.migratedRows = migrated;
        }
      } else {
        // Otherwise, just recreate constraints (existing behavior)
        tm.action = "recreate-constraints";
        log4j.info("Recreating constraints for {} (firstRun = {}, structuralChanges = {})", 
                   tableName, firstPartitionRun, isStructuralChange);
        if (dryRun) {
          String ddl = constraintUtils.buildConstraintSql(tableName, cp, pkCol, partitionCol, getSourcePath());
          log4j.info("[DRY-RUN] Would execute constraint SQL for {} ({} chars)", tableName, ddl.length());
        } else {
          sql.append(constraintUtils.buildConstraintSql(tableName, cp, pkCol, partitionCol, getSourcePath()));
        }
      }
      // Post-processing optimizations for large tables
      if (isLargeTable) {
        if (!dryRun) {
          dbOptimizer.createOptimizedIndexes(cp, tableName, partitionCol);
          dbOptimizer.analyzePartitionedTable(cp, tableName);
          dbOptimizer.logPerformanceMetrics(cp, "Post-partition " + tableName);
        } else {
          log4j.info("[DRY-RUN] Skipping index creation and ANALYZE.");
        }
      }
      
    } finally {
      // Restore database settings if they were modified
      if (isLargeTable && !dryRun) {
        dbOptimizer.restoreDatabaseSettings(cp);
      }
    }
  }

  /**
   * Determines whether a table should be skipped based on its configuration
   * and partitioning state.
   *
   * @param isIncomplete true if the table configuration is incomplete.
   * @param firstPartitionRun true if this is the first partitioning run for the table.
   * @param isUnchangedAndPartitioned true if the table definition has not changed and is already properly partitioned.
   * @return true if the table should be skipped, false otherwise.
   */
  private boolean shouldSkipTable(boolean isIncomplete, boolean firstPartitionRun, boolean isUnchangedAndPartitioned) {
    return isIncomplete || (!firstPartitionRun && isUnchangedAndPartitioned);
  }

  /**
   * Performs a full migration of a partitioned table when structural changes are detected.
   * This method implements a process similar to migrate.py:
   * 1. Rename the current partitioned table to a temporary name
   * 2. Create a new partitioned table with the updated structure
   * 3. Migrate all data from the temporary table to the new table
   * 4. Drop the temporary table
   * 5. Recreate constraints and foreign keys
   *
   * @param cp the connection provider for accessing the database.
   * @param tableName the name of the partitioned table to migrate.
   * @param partitionCol the partition column name.
   * @param pkCol the primary key column name.
   * @throws Exception if any error occurs during the migration process.
   */
  private long performPartitionedTableMigration(ConnectionProvider cp, String tableName, 
                                               String partitionCol, String pkCol) throws Exception {
    log4j.info("========== STARTING PARTITIONED TABLE MIGRATION FOR {} ==========", tableName);
    
    String tempTableName = tableName + "_etarc_migration_temp";
    String partitionsSchema = "partitions";
    String dataSourceTable = tempTableName; // Initialize with temp table name
    long migratedRows = 0L;
    
    try {
      // Step 1: Drop dependent views (they'll be recreated by Etendo's process)
      log4j.info("Step 1: Dropping dependent views and foreign keys for table {}", tableName);
      if (dryRun) {
        log4j.info("[DRY-RUN] Would drop dependent views and constraints for {}", tableName);
      } else {
        constraintUtils.dropDependentViewsAndConstraints(cp, tableName);
      }
      
      // Step 2: Rename current partitioned table to temporary name
      log4j.info("Step 2: Renaming {} to {}", tableName, tempTableName);
      safeConstraintExecuteUpdate(cp, String.format(ALTER_TABLE_RENAME_SQL, 
                                     tableName, tempTableName));
      
      // Step 3: Create new partitioned table with updated structure based on XML
      log4j.info("Step 3: Creating new partitioned table {} with updated structure", tableName);
      if (dryRun) {
        log4j.info("[DRY-RUN] Would create new partitioned table {} with updated structure", tableName);
      } else {
        createUpdatedPartitionedTable(cp, tableName, partitionCol);
      }
      
      // Step 4: Ensure partitions schema exists
      log4j.info("Step 4: Ensuring partitions schema '{}' exists", partitionsSchema);
      safeExecuteUpdate(cp, String.format(CREATE_SCHEMA_SQL, partitionsSchema));
      
      // Step 5: Create partitions for the new table
      log4j.info("Step 5: Creating partitions for new table {}", tableName);
      if (dryRun) {
        log4j.info("[DRY-RUN] Would create partitions for {} based on {}", tableName, tempTableName);
      } else {
        createPartitionsForTable(cp, tableName, tempTableName, partitionCol, partitionsSchema);
      }
      
      // Step 6: Migrate data from temporary table to new partitioned table
      log4j.info("Step 6: Migrating data from {} to {}", tempTableName, tableName);
      
      // Check if temporary table has data, if not try to use backup
      long tempTableRows = backupManager.getTableRowCount(cp, tempTableName);
      dataSourceTable = tempTableName; // Reset to temp table name
      
      if (tempTableRows == 0) {
        log4j.warn("Temporary table {} is empty, checking for backup data...", tempTableName);
        String backupTableName = backupManager.getLatestBackupTable(cp, tableName);
        if (backupTableName != null) {
          String fullBackupTableName = BACKUP_SCHEMA + "." + backupTableName;
          long backupRows = backupManager.getTableRowCount(cp, fullBackupTableName);
          if (backupRows > 0) {
            log4j.info("Found backup table {} with {} rows. Using backup for data migration.", 
                      backupTableName, backupRows);
            dataSourceTable = fullBackupTableName;
            tempTableRows = backupRows;
          }
        }
        
        if (tempTableRows == 0) {
          log4j.warn("No backup data found. Table {} will be empty after migration.", tableName);
        }
      }
      
      if (dryRun) {
        log4j.info("[DRY-RUN] Would migrate data from {} to {}", dataSourceTable, tableName);
      } else {
        migratedRows = migrationService.migrateDataToNewTable(cp, dataSourceTable, tableName, partitionCol);
        LoggingUtils.logMigrationSuccessMessage(tableName, migratedRows, dataSourceTable);
      }

      // Step 7: Verify the new table exists and is properly partitioned before proceeding
      log4j.info("Step 7: Verifying new partitioned table {} was created successfully", tableName);
      if (!dryRun && !isTablePartitioned(cp, tableName)) {
        throw new TableMigrationException("New table " + tableName + " is not properly partitioned after migration");
      }
      
      // Step 8: Try to recreate constraints (with error handling)
      log4j.info("Step 8: Recreating constraints for {}", tableName);
      try {
        String constraintSql = constraintUtils.buildConstraintSql(tableName, cp, pkCol, partitionCol, getSourcePath());
        if (!isBlank(constraintSql)) {
          safeConstraintExecuteUpdate(cp, constraintSql);
          if (!dryRun) {
            LoggingUtils.logSuccessRecreatedConstraints(tableName);
          }
        }
      } catch (Exception constraintError) {
        log4j.error("WARNING: Failed to recreate constraints for {}: {}", tableName, constraintError.getMessage());
        log4j.error("The table migration was successful, but constraint recreation failed.");
        log4j.error("Constraints can be recreated manually later.");
        
        // Don't re-throw for constraint errors - the migration itself was successful
        // Constraints already existing is not a critical error that should stop the process
        if (constraintError.getMessage().contains("already exists")) {
          log4j.info("Constraint already exists - this is expected and not an error. Continuing...");
        } else {
          log4j.error("Keeping temporary table for safety due to unexpected constraint error.");
          // For other constraint errors, we might want to keep the temp table
          // but still not fail the entire migration
        }
      }
      
      // Step 9: Only drop temporary table after everything succeeds
      log4j.info("Step 9: Dropping temporary table {} (migration completed successfully)", tempTableName);
      safeExecuteUpdate(cp, String.format(DROP_TABLE_CASCADE_SQL, tempTableName));
      
      // Step 10: Clean up backup if we used it
      if (!dataSourceTable.equals(tempTableName)) {
        // We used backup data, clean it up
        String backupTableName = dataSourceTable.substring(dataSourceTable.lastIndexOf(".") + 1);
        log4j.info("Step 10: Cleaning up backup table {} (migration completed successfully)", backupTableName);
        try {
          if (dryRun) {
            log4j.info("[DRY-RUN] Would cleanup backup table {}", backupTableName);
          } else {
            backupManager.cleanupBackup(cp, tableName, backupTableName);
          }
        } catch (Exception backupCleanupError) {
          log4j.warn("Failed to cleanup backup table {}: {}", backupTableName, backupCleanupError.getMessage());
          // Don't fail the migration for backup cleanup errors
        }
      }
      
      log4j.info("========== COMPLETED PARTITIONED TABLE MIGRATION FOR {} ==========", tableName);
      return migratedRows;
      
    } catch (Exception e) {
      log4j.error("ERROR during partitioned table migration for {}: {}", tableName, e.getMessage(), e);
      
      // Attempt to restore original table if migration failed and temp table still exists
      try {
        // First check if the temporary table still exists
        boolean tempTableExists = false;
        try (PreparedStatement checkPs = cp.getPreparedStatement(
            "SELECT 1 FROM information_schema.tables WHERE table_name = ? AND table_schema = 'public'")) {
          checkPs.setString(1, tempTableName);
          try (ResultSet rs = checkPs.executeQuery()) {
            tempTableExists = rs.next();
          }
        }
        
        if (tempTableExists) {
          LoggingUtils.logRestoreOriginalTable(tableName, tempTableName);
          safeExecuteUpdate(cp, String.format(DROP_TABLE_CASCADE_SQL, tableName));
          safeExecuteUpdate(cp, String.format(ALTER_TABLE_RENAME_SQL, 
                                         tempTableName, tableName));
          LoggingUtils.logSuccessRestoredTable(tableName);
        } else {
          log4j.error("CRITICAL: Cannot restore original table {} - temporary table {} no longer exists", 
                     tableName, tempTableName);
          log4j.error("Manual intervention required to restore the table from backup");
        }
      } catch (Exception restoreError) {
        LoggingUtils.logCriticalRestorationError(tableName, restoreError);
      }
      return migratedRows;
    }
  }

  /**
   * Restores partitioning to a table that should be partitioned but isn't.
   * This handles cases where update.database has converted a partitioned table
   * back to a regular table, and we need to restore the partitioning while
   * preserving all existing data. If the table is empty, but we have a backup,
   * it will restore from the backup.
   *
   * @param cp the connection provider for accessing the database.
   * @param tableName the name of the table to restore partitioning for.
   * @param partitionCol the partition column name.
   * @param pkCol the primary key column name.
   * @throws Exception if any error occurs during the restoration process.
   */
  private long restorePartitioningWithData(ConnectionProvider cp, String tableName, 
                                         String partitionCol, String pkCol) throws Exception {
    log4j.info("========== RESTORING PARTITIONING FOR {} ==========", tableName);
    
    String tempTableName = tableName + "_etarc_restore_temp";
    String partitionsSchema = "partitions";
    long migratedRows = 0L;
    
    try {
      // Step 1: Check if table has data
      long rowCount = backupManager.getTableRowCount(cp, tableName);
      log4j.info("Step 1: Table {} contains {} rows", tableName, rowCount);
      
      // Step 2: If table is empty, check for backup data
      String dataSourceTable = tableName;
      String backupTableName = null; // Declare backup table name variable
      boolean usingBackup = false;
      
      if (rowCount == 0) {
        backupTableName = backupManager.getLatestBackupTable(cp, tableName);
        if (backupTableName != null) {
          long backupRowCount = backupManager.getTableRowCount(cp, BACKUP_SCHEMA + "." + backupTableName);
          if (backupRowCount > 0) {
            log4j.info("Table is empty but found backup {} with {} rows. Will restore from backup.", 
                      backupTableName, backupRowCount);
            dataSourceTable = BACKUP_SCHEMA + "." + backupTableName;
            rowCount = backupRowCount;
            usingBackup = true;
          }
        }
        
        if (!usingBackup) {
          log4j.warn("Table {} is empty and no backup found. Creating empty partitioned table.", tableName);
        }
      }
      
      if (rowCount == 0 && !usingBackup) {
        // No data, we can simply convert to partitioned table directly
        log4j.info("Table is empty, converting directly to partitioned table");
        if (dryRun) {
          log4j.info("[DRY-RUN] Would convert empty table {} to partitioned.", tableName);
        } else {
          migrationService.convertEmptyTableToPartitioned(tableName);
        }
      } else {
        // Has data (either original or from backup), need to migrate safely
        log4j.info("Table has data{}, performing safe migration to restore partitioning", 
                  usingBackup ? " (from backup)" : "");
        
        // Step 3: Rename current table to temporary name
        log4j.info("Step 3: Renaming {} to {}", tableName, tempTableName);
        safeExecuteUpdate(cp, String.format(ALTER_TABLE_RENAME_SQL, 
                                       tableName, tempTableName));
        
        // Step 4: Create new partitioned table with same structure
        log4j.info("Step 4: Creating new partitioned table {} with same structure", tableName);
        if (dryRun) {
          log4j.info("[DRY-RUN] Would create new partitioned table {} from template {}", tableName, usingBackup ? backupTableName : tempTableName);
        } else {
          if (usingBackup) {
            migrationService.createPartitionedTableFromTemplate(cp, tableName, backupTableName, partitionCol);
          } else {
            migrationService.createPartitionedTableFromTemplate(cp, tableName, tempTableName, partitionCol);
          }
        }
        
        // Step 5: Ensure partitions schema exists
        log4j.info("Step 5: Ensuring partitions schema '{}' exists", partitionsSchema);
        safeExecuteUpdate(cp, String.format(CREATE_SCHEMA_SQL, partitionsSchema));
        
        // Step 6: Create partitions based on data source
        log4j.info("Step 6: Creating partitions for table {}", tableName);
        if (dryRun) {
          log4j.info("[DRY-RUN] Would create partitions for {} based on {}", tableName, dataSourceTable);
        } else {
          createPartitionsForTable(cp, tableName, dataSourceTable, partitionCol, partitionsSchema);
        }
        
        // Step 7: Migrate data to new partitioned table
        log4j.info("Step 7: Migrating data from {} to {}", dataSourceTable, tableName);
        if (dryRun) {
          log4j.info("[DRY-RUN] Would migrate data from {} to {}", dataSourceTable, tableName);
        } else {
          migratedRows = migrationService.migrateDataToNewTable(cp, dataSourceTable, tableName, partitionCol);
          LoggingUtils.logMigrationSuccessMessage(tableName, migratedRows, dataSourceTable);
        }

        // Step 8: Drop temporary table
        log4j.info("Step 8: Dropping temporary table {} (restoration completed successfully)", tempTableName);
        safeExecuteUpdate(cp, String.format(DROP_TABLE_CASCADE_SQL, tempTableName));
        
        // Step 9: Clean up backup if used
        if (usingBackup) {
          log4j.info("Step 9: Cleaning up backup table {}", dataSourceTable);
          if (dryRun) {
            log4j.info("[DRY-RUN] Would cleanup backup {}", dataSourceTable);
          } else {
            backupManager.cleanupBackup(cp, tableName, backupTableName);
          }
        }
      }
      
      // Step 10: Recreate constraints for the now-partitioned table
      log4j.info("Step 10: Recreating constraints for restored partitioned table {}", tableName);
      String constraintSql = constraintUtils.buildConstraintSql(tableName, cp, pkCol, partitionCol, getSourcePath());
      if (!isBlank(constraintSql)) {
        safeConstraintExecuteUpdate(cp, constraintSql);
        if (!dryRun) {
          LoggingUtils.logSuccessRecreatedConstraints(tableName);
        }
      }
      
      log4j.info("========== COMPLETED PARTITIONING RESTORATION FOR {} ==========", tableName);
      return migratedRows;
      
    } catch (Exception e) {
      log4j.error("ERROR during partitioning restoration for {}: {}", tableName, e.getMessage(), e);
      
      // Attempt to restore original table if restoration failed
      try {
        boolean tempTableExists = backupManager.tableExists(cp, tempTableName);
        
        if (tempTableExists) {
          LoggingUtils.logRestoreOriginalTable(tableName, tempTableName);
          safeExecuteUpdate(cp, String.format(DROP_TABLE_CASCADE_SQL, tableName));
          safeExecuteUpdate(cp, String.format(ALTER_TABLE_RENAME_SQL, 
                                         tempTableName, tableName));
          LoggingUtils.logSuccessRestoredTable(tableName);
        }
      } catch (Exception restoreError) {
        LoggingUtils.logCriticalRestorationError(tableName, restoreError);
      }
      
      throw new PartitioningException("Partitioning restoration failed for " + tableName, e);
    }
  }

  /**
   * Executes a SQL update statement and returns the number of affected rows.
   *
   * @param cp the connection provider
   * @param sql the SQL statement to execute
   * @return the number of affected rows
   * @throws Exception if execution fails
   */
  private int executeUpdateWithRowCount(ConnectionProvider cp, String sql) throws Exception {
    try (PreparedStatement ps = cp.getPreparedStatement(sql)) {
      return ps.executeUpdate();
    }
  }

  /**
   * Executes a SQL update statement.
   *
   * @param cp the connection provider
   * @param sql the SQL statement to execute
   * @throws Exception if execution fails
   */
  private void executeUpdate(ConnectionProvider cp, String sql) throws Exception {
    try (PreparedStatement ps = cp.getPreparedStatement(sql)) {
      ps.executeUpdate();
    }
  }

  // Safe wrappers honoring dry-run
  private void safeExecuteUpdate(ConnectionProvider cp, String sql) throws Exception {
    if (dryRun) {
      log4j.info("[DRY-RUN] SQL: {}", sql);
      return;
    }
    executeUpdate(cp, sql);
  }

  private void safeConstraintExecuteUpdate(ConnectionProvider cp, String sql) throws Exception {
    if (StringUtils.isBlank(sql)) return;
    if (dryRun) {
      log4j.info("[DRY-RUN] Constraints SQL ({} chars) would be executed", sql.length());
      return;
    }
    constraintUtils.executeUpdate(cp, sql);
  }

  private void safeExecuteConstraintSqlIfNeeded(ConnectionProvider cp, String sql) throws Exception {
    if (StringUtils.isBlank(sql)) return;
    if (dryRun) {
      log4j.info("[DRY-RUN] Accumulated constraint SQL ({} chars) would be executed.", sql.length());
      return;
    }
    constraintUtils.executeConstraintSqlIfNeeded(cp, sql);
  }

  /**
   * Logs the creation of a partitioned table.
   *
   * @param createTableSql the SQL statement being executed
   */
  private void logCreatingPartitionedTable(String createTableSql) {
    log4j.info("Creating partitioned table with SQL: {}", createTableSql);
  }

  /**
   * Gets a Document from an XML file with XXE protection.
   *
   * @param xmlFile the XML file to parse
   * @return the parsed Document
   * @throws Exception if parsing fails
   */
  private static Document getDocument(File xmlFile) throws Exception {
    return XmlParsingUtils.getDocument(xmlFile);
  }

  private boolean isDryRunEnabled() {
    String sys = System.getProperty("etarc.dryRun", System.getProperty("dryRun", "false"));
    String env = System.getenv("ETARC_DRY_RUN");
    return "true".equalsIgnoreCase(sys) || "1".equals(env) || "true".equalsIgnoreCase(env);
  }

  // Basic per-table metrics holder
  private static class TableMetrics {
    final String tableName;
    String action = "unknown";
    long startNs = System.nanoTime();
    long endNs = startNs;
    long estimatedRows = -1;
    long migratedRows = -1;
    boolean largeTable = false;

    TableMetrics(String tableName) { this.tableName = tableName; }
  }

  /**
   * Handles general errors in the module script.
   *
   * @param e the exception that occurred
   */
  private void handleError(Exception e) {
    log4j.error("Error in PartitionedConstraintsHandling: {}", e.getMessage(), e);
    throw new RuntimeException("Module script execution failed", e);
  }

  // Summary logging
  private void logRunMetricsSummary(long runStartNs) {
    long totalMs = (System.nanoTime() - runStartNs) / 1_000_000;
    LoggingUtils.logSeparator();
    log4j.info("Partitioning run summary{}: {} ms", dryRun ? " (DRY-RUN)" : "", totalMs);
    for (TableMetrics tm : runMetrics) {
      long durMs = Math.max(0, (tm.endNs - tm.startNs) / 1_000_000);
      log4j.info("- {} | action={} | durationMs={} | estimatedRows={}{}{}",
          tm.tableName,
          tm.action,
          durMs,
          tm.estimatedRows,
          tm.largeTable ? " | large" : "",
          tm.migratedRows >= 0 ? " | migratedRows=" + tm.migratedRows : "");
    }
    LoggingUtils.logSeparator();
  }

  /**
   * Checks whether the given table is currently partitioned in the PostgreSQL database.
   *
   * @param cp the connection provider for accessing the database.
   * @param tableName the name of the table to check.
   * @return true if the table is partitioned, false otherwise.
   * @throws Exception if a database access error occurs.
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
   * Searches the provided list of XML table definition files for the "primaryKey"
   * attribute on the single <table> element in each file.
   * <p>
   * For each XML file:
   * <ul>
   *   <li>If the file does not exist, logs an error and returns null.</li>
   *   <li>If exactly one <table> element is found, and it has a "primaryKey" attribute,
   *       returns that attribute’s value.</li>
   *   <li>If no <table> elements or multiple <table> elements are found, or if the
   *       attribute is missing, logs the appropriate warning/error and returns null.</li>
   * </ul>
   *
   * @param xmlFiles the list of XML files to inspect
   * @return the name of the primary key if found, or null if missing or on error
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
        NodeList tableList = doc.getElementsByTagName("table");

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
          log4j.error("Error: Found {} <table> tags in: {}", tableList.getLength(),
              xml.getAbsolutePath());
          return null;
        }
      }
    } catch (Exception e) {
      log4j.error("Error processing XML: {}", e.getMessage(), e);
    }
    return null;
  }

  /**
   * Gathers all directories that potentially contain table XML files.
   * <p>
   * Scans each module directory under the project root (as defined by ModulesUtil),
   * adding:
   * <ul>
   *   <li>The module’s own “src-db/database/model/tables” directory, if present.</li>
   *   <li>That same tables directory under each immediate subdirectory of the module.</li>
   * </ul>
   * Finally, adds the project‐root “src-db/database/model/tables” directory.
   * Only existing directories are returned.
   *
   * @return a List of File objects representing each valid tables directory
   */
  private List<File> collectTableDirs() throws NoSuchFileException {
    List<File> dirs = new ArrayList<>();
    File root = new File(getSourcePath());
    for (String mod : moduleDirs) {
      File modBase = new File(root, mod);
      if (!modBase.isDirectory()) continue;
      dirs.add(new File(modBase, SRC_DB_DATABASE_MODEL_TABLES));
      for (File sd : Objects.requireNonNull(modBase.listFiles(File::isDirectory))) {
        dirs.add(new File(sd, SRC_DB_DATABASE_MODEL_TABLES));
      }
      dirs.add(new File(modBase, SRC_DB_DATABASE_MODEL_MODIFIED_TABLES));
      for (File sd : Objects.requireNonNull(modBase.listFiles(File::isDirectory))) {
        dirs.add(new File(sd, SRC_DB_DATABASE_MODEL_MODIFIED_TABLES));
      }
    }
    dirs.add(new File(root, SRC_DB_DATABASE_MODEL_TABLES));
    return dirs.stream().filter(File::isDirectory).collect(Collectors.toList());
  }

  /**
   * Finds the .xml file(s) matching the given table name (case-insensitive)
   * under each module’s “tables” directory and under the project's root.
   * <p>
   * Constructs a target filename of the form {@code tableName + ".xml"}, then
   * filters all XMLs in the discovered directories to only those whose name
   * equals the target.
   *
   * @param tableName the base name of the table (without the .xml extension)
   * @return a List of matching XML files (maybe empty if none found)
   */
  public List<File> findTableXmlFiles(String tableName) throws NoSuchFileException {
    String target = tableName.toLowerCase() + ".xml";
    return collectTableDirs().stream()
        .flatMap(dir -> {
          File[] files = dir.listFiles(f -> f.isFile() && f.getName().endsWith(".xml"));
          return files == null ? Stream.empty() : Arrays.stream(files);
        })
        .filter(f -> f.isFile() && f.getName().equalsIgnoreCase(target))
        .collect(Collectors.toList());
  }

  /**
   * Creates a new partitioned table with updated structure based on the temporary table
   * but without the constraints that will be recreated later.
   *
   * @param cp the connection provider
   * @param newTableName the name of the new partitioned table
   * @param partitionCol the partition column name
   * @throws Exception if table creation fails
   */
  private void createUpdatedPartitionedTable(ConnectionProvider cp, String newTableName,
                                             String partitionCol) throws Exception {
    // Create the new partitioned table based on XML definitions, not the old table structure
    List<File> xmlFiles = findTableXmlFiles(newTableName);
    if (xmlFiles.isEmpty()) {
      throw new Exception("No XML definition files found for table " + newTableName);
    }
    
    String createTableSql = generateCreateTableFromXml(newTableName, xmlFiles, partitionCol);

    logCreatingPartitionedTable(createTableSql);
  safeExecuteUpdate(cp, createTableSql);
    log4j.info("Created new partitioned table: public.{}", newTableName);
  }

  /**
   * Generates a CREATE TABLE statement based on XML definitions.
   * 
   * @param tableName the name of the table to create
   * @param xmlFiles list of XML files defining the table structure
   * @param partitionCol the column to partition by
   * @return the complete CREATE TABLE SQL statement
   * @throws Exception if parsing XML fails
   */
  private String generateCreateTableFromXml(String tableName, List<File> xmlFiles, String partitionCol) throws Exception {
    // Parse all XML files to get the complete column definition
    Map<String, ColumnDefinition> xmlColumns = new LinkedHashMap<>();
    
    for (File xmlFile : xmlFiles) {
      Map<String, ColumnDefinition> partial = parseXmlDefinition(xmlFile);
      for (Map.Entry<String, ColumnDefinition> entry : partial.entrySet()) {
        xmlColumns.putIfAbsent(entry.getKey(), entry.getValue());
      }
    }
    
    if (xmlColumns.isEmpty()) {
      throw new TableMigrationException("No column definitions found in XML files for table " + tableName);
    }
    
    StringBuilder sql = new StringBuilder();
    sql.append("CREATE TABLE public.").append(tableName).append(" (\n");
    
    boolean first = true;
    for (ColumnDefinition column : xmlColumns.values()) {
      if (!first) {
        sql.append(",\n");
      }
      first = false;
      
      sql.append("  ").append(column.name()).append(" ");
      sql.append(mapXmlTypeToPostgreSQL(column.dataType(), column.length()));
      
      if (Boolean.FALSE.equals(column.isNullable())) {
        sql.append(" NOT NULL");
      }
    }
    
    sql.append("\n) PARTITION BY RANGE (").append(partitionCol).append(")");
    
    return sql.toString();
  }

  /**
   * Helper method to access parseXmlDefinition from TableDefinitionComparator
   */
  private Map<String, ColumnDefinition> parseXmlDefinition(File xmlFile) throws Exception {
    // We need to use reflection or create a public method in TableDefinitionComparator
    // For now, let's implement our own XML parsing here
    Map<String, ColumnDefinition> columns = new LinkedHashMap<>();
    
    DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
    dbFactory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
    dbFactory.setFeature("http://xml.org/sax/features/external-general-entities", false);
    dbFactory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
    dbFactory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
    dbFactory.setXIncludeAware(false);
    dbFactory.setExpandEntityReferences(false);
    
    DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
    Document doc = dBuilder.parse(xmlFile);
    doc.getDocumentElement().normalize();
    
    NodeList columnList = doc.getElementsByTagName("column");
    for (int i = 0; i < columnList.getLength(); i++) {
      Element colElem = (Element) columnList.item(i);
      String name = colElem.getAttribute("name").toLowerCase();
      String dataType = colElem.getAttribute("type").toLowerCase();
      
      // In Etendo XML, "required" attribute determines if column is nullable
      // required="true" means NOT NULL, required="false" or missing means NULL allowed
      boolean isRequired = "true".equalsIgnoreCase(colElem.getAttribute("required"));
      boolean isNullable = !isRequired; // Invert: required=true means nullable=false
      
      Integer length = null;
      if (colElem.hasAttribute("length")) {
        length = parseIntLenient(colElem.getAttribute("length"));
      } else if (colElem.hasAttribute("size")) {
        length = parseIntLenient(colElem.getAttribute("size"));
      }

      boolean isPrimaryKey = "true".equalsIgnoreCase(colElem.getAttribute("primarykey"));
      
      columns.put(name, new ColumnDefinition(name, dataType, length, isNullable, isPrimaryKey));
    }
    
    return columns;
  }

  /**
   * Helper method for lenient integer parsing
   */
  private int parseIntLenient(String raw) {
    if (raw == null || raw.trim().isEmpty()) {
      return 0;
    }
    String cleaned = raw.trim().replace(",", ".");
    try {
      return Integer.parseInt(cleaned);
    } catch (NumberFormatException e) {
      try {
        return (int) Math.round(Double.parseDouble(cleaned));
      } catch (NumberFormatException e2) {
        return 0;
      }
    }
  }

  /**
   * Maps XML data types to PostgreSQL data types
   */
  private String mapXmlTypeToPostgreSQL(String xmlType, Integer length) {
    switch (xmlType.toLowerCase()) {
      case "id", "varchar", "nvarchar", "string":
        if (length != null && length > 0) {
          return VARCHAR_PREFIX + length + ")";
        }
        return VARCHAR_255; // default length
      case "text":
        return "TEXT";
      case "bigint":
        return "BIGINT";
      case "integer":
        return "INTEGER";
      case "numeric":
        if (length != null && length > 0) {
          return "NUMERIC(" + length + ")";
        }
        return "NUMERIC";
      case "decimal":
        if (length != null && length > 0) {
          return "DECIMAL(" + length + ")";
        }
        return "DECIMAL";
      case "timestamp":
        return "TIMESTAMP WITHOUT TIME ZONE";
      case "date":
        return "DATE";
      case "boolean":
        return "BOOLEAN";
      case "char":
        if (length != null && length > 0) {
          return "CHAR(" + length + ")";
        }
        return "CHAR(1)";
      default:
        // Default fallback
        if (length != null && length > 0) {
          return VARCHAR_PREFIX + length + ")";
        }
        return VARCHAR_255;
    }
  }

  /**
   * Inner class to represent column definitions (similar to TableDefinitionComparator's)
   */
    private record ColumnDefinition(String name, String dataType, Integer length, Boolean isNullable,
                                    Boolean isPrimaryKey) {
  }

  /**
   * Creates partitions for the new partitioned table based on the data 
   * in the temporary table.
   *
   * @param cp the connection provider
   * @param newTableName the new partitioned table name
   * @param dataSourceTable the table containing the data (can include schema)
   * @param partitionCol the partition column name
   * @param partitionsSchema the schema where partitions will be created
   * @throws Exception if partition creation fails
   */
  private void createPartitionsForTable(ConnectionProvider cp, String newTableName, String dataSourceTable,
                                       String partitionCol, String partitionsSchema) throws Exception {
    
    log4j.info("Creating partitions using simplified approach for table {}", newTableName);
    
    // First, drop any existing partitions for this table to avoid conflicts
    String dropExistingPartitionsSql = String.format(
        "SELECT 'DROP TABLE IF EXISTS ' || schemaname || '.' || tablename || ' CASCADE;' " +
        "FROM pg_tables WHERE tablename LIKE '%s_y%%' AND schemaname = '%s'",
        newTableName.toLowerCase(), partitionsSchema
    );
    
    log4j.info("Dropping existing partitions to avoid conflicts:");
    try (PreparedStatement dropPs = cp.getPreparedStatement(dropExistingPartitionsSql);
         ResultSet dropRs = dropPs.executeQuery()) {
      while (dropRs.next()) {
        String dropStmt = dropRs.getString(1);
        log4j.info("Executing: {}", dropStmt);
  safeExecuteUpdate(cp, dropStmt);
      }
    }
    
    // Handle data source table that might include schema
    String dataSourceReference = dataSourceTable;
    if (!dataSourceTable.contains(".")) {
      dataSourceReference = "public." + dataSourceTable;
    }
    
    // Get year range from the data source table
    String yearRangeSql = String.format(
        "SELECT EXTRACT(YEAR FROM MIN(%s))::int, EXTRACT(YEAR FROM MAX(%s))::int " +
        "FROM %s WHERE %s IS NOT NULL",
        partitionCol, partitionCol, dataSourceReference, partitionCol
    );
    
    int startYear = 2011;
    int endYear = 2025;
    
    try (PreparedStatement ps = cp.getPreparedStatement(yearRangeSql);
         ResultSet rs = ps.executeQuery()) {
      if (rs.next()) {
        Integer minYear = (Integer) rs.getObject(1);
        Integer maxYear = (Integer) rs.getObject(2);
        if (minYear != null && maxYear != null) {
          startYear = minYear;
          endYear = Math.max(maxYear, java.time.Year.now().getValue());
        }
      }
    }
    
    log4j.info("Will create partitions for years {} to {} plus one future year", startYear, endYear);

    // Create partitions for each year using simple string literals
    for (int year = startYear; year <= endYear + 1; year++) {
      String partitionName = String.format("%s_y%d", newTableName.toLowerCase(), year);
      
      // Use simple date literals that PostgreSQL will understand
      String fromDate = String.format("%d-01-01 00:00:00", year);
      String toDate = String.format("%d-01-01 00:00:00", year + 1);
      
      String createPartitionSql = String.format(
          "CREATE TABLE IF NOT EXISTS %s.%s PARTITION OF public.%s " +
          "FOR VALUES FROM ('%s') TO ('%s')",
          partitionsSchema, partitionName, newTableName, fromDate, toDate
      );
      
      try {
        log4j.info("Creating partition: {}", createPartitionSql);
        safeExecuteUpdate(cp, createPartitionSql);
        log4j.info("Successfully created partition {}.{}", partitionsSchema, partitionName);
      } catch (Exception e) {
        if (e.getMessage().contains("already exists")) {
          log4j.warn("Partition {} already exists, skipping", partitionName);
        } else {
          log4j.error("Failed to create partition {}: {}", partitionName, e.getMessage());
          throw e;
        }
      }
    }
    
    // Test if the first few records would fit
    String testSql = String.format(
        "SELECT %s, EXTRACT(YEAR FROM %s) as year FROM %s WHERE %s IS NOT NULL ORDER BY %s LIMIT 5",
        partitionCol, partitionCol, dataSourceReference, partitionCol, partitionCol
    );
    
    log4j.info("Testing sample data against partition bounds:");
    try (PreparedStatement testPs = cp.getPreparedStatement(testSql);
         ResultSet testRs = testPs.executeQuery()) {
      while (testRs.next()) {
        Object dateValue = testRs.getObject(1);
        int year = testRs.getInt(2);
        log4j.info("  Sample: {} (year {}) should go to partition {}_y{}", dateValue, year, newTableName.toLowerCase(), year);
      }
    }
  }
}
