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
import org.openbravo.base.exception.*;
import org.openbravo.database.ConnectionProvider;
import org.openbravo.modulescript.ModuleScript;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.*;
import java.io.*;
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
import org.xml.sax.*;

public class PartitionedConstraintsHandling extends ModuleScript {

  public static final String ALTER_TABLE = "ALTER TABLE IF EXISTS PUBLIC.%s\n";
  private static final Logger log4j = LogManager.getLogger();

  // Schema for backup tables to preserve data during structural changes
  private static final String BACKUP_SCHEMA = "etarc_backup";
  
  // Performance optimization constants  
  private static final int LARGE_TABLE_THRESHOLD = 1000000;
  
  // Constants for commonly used strings to avoid duplication
  private static final String TABLE_NAME_KEY = "tableName";
  private static final String COLUMN_NAME_KEY = "columnName";
  private static final String PK_COLUMN_NAME_KEY = "pkColumnName";
  private static final String CREATE_SCHEMA_SQL = "CREATE SCHEMA IF NOT EXISTS %s";
  private static final String DROP_TABLE_CASCADE_SQL = "DROP TABLE IF EXISTS public.%s CASCADE";
  private static final String ALTER_TABLE_RENAME_SQL = "ALTER TABLE IF EXISTS public.%s RENAME TO %s";

  // Utility classes for better code organization
  private final TableBackupManager backupManager;
  private final DataMigrationService migrationService;
  private final DatabaseOptimizerUtil dbOptimizer;
  private final TableAnalyzer tableAnalyzer;
  private final ConstraintManagementUtils constraintUtils;
  
  // Execution metrics
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
  }
  
  public static class PartitioningException extends Exception {
    public PartitioningException(String message) {
      super(message);
    }
    
    public PartitioningException(String message, Throwable cause) {
      super(message, cause);
    }
  }



  public void execute() {
    try {
      ConnectionProvider cp = getConnectionProvider();
      long runStart = System.nanoTime();
      
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
        backupManager.cleanupOldBackups(cp);
      }
      constraintUtils.executeConstraintSqlIfNeeded(cp, sql.toString());

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
    constraintUtils.executeUpdate(cp, String.format(CREATE_SCHEMA_SQL, BACKUP_SCHEMA));

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
      List<File> xmlFiles = XmlParsingUtils.findTableXmlFiles(tableName, getSourcePath());
      boolean willHaveStructuralChanges = (new TableDefinitionComparator()).isTableDefinitionChanged(tableName, cp, xmlFiles);
      
      if (willHaveStructuralChanges) {
          log4j.info("Table {} is partitioned and will have structural changes. Creating backup...", tableName);
          backupManager.backupTableData(cp, tableName);
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
    return !StringUtils.isBlank(tableName) && !StringUtils.isBlank(partitionCol) && !StringUtils.isBlank(pkCol);
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

    boolean isIncomplete = StringUtils.isBlank(tableName) || StringUtils.isBlank(partitionCol) || StringUtils.isBlank(pkCol);
    List<File> xmlFiles = isIncomplete ? Collections.emptyList() : XmlParsingUtils.findTableXmlFiles(tableName, getSourcePath());

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
      dbOptimizer.optimizeDatabaseForLargeOperations(cp);
      dbOptimizer.logPerformanceMetrics(cp, "Pre-partition " + tableName);

    }

    try {
      // If table needs partition restoration (was unpartitioned by update.database), re-partition it
      if (needsPartitionRestoration) {
        tm.action = "restore-partitioning";
        log4j.info("Table {} should be partitioned but isn't. Restoring partitioning with current data...", tableName);
        tm.migratedRows = restorePartitioningWithData(cp, tableName, partitionCol, pkCol);
      }
      // If the table is partitioned AND has structural changes, perform full migration
      else if (isPartitioned && isStructuralChange) {
        tm.action = "migrate-structural";
        log4j.info("Structural changes detected in partitioned table {}. Performing full migration...", tableName);
        tm.migratedRows = performPartitionedTableMigration(cp, tableName, partitionCol, pkCol);
      } else {
        // Otherwise, just recreate constraints (existing behavior)
        tm.action = "recreate-constraints";
        log4j.info("Recreating constraints for {} (firstRun = {}, structuralChanges = {})", 
                   tableName, firstPartitionRun, isStructuralChange);
        sql.append(constraintUtils.buildConstraintSql(tableName, cp, pkCol, partitionCol, getSourcePath()));
      }
      // Post-processing optimizations for large tables
      if (isLargeTable) {
        dbOptimizer.createOptimizedIndexes(cp, tableName, partitionCol);
        dbOptimizer.analyzePartitionedTable(cp, tableName);
        dbOptimizer.logPerformanceMetrics(cp, "Post-partition " + tableName);
      }
      
    } finally {
      // Restore database settings if they were modified
      if (isLargeTable) {
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
      constraintUtils.dropDependentViewsAndConstraints(cp, tableName);
      
      // Step 2: Rename current partitioned table to temporary name
      log4j.info("Step 2: Renaming {} to {}", tableName, tempTableName);
      constraintUtils.executeUpdate(cp, String.format(ALTER_TABLE_RENAME_SQL, 
                                     tableName, tempTableName));
      
      // Step 3: Create new partitioned table with updated structure based on XML
      log4j.info("Step 3: Creating new partitioned table {} with updated structure", tableName);
      createUpdatedPartitionedTable(cp, tableName, partitionCol);
      
      // Step 4: Ensure partitions schema exists
      log4j.info("Step 4: Ensuring partitions schema '{}' exists", partitionsSchema);
      constraintUtils.executeUpdate(cp, String.format(CREATE_SCHEMA_SQL, partitionsSchema));
      
      // Step 5: Create partitions for the new table
      log4j.info("Step 5: Creating partitions for new table {}", tableName);
      createPartitionsForTable(cp, tableName, tempTableName, partitionCol, partitionsSchema);
      
      // Step 6: Migrate data from temporary table to new partitioned table
      log4j.info("Step 6: Migrating data from {} to {}", tempTableName, tableName);
      long tempTableRows = backupManager.getTableRowCount(cp, tempTableName);
      dataSourceTable = tempTableName;
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
      
      try {
        // Intento principal
        migratedRows = migrationService.migrateDataToNewTable(cp, dataSourceTable, tableName, partitionCol);
        LoggingUtils.logMigrationSuccessMessage(tableName, migratedRows, dataSourceTable);
      } catch (Exception ex) {
          final String srcFqn = dataSourceTable.contains(".") ? dataSourceTable : ("public." + dataSourceTable);
          final String dstFqn = "public." + tableName;
          log4j.warn("Primary migration failed ({}). Falling back to column intersection between {} and {}",
                     ex.getMessage(), srcFqn, dstFqn);
          List<String> matchingCols = findMatchingColumnsPortable(cp, srcFqn, dstFqn);
          if (matchingCols.isEmpty()) {
            log4j.error("Fallback failed: no matching columns found between {} and {}", srcFqn, dstFqn);
            throw ex;
        }
        migratedRows = insertByColumnIntersection(cp, srcFqn, dstFqn, matchingCols);
        LoggingUtils.logMigrationSuccessMessage(tableName, migratedRows, dataSourceTable);
      }      // Step 7: Verify the new table exists and is properly partitioned before proceeding
      log4j.info("Step 7: Verifying new partitioned table {} was created successfully", tableName);
      if (!tableAnalyzer.isTablePartitioned(cp, tableName)) {
        throw new TableMigrationException("New table " + tableName + " is not properly partitioned after migration");
      }
      
      // Step 8: Try to recreate constraints (with error handling)
      log4j.info("Step 8: Recreating constraints for {}", tableName);
      try {
        String constraintSql = constraintUtils.buildConstraintSql(tableName, cp, pkCol, partitionCol, getSourcePath());
        if (!StringUtils.isBlank(constraintSql)) {
          constraintUtils.executeUpdate(cp, constraintSql);
          LoggingUtils.logSuccessRecreatedConstraints(tableName);
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
      constraintUtils.executeUpdate(cp, String.format(DROP_TABLE_CASCADE_SQL, tempTableName));
      
      // Step 10: Clean up backup if we used it
      if (!dataSourceTable.equals(tempTableName)) {
        // We used backup data, clean it up
        String backupTableName = dataSourceTable.substring(dataSourceTable.lastIndexOf(".") + 1);
        log4j.info("Step 10: Cleaning up backup table {} (migration completed successfully)", backupTableName);
        try {
          backupManager.cleanupBackup(cp, tableName, backupTableName);
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
          constraintUtils.executeUpdate(cp, String.format(DROP_TABLE_CASCADE_SQL, tableName));
          constraintUtils.executeUpdate(cp, String.format(ALTER_TABLE_RENAME_SQL, 
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
        migrationService.convertEmptyTableToPartitioned(tableName);
      } else {
        // Has data (either original or from backup), need to migrate safely
        log4j.info("Table has data{}, performing safe migration to restore partitioning", 
                  usingBackup ? " (from backup)" : "");
        
        // Step 3: Rename current table to temporary name
        log4j.info("Step 3: Renaming {} to {}", tableName, tempTableName);
        constraintUtils.executeUpdate(cp, String.format(ALTER_TABLE_RENAME_SQL, 
                                       tableName, tempTableName));
        
        // Step 4: Create new partitioned table with same structure
        log4j.info("Step 4: Creating new partitioned table {} with same structure", tableName);
        if (usingBackup) {
          migrationService.createPartitionedTableFromTemplate(cp, tableName, backupTableName, partitionCol);
        } else {
          migrationService.createPartitionedTableFromTemplate(cp, tableName, tempTableName, partitionCol);
        }
        
        // Step 5: Ensure partitions schema exists
        log4j.info("Step 5: Ensuring partitions schema '{}' exists", partitionsSchema);
        constraintUtils.executeUpdate(cp, String.format(CREATE_SCHEMA_SQL, partitionsSchema));
        
        // Step 6: Create partitions based on data source
        log4j.info("Step 6: Creating partitions for table {}", tableName);
        createPartitionsForTable(cp, tableName, dataSourceTable, partitionCol, partitionsSchema);
        
        // Step 7: Migrate data to new partitioned table
        log4j.info("Step 7: Migrating data from {} to {}", dataSourceTable, tableName);
        migratedRows = migrationService.migrateDataToNewTable(cp, dataSourceTable, tableName, partitionCol);
        LoggingUtils.logMigrationSuccessMessage(tableName, migratedRows, dataSourceTable);

        // Step 8: Drop temporary table
        log4j.info("Step 8: Dropping temporary table {} (restoration completed successfully)", tempTableName);
        constraintUtils.executeUpdate(cp, String.format(DROP_TABLE_CASCADE_SQL, tempTableName));
        
        // Step 9: Clean up backup if used
        if (usingBackup) {
          log4j.info("Step 9: Cleaning up backup table {}", dataSourceTable);
          backupManager.cleanupBackup(cp, tableName, backupTableName);
        }
      }
      
      // Step 10: Recreate constraints for the now-partitioned table
      log4j.info("Step 10: Recreating constraints for restored partitioned table {}", tableName);
      String constraintSql = constraintUtils.buildConstraintSql(tableName, cp, pkCol, partitionCol, getSourcePath());
      if (!StringUtils.isBlank(constraintSql)) {
        constraintUtils.executeUpdate(cp, constraintSql);
        LoggingUtils.logSuccessRecreatedConstraints(tableName);
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
          constraintUtils.executeUpdate(cp, String.format(DROP_TABLE_CASCADE_SQL, tableName));
          constraintUtils.executeUpdate(cp, String.format(ALTER_TABLE_RENAME_SQL, 
                                         tempTableName, tableName));
          LoggingUtils.logSuccessRestoredTable(tableName);
        }
      } catch (Exception restoreError) {
        LoggingUtils.logCriticalRestorationError(tableName, restoreError);
      }
      
      throw new PartitioningException("Partitioning restoration failed for " + tableName, e);
    }
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
    throw new OBException("Module script execution failed", e);
  }

  // Summary logging
  private void logRunMetricsSummary(long runStartNs) {
    long totalMs = (System.nanoTime() - runStartNs) / 1_000_000;
    LoggingUtils.logSeparator();
    log4j.info("Partitioning run summary: {} ms", totalMs);
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
    List<File> xmlFiles = XmlParsingUtils.findTableXmlFiles(newTableName, getSourcePath());
    if (xmlFiles.isEmpty()) {
      throw new OBException("No XML definition files found for table " + newTableName);
    }
    
    String createTableSql = generateCreateTableFromXml(newTableName, xmlFiles, partitionCol);

    LoggingUtils.logCreatingPartitionedTable(createTableSql);
  constraintUtils.executeUpdate(cp, createTableSql);
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
      sql.append(XmlParsingUtils.mapXmlTypeToPostgreSQL(column.dataType(), column.length()));

      
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
  private Map<String, ColumnDefinition> parseXmlDefinition(File xmlFile) throws ParserConfigurationException, IOException, SAXException {
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
        constraintUtils.executeUpdate(cp, dropStmt);
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
        constraintUtils.executeUpdate(cp, createPartitionSql);
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

  private List<String> listColumnsForRelation(ConnectionProvider cp, String fqName) throws Exception {
    String fqn = fqName.toLowerCase();
    List<String> cols = new ArrayList<>();
    String sql = ""
      + "SELECT attname "
      + "FROM pg_attribute "
      + "WHERE attrelid = to_regclass(?) "
      + "  AND attnum > 0 "
      + "  AND NOT attisdropped "
      + "ORDER BY attnum";
    try (PreparedStatement ps = cp.getPreparedStatement(sql)) {
      ps.setString(1, fqn);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) cols.add(rs.getString(1));
      }
    }
    return cols;
  }

  private List<String> findMatchingColumnsPortable(ConnectionProvider cp, String srcFqn, String dstFqn) throws Exception {
    List<String> srcCols = listColumnsForRelation(cp, srcFqn);
    List<String> dstCols = listColumnsForRelation(cp, dstFqn);
    if (srcCols.isEmpty() || dstCols.isEmpty()) return Collections.emptyList();
    java.util.Set<String> srcSet = new java.util.HashSet<>(srcCols);
    List<String> intersection = new ArrayList<>();
    for (String d : dstCols) {
      if (srcSet.contains(d)) intersection.add(d);
    }
    log4j.info("Matching columns between {} and {}: {}", srcFqn, dstFqn, intersection);
    return intersection;
  }

  private long insertByColumnIntersection(ConnectionProvider cp, String srcFqn, String dstFqn, List<String> cols) throws Exception {
    if (cols.isEmpty()) return 0L;
    String colList = String.join(", ", cols);
    String sql = "INSERT INTO " + dstFqn + " (" + colList + ") SELECT " + colList + " FROM " + srcFqn;
    try (PreparedStatement ps = cp.getPreparedStatement(sql)) {
      return ps.executeUpdate();
    }
  }
}
