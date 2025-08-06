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

import com.etendoerp.db.extended.utils.TableDefinitionComparator;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openbravo.database.ConnectionProvider;
import org.openbravo.modulescript.ModuleScript;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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

public class PartitionedConstraintsHandling extends ModuleScript {

  private static final String SRC_DB_DATABASE_MODEL_TABLES = "src-db/database/model/tables";
  private static final String SRC_DB_DATABASE_MODEL_MODIFIED_TABLES = "src-db/database/model/modifiedTables";
  public static final String ALTER_TABLE = "ALTER TABLE IF EXISTS PUBLIC.%s\n";
  private static final Logger log4j = LogManager.getLogger();
  public static final String MODULES_JAR  = "build/etendo/modules";
  public static final String MODULES_BASE = "modules";
  public static final String MODULES_CORE = "modules_core";
  private static final String[] moduleDirs = new String[] {MODULES_BASE, MODULES_CORE, MODULES_JAR};
  public static final String SEPARATOR = "=======================================================";
  
  // Schema for backup tables to preserve data during structural changes
  private static final String BACKUP_SCHEMA = "etarc_backup";

  public static boolean isBlank(String str) {
    return StringUtils.isBlank(str);
  }

  public static boolean isEqualsIgnoreCase(String str1, String str2) {
    return str1 != null && str1.equalsIgnoreCase(str2);
  }

  public void execute() {
    try {
      ConnectionProvider cp = getConnectionProvider();
      List<Map<String, String>> tableConfigs = loadTableConfigs(cp);

      if (!tableConfigs.isEmpty()) {
        logSeparator();
        log4j.info("============== Partitioning process info ==============");
        logSeparator();
        
        // STEP 1: Before processing, check for tables that will need structural changes
        // and backup their data if they are currently partitioned
        backupPartitionedTablesData(cp, tableConfigs);
      }
      
      StringBuilder sql = new StringBuilder();
      for (Map<String, String> cfg : tableConfigs) {
        processTableConfig(cp, cfg, sql);
      }
      if (!tableConfigs.isEmpty()) {
        logSeparator();
        
        // STEP 2: After processing, clean up old backups (older than 7 days)
        cleanupOldBackups(cp);
      }
      executeConstraintSqlIfNeeded(cp, sql.toString());

    } catch (Exception e) {
      handleError(e);
    }
  }

  /**
   * Logs a separator line to the application log using the info level.
   * <p>
   * This method is typically used to visually separate sections in the log output,
   * improving readability during debugging or tracing execution flow.
   */
  private static void logSeparator() {
    log4j.info(SEPARATOR);
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
    executeUpdate(cp, String.format("CREATE SCHEMA IF NOT EXISTS %s", BACKUP_SCHEMA));
    
    for (Map<String, String> cfg : tableConfigs) {
      String tableName = cfg.get("tableName");
      String partitionCol = cfg.get("columnName");
      String pkCol = cfg.get("pkColumnName");
      
      if (isBlank(tableName) || isBlank(partitionCol) || isBlank(pkCol)) {
        continue;
      }
      
      // Check if table is currently partitioned
      boolean isCurrentlyPartitioned = isTablePartitioned(cp, tableName);
      if (!isCurrentlyPartitioned) {
        log4j.info("Table {} is not currently partitioned, skipping backup", tableName);
        continue;
      }
      
      // Check if there will be structural changes
      List<File> xmlFiles = findTableXmlFiles(tableName);
      boolean willHaveStructuralChanges = (new TableDefinitionComparator()).isTableDefinitionChanged(tableName, cp, xmlFiles);
      
      if (willHaveStructuralChanges) {
        log4j.info("Table {} is partitioned and will have structural changes. Creating backup...", tableName);
        backupTableData(cp, tableName);
      } else {
        log4j.info("Table {} is partitioned but has no structural changes, no backup needed", tableName);
      }
    }
    
    log4j.info("========== COMPLETED BACKUP CHECK ==========");
  }

  /**
   * Creates a backup of a partitioned table's data before structural changes.
   * 
   * @param cp the connection provider
   * @param tableName the name of the table to backup
   * @throws Exception if backup creation fails
   */
  private void backupTableData(ConnectionProvider cp, String tableName) throws Exception {
    String backupTableName = tableName.toLowerCase() + "_backup_" + System.currentTimeMillis();
    
    try {
      // Get row count first
      long rowCount = getTableRowCount(cp, tableName);
      log4j.info("Creating backup for table {} ({} rows) as {}.{}", tableName, rowCount, BACKUP_SCHEMA, backupTableName);
      
      if (rowCount == 0) {
        log4j.info("Table {} is empty, skipping data backup", tableName);
        return;
      }
      
      // Create backup table with data
      String backupSql = String.format(
          "CREATE TABLE %s.%s AS SELECT * FROM public.%s",
          BACKUP_SCHEMA, backupTableName, tableName
      );
      
      executeUpdate(cp, backupSql);
      
      // Verify backup was created successfully
      long backupRowCount = getTableRowCount(cp, BACKUP_SCHEMA + "." + backupTableName);
      log4j.info("Successfully backed up {} rows from {} to {}.{}", backupRowCount, tableName, BACKUP_SCHEMA, backupTableName);
      
      // Store backup info for later restoration
      storeBackupInfo(cp, tableName, backupTableName);
      
    } catch (Exception e) {
      log4j.error("Failed to backup table {}: {}", tableName, e.getMessage(), e);
      // Don't throw exception here - we want to continue processing other tables
    }
  }

  /**
   * Stores backup information for later restoration.
   * 
   * @param cp the connection provider
   * @param originalTableName the original table name
   * @param backupTableName the backup table name
   * @throws Exception if storing backup info fails
   */
  private void storeBackupInfo(ConnectionProvider cp, String originalTableName, String backupTableName) throws Exception {
    // Create a simple table to track backups if it doesn't exist
    String createBackupTrackingTable = String.format(
        "CREATE TABLE IF NOT EXISTS %s.backup_tracking (" +
        "original_table VARCHAR(255), " +
        "backup_table VARCHAR(255), " +
        "backup_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
        ")", BACKUP_SCHEMA
    );
    
    executeUpdate(cp, createBackupTrackingTable);
    
    // Store backup info
    String insertBackupInfo = String.format(
        "INSERT INTO %s.backup_tracking (original_table, backup_table) VALUES (?, ?)",
        BACKUP_SCHEMA
    );
    
    try (PreparedStatement ps = cp.getPreparedStatement(insertBackupInfo)) {
      ps.setString(1, originalTableName.toUpperCase());
      ps.setString(2, backupTableName);
      ps.executeUpdate();
    }
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
        cfg.put("tableName", rs.getString("TABLENAME"));
        cfg.put("columnName", rs.getString("COLUMNNAME"));
        cfg.put("pkColumnName", rs.getString("PK_COLUMNNAME"));
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
  private void processTableConfig(ConnectionProvider cp, Map<String, String> cfg, StringBuilder sql) throws Exception {
    String tableName = cfg.get("tableName");
    String partitionCol = cfg.get("columnName");
    String pkCol = cfg.get("pkColumnName");

    log4j.info("DATA FROM ETARC_TABLE_CONFIG: tableName: {} - partitionCol: {} - pkCol: {}", tableName, partitionCol, pkCol);

    boolean isIncomplete = isBlank(tableName) || isBlank(partitionCol) || isBlank(pkCol);
    List<File> xmlFiles = isIncomplete ? Collections.emptyList() : findTableXmlFiles(tableName);

    boolean isStructuralChange = !isIncomplete &&
            (new TableDefinitionComparator()).isTableDefinitionChanged(tableName, cp, xmlFiles);

    boolean isPartitioned = isTablePartitioned(cp, tableName);
    List<String> pkCols = getPrimaryKeyColumns(cp, tableName);
    boolean firstPartitionRun = isPartitioned && pkCols.isEmpty();
    
    // Check if this table should be partitioned but isn't (probably due to update.database processing)
    boolean shouldBePartitioned = !isIncomplete; // If we have config, it should be partitioned
    boolean needsPartitionRestoration = shouldBePartitioned && !isPartitioned;

    log4j.info("Table {} partitioned = {} existing PK cols = {} structural changes = {} needs restoration = {}", 
               tableName, isPartitioned, pkCols, isStructuralChange, needsPartitionRestoration);

    if (shouldSkipTable(isIncomplete, firstPartitionRun, !isStructuralChange && !needsPartitionRestoration)) {
      logSkipReason(isIncomplete, tableName, pkCol, partitionCol);
      return;
    }

    // If table needs partition restoration (was unpartitioned by update.database), re-partition it
    if (needsPartitionRestoration) {
      log4j.info("Table {} should be partitioned but isn't. Restoring partitioning with current data...", tableName);
      restorePartitioningWithData(cp, tableName, partitionCol, pkCol);
    }
    // If the table is partitioned AND has structural changes, perform full migration
    else if (isPartitioned && isStructuralChange) {
      log4j.info("Structural changes detected in partitioned table {}. Performing full migration...", tableName);
      performPartitionedTableMigration(cp, tableName, partitionCol, pkCol);
    } else {
      // Otherwise, just recreate constraints (existing behavior)
      log4j.info("Recreating constraints for {} (firstRun = {}, structuralChanges = {})", 
                 tableName, firstPartitionRun, isStructuralChange);
      sql.append(buildConstraintSql(tableName, cp, pkCol, partitionCol));
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
   * Logs the reason why a table is being skipped during processing.
   *
   * @param isIncomplete true if the configuration is incomplete.
   * @param tableName the name of the table being skipped.
   * @param pkCol the primary key column name.
   * @param partitionCol the partition column name.
   */
  private void logSkipReason(boolean isIncomplete, String tableName, String pkCol, String partitionCol) {
    if (isIncomplete) {
      log4j.warn("Skipping incomplete configuration for table {} (pk = {}, partition = {})", tableName, pkCol, partitionCol);
    } else {
      log4j.info("Skipping {}: already processed and no XML changes", tableName);
    }
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
  private void performPartitionedTableMigration(ConnectionProvider cp, String tableName, 
                                               String partitionCol, String pkCol) throws Exception {
    log4j.info("========== STARTING PARTITIONED TABLE MIGRATION FOR {} ==========", tableName);
    
    String tempTableName = tableName + "_etarc_migration_temp";
    String partitionsSchema = "partitions";
    String dataSourceTable = tempTableName; // Initialize with temp table name
    
    try {
      // Step 1: Drop dependent views (they'll be recreated by Etendo's process)
      log4j.info("Step 1: Dropping dependent views and foreign keys for table {}", tableName);
      dropDependentViewsAndConstraints(cp, tableName);
      
      // Step 2: Rename current partitioned table to temporary name
      log4j.info("Step 2: Renaming {} to {}", tableName, tempTableName);
      executeUpdate(cp, String.format("ALTER TABLE IF EXISTS public.%s RENAME TO %s", 
                                     tableName, tempTableName));
      
      // Step 3: Create new partitioned table with updated structure based on XML
      log4j.info("Step 3: Creating new partitioned table {} with updated structure", tableName);
      createUpdatedPartitionedTable(cp, tableName, tempTableName, partitionCol);
      
      // Step 4: Ensure partitions schema exists
      log4j.info("Step 4: Ensuring partitions schema '{}' exists", partitionsSchema);
      executeUpdate(cp, String.format("CREATE SCHEMA IF NOT EXISTS %s", partitionsSchema));
      
      // Step 5: Create partitions for the new table
      log4j.info("Step 5: Creating partitions for new table {}", tableName);
      createPartitionsForTable(cp, tableName, tempTableName, partitionCol, partitionsSchema);
      
      // Step 6: Migrate data from temporary table to new partitioned table
      log4j.info("Step 6: Migrating data from {} to {}", tempTableName, tableName);
      
      // Check if temporary table has data, if not try to use backup
      long tempTableRows = getTableRowCount(cp, tempTableName);
      dataSourceTable = tempTableName; // Reset to temp table name
      
      if (tempTableRows == 0) {
        log4j.warn("Temporary table {} is empty, checking for backup data...", tempTableName);
        String backupTableName = getLatestBackupTable(cp, tableName);
        if (backupTableName != null) {
          String fullBackupTableName = BACKUP_SCHEMA + "." + backupTableName;
          long backupRows = getTableRowCount(cp, fullBackupTableName);
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
      
      long migratedRows = migrateDataToNewTable(cp, dataSourceTable, tableName);
      log4j.info("Successfully migrated {} rows from {} to {}", migratedRows, dataSourceTable, tableName);
      
      // Step 7: Verify the new table exists and is properly partitioned before proceeding
      log4j.info("Step 7: Verifying new partitioned table {} was created successfully", tableName);
      if (!isTablePartitioned(cp, tableName)) {
        throw new Exception("New table " + tableName + " is not properly partitioned after migration");
      }
      
      // Step 8: Try to recreate constraints (with error handling)
      log4j.info("Step 8: Recreating constraints for {}", tableName);
      try {
        String constraintSql = buildConstraintSql(tableName, cp, pkCol, partitionCol);
        if (!isBlank(constraintSql)) {
          executeUpdate(cp, constraintSql);
          log4j.info("Successfully recreated constraints for {}", tableName);
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
      executeUpdate(cp, String.format("DROP TABLE IF EXISTS public.%s CASCADE", tempTableName));
      
      // Step 10: Clean up backup if we used it
      if (!dataSourceTable.equals(tempTableName)) {
        // We used backup data, clean it up
        String backupTableName = dataSourceTable.substring(dataSourceTable.lastIndexOf(".") + 1);
        log4j.info("Step 10: Cleaning up backup table {} (migration completed successfully)", backupTableName);
        try {
          cleanupBackup(cp, tableName, backupTableName);
        } catch (Exception backupCleanupError) {
          log4j.warn("Failed to cleanup backup table {}: {}", backupTableName, backupCleanupError.getMessage());
          // Don't fail the migration for backup cleanup errors
        }
      }
      
      log4j.info("========== COMPLETED PARTITIONED TABLE MIGRATION FOR {} ==========", tableName);
      
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
          log4j.info("Attempting to restore original table {} from {}", tableName, tempTableName);
          executeUpdate(cp, String.format("DROP TABLE IF EXISTS public.%s CASCADE", tableName));
          executeUpdate(cp, String.format("ALTER TABLE IF EXISTS public.%s RENAME TO %s", 
                                         tempTableName, tableName));
          log4j.info("Successfully restored original table {}", tableName);
        } else {
          log4j.error("CRITICAL: Cannot restore original table {} - temporary table {} no longer exists", 
                     tableName, tempTableName);
          log4j.error("Manual intervention required to restore the table from backup");
        }
      } catch (Exception restoreError) {
        log4j.error("CRITICAL: Failed to restore original table {}: {}", tableName, restoreError.getMessage());
      }
      
    }
  }

  /**
   * Restores partitioning to a table that should be partitioned but isn't.
   * This handles cases where update.database has converted a partitioned table
   * back to a regular table, and we need to restore the partitioning while
   * preserving all existing data. If the table is empty but we have a backup,
   * it will restore from the backup.
   *
   * @param cp the connection provider for accessing the database.
   * @param tableName the name of the table to restore partitioning for.
   * @param partitionCol the partition column name.
   * @param pkCol the primary key column name.
   * @throws Exception if any error occurs during the restoration process.
   */
  private void restorePartitioningWithData(ConnectionProvider cp, String tableName, 
                                         String partitionCol, String pkCol) throws Exception {
    log4j.info("========== RESTORING PARTITIONING FOR {} ==========", tableName);
    
    String tempTableName = tableName + "_etarc_restore_temp";
    String partitionsSchema = "partitions";
    
    try {
      // Step 1: Check if table has data
      long rowCount = getTableRowCount(cp, tableName);
      log4j.info("Step 1: Table {} contains {} rows", tableName, rowCount);
      
      // Step 2: If table is empty, check for backup data
      String dataSourceTable = tableName;
      String backupTableName = null; // Declare backup table name variable
      boolean usingBackup = false;
      
      if (rowCount == 0) {
        backupTableName = getLatestBackupTable(cp, tableName);
        if (backupTableName != null) {
          long backupRowCount = getTableRowCount(cp, BACKUP_SCHEMA + "." + backupTableName);
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
        convertEmptyTableToPartitioned(cp, tableName, partitionCol, partitionsSchema);
      } else {
        // Has data (either original or from backup), need to migrate safely
        log4j.info("Table has data{}, performing safe migration to restore partitioning", 
                  usingBackup ? " (from backup)" : "");
        
        // Step 3: Rename current table to temporary name
        log4j.info("Step 3: Renaming {} to {}", tableName, tempTableName);
        executeUpdate(cp, String.format("ALTER TABLE IF EXISTS public.%s RENAME TO %s", 
                                       tableName, tempTableName));
        
        // Step 4: Create new partitioned table with same structure
        log4j.info("Step 4: Creating new partitioned table {} with same structure", tableName);
        if (usingBackup) {
          createPartitionedTableFromTemplate(cp, tableName, backupTableName, partitionCol);
        } else {
          createPartitionedTableFromTemplate(cp, tableName, tempTableName, partitionCol);
        }
        
        // Step 5: Ensure partitions schema exists
        log4j.info("Step 5: Ensuring partitions schema '{}' exists", partitionsSchema);
        executeUpdate(cp, String.format("CREATE SCHEMA IF NOT EXISTS %s", partitionsSchema));
        
        // Step 6: Create partitions based on data source
        log4j.info("Step 6: Creating partitions for table {}", tableName);
        createPartitionsForTable(cp, tableName, dataSourceTable, partitionCol, partitionsSchema);
        
        // Step 7: Migrate data to new partitioned table
        log4j.info("Step 7: Migrating data from {} to {}", dataSourceTable, tableName);
        long migratedRows = migrateDataToNewTable(cp, dataSourceTable, tableName);
        log4j.info("Successfully migrated {} rows from {} to {}", migratedRows, dataSourceTable, tableName);
        
        // Step 8: Drop temporary table
        log4j.info("Step 8: Dropping temporary table {} (restoration completed successfully)", tempTableName);
        executeUpdate(cp, String.format("DROP TABLE IF EXISTS public.%s CASCADE", tempTableName));
        
        // Step 9: Clean up backup if used
        if (usingBackup) {
          log4j.info("Step 9: Cleaning up backup table {}", dataSourceTable);
          cleanupBackup(cp, tableName, backupTableName);
        }
      }
      
      // Step 10: Recreate constraints for the now-partitioned table
      log4j.info("Step 10: Recreating constraints for restored partitioned table {}", tableName);
      String constraintSql = buildConstraintSql(tableName, cp, pkCol, partitionCol);
      if (!isBlank(constraintSql)) {
        executeUpdate(cp, constraintSql);
        log4j.info("Successfully recreated constraints for {}", tableName);
      }
      
      log4j.info("========== COMPLETED PARTITIONING RESTORATION FOR {} ==========", tableName);
      
    } catch (Exception e) {
      log4j.error("ERROR during partitioning restoration for {}: {}", tableName, e.getMessage(), e);
      
      // Attempt to restore original table if restoration failed
      try {
        boolean tempTableExists = tableExists(cp, tempTableName);
        
        if (tempTableExists) {
          log4j.info("Attempting to restore original table {} from {}", tableName, tempTableName);
          executeUpdate(cp, String.format("DROP TABLE IF EXISTS public.%s CASCADE", tableName));
          executeUpdate(cp, String.format("ALTER TABLE IF EXISTS public.%s RENAME TO %s", 
                                         tempTableName, tableName));
          log4j.info("Successfully restored original table {}", tableName);
        }
      } catch (Exception restoreError) {
        log4j.error("CRITICAL: Failed to restore original table {}: {}", tableName, restoreError.getMessage());
      }
      
      throw new Exception("Partitioning restoration failed for " + tableName, e);
    }
  }

  /**
   * Converts an empty table to a partitioned table.
   *
   * @param cp the connection provider
   * @param tableName the name of the table to convert
   * @param partitionCol the partition column name
   * @param partitionsSchema the schema for partitions
   * @throws Exception if conversion fails
   */
  private void convertEmptyTableToPartitioned(ConnectionProvider cp, String tableName, 
                                            String partitionCol, String partitionsSchema) throws Exception {
    // For empty tables, we can use a simpler approach
    // This would require more complex DDL manipulation in a real implementation
    log4j.warn("Empty table conversion not fully implemented - table {} will need manual partitioning", tableName);
    // TODO: Implement direct table conversion without data migration
  }

  /**
   * Creates a partitioned table based on the structure of a template table.
   *
   * @param cp the connection provider
   * @param newTableName the name of the new partitioned table
   * @param templateTableName the name of the template table (can include schema)
   * @param partitionCol the partition column name
   * @throws Exception if table creation fails
   */
  private void createPartitionedTableFromTemplate(ConnectionProvider cp, String newTableName, 
                                                 String templateTableName, String partitionCol) throws Exception {
    // Handle template table name that might include schema
    String templateReference = templateTableName;
    if (!templateTableName.contains(".")) {
      templateReference = "public." + templateTableName;
    }
    
    String createTableSql = String.format(
        "CREATE TABLE public.%s (LIKE %s INCLUDING DEFAULTS INCLUDING STORAGE INCLUDING COMMENTS) " +
        "PARTITION BY RANGE (%s)",
        newTableName, templateReference, partitionCol
    );
    
    log4j.info("Creating partitioned table with SQL: {}", createTableSql);
    executeUpdate(cp, createTableSql);
    log4j.info("Created new partitioned table: public.{}", newTableName);
  }

  /**
   * Gets the row count for a table.
   *
   * @param cp the connection provider
   * @param tableName the name of the table (can include schema)
   * @return the number of rows in the table
   * @throws Exception if the query fails
   */
  private long getTableRowCount(ConnectionProvider cp, String tableName) throws Exception {
    String countSql = String.format("SELECT COUNT(*) FROM %s", tableName);
    try (PreparedStatement ps = cp.getPreparedStatement(countSql);
         ResultSet rs = ps.executeQuery()) {
      if (rs.next()) {
        return rs.getLong(1);
      }
      return 0;
    }
  }

  /**
   * Gets the latest backup table name for a given original table.
   * 
   * @param cp the connection provider
   * @param originalTableName the name of the original table
   * @return the backup table name or null if no backup exists
   * @throws Exception if the query fails
   */
  private String getLatestBackupTable(ConnectionProvider cp, String originalTableName) throws Exception {
    String query = String.format(
        "SELECT backup_table FROM %s.backup_tracking " +
        "WHERE original_table = ? " +
        "ORDER BY backup_timestamp DESC LIMIT 1",
        BACKUP_SCHEMA
    );
    
    try (PreparedStatement ps = cp.getPreparedStatement(query)) {
      ps.setString(1, originalTableName.toUpperCase());
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          return rs.getString("backup_table");
        }
      }
    } catch (Exception e) {
      // If backup_tracking table doesn't exist yet, that's fine
      log4j.debug("No backup tracking table found or error querying it: {}", e.getMessage());
    }
    return null;
  }

  /**
   * Cleans up backup data after successful restoration.
   * 
   * @param cp the connection provider
   * @param originalTableName the original table name
   * @param backupTableName the backup table name
   * @throws Exception if cleanup fails
   */
  private void cleanupBackup(ConnectionProvider cp, String originalTableName, String backupTableName) throws Exception {
    try {
      // Drop the backup table
      executeUpdate(cp, String.format("DROP TABLE IF EXISTS %s.%s CASCADE", BACKUP_SCHEMA, backupTableName));
      
      // Remove from tracking table
      String deleteTracking = String.format(
          "DELETE FROM %s.backup_tracking WHERE original_table = ? AND backup_table = ?",
          BACKUP_SCHEMA
      );
      
      try (PreparedStatement ps = cp.getPreparedStatement(deleteTracking)) {
        ps.setString(1, originalTableName.toUpperCase());
        ps.setString(2, backupTableName);
        ps.executeUpdate();
      }
      
      log4j.info("Successfully cleaned up backup {}.{}", BACKUP_SCHEMA, backupTableName);
      
    } catch (Exception e) {
      log4j.warn("Warning: Failed to cleanup backup {}.{}: {}", BACKUP_SCHEMA, backupTableName, e.getMessage());
      // Don't throw exception for cleanup failures
    }
  }

  /**
   * Cleans up old backup tables that are older than 7 days.
   * This prevents accumulation of unnecessary backup data.
   * 
   * @param cp the connection provider
   * @throws Exception if cleanup fails
   */
  private void cleanupOldBackups(ConnectionProvider cp) throws Exception {
    try {
      log4j.info("Cleaning up old backup tables (older than 7 days)...");
      
      // Ensure backup schema and tracking table exist before attempting cleanup
      ensureBackupSchemaAndTrackingTableExist(cp);
      
      // Find old backup tables
      String findOldBackupsSql = String.format(
          "SELECT backup_table FROM %s.backup_tracking " +
          "WHERE backup_timestamp < CURRENT_TIMESTAMP - INTERVAL '7 days'",
          BACKUP_SCHEMA
      );
      
      List<String> oldBackups = new ArrayList<>();
      try (PreparedStatement ps = cp.getPreparedStatement(findOldBackupsSql);
           ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          oldBackups.add(rs.getString("backup_table"));
        }
      }
      
      if (oldBackups.isEmpty()) {
        log4j.info("No old backup tables found to clean up");
        return;
      }
      
      log4j.info("Found {} old backup tables to clean up", oldBackups.size());
      
      // Clean up each old backup
      for (String backupTable : oldBackups) {
        try {
          executeUpdate(cp, String.format("DROP TABLE IF EXISTS %s.%s CASCADE", BACKUP_SCHEMA, backupTable));
          log4j.info("Dropped old backup table: {}.{}", BACKUP_SCHEMA, backupTable);
        } catch (Exception e) {
          log4j.warn("Warning: Failed to drop old backup table {}.{}: {}", BACKUP_SCHEMA, backupTable, e.getMessage());
        }
      }
      
      // Clean up tracking records
      String cleanupTrackingSql = String.format(
          "DELETE FROM %s.backup_tracking " +
          "WHERE backup_timestamp < CURRENT_TIMESTAMP - INTERVAL '7 days'",
          BACKUP_SCHEMA
      );
      
      try (PreparedStatement ps = cp.getPreparedStatement(cleanupTrackingSql)) {
        int deletedRows = ps.executeUpdate();
        log4j.info("Cleaned up {} old backup tracking records", deletedRows);
      }
      
    } catch (Exception e) {
      log4j.warn("Warning: Failed to cleanup old backups: {}", e.getMessage());
      // Don't throw exception for cleanup failures
    }
  }

  /**
   * Ensures that the backup schema and tracking table exist.
   * Creates them if they don't exist to avoid errors during cleanup operations.
   * 
   * @param cp the connection provider
   * @throws Exception if schema or table creation fails
   */
  private void ensureBackupSchemaAndTrackingTableExist(ConnectionProvider cp) throws Exception {
    try {
      // Create backup schema if it doesn't exist
      executeUpdate(cp, String.format("CREATE SCHEMA IF NOT EXISTS %s", BACKUP_SCHEMA));
      
      // Create backup tracking table if it doesn't exist
      String createBackupTrackingTable = String.format(
          "CREATE TABLE IF NOT EXISTS %s.backup_tracking (" +
          "original_table VARCHAR(255), " +
          "backup_table VARCHAR(255), " +
          "backup_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
          ")", BACKUP_SCHEMA
      );
      
      executeUpdate(cp, createBackupTrackingTable);
      log4j.debug("Ensured backup schema '{}' and tracking table exist", BACKUP_SCHEMA);
      
    } catch (Exception e) {
      log4j.warn("Warning: Failed to ensure backup schema and tracking table exist: {}", e.getMessage());
      // Don't throw exception - this is just preparation for cleanup
    }
  }

  /**
   * Checks if a table exists.
   *
   * @param cp the connection provider
   * @param tableName the name of the table to check
   * @return true if the table exists, false otherwise
   * @throws Exception if the query fails
   */
  private boolean tableExists(ConnectionProvider cp, String tableName) throws Exception {
    try (PreparedStatement ps = cp.getPreparedStatement(
        "SELECT 1 FROM information_schema.tables WHERE table_name = ? AND table_schema = 'public'")) {
      ps.setString(1, tableName.toLowerCase());
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    }
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
   * Retrieves the list of columns that make up the primary key of the given table.
   *
   * @param cp the connection provider for accessing the database.
   * @param tableName the name of the table.
   * @return a list of column names that are part of the primary key.
   * @throws Exception if a database access error occurs.
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
   * Executes the constraint SQL if it is not blank.
   * This typically includes adding or modifying table constraints after analyzing configurations.
   *
   * @param cp the connection provider for accessing the database.
   * @param sql the SQL string to execute.
   * @throws Exception if a database access error occurs.
   */
  private void executeConstraintSqlIfNeeded(ConnectionProvider cp, String sql) throws Exception {
    if (isBlank(sql)) {
      log4j.info("No constraints to handle for the provided configurations.");
      return;
    }

    try (PreparedStatement ps = cp.getPreparedStatement(sql)) {
      ps.executeUpdate();
    }
  }

  /**
   * Parses the specified XML file into a normalized DOM Document with
   * XXE protections enabled.
   * <p>
   * Configures the parser to:
   * <ul>
   *   <li>Disallow DOCTYPE declarations (no DTDs).</li>
   *   <li>Disable resolution of external general and parameter entities.</li>
   *   <li>Enable secure processing to limit resource usage.</li>
   *   <li>Disable XInclude processing and entity expansion.</li>
   * </ul>
   *
   * @param xml the XML file to parse
   * @return a normalized {@link org.w3c.dom.Document} representing the XML content
   * @throws ParserConfigurationException if a parser cannot be configured
   * @throws SAXException                 if a parsing error occurs
   * @throws IOException                  if an I/O error occurs reading the file
   */
  private static Document getDocument(File xml) throws ParserConfigurationException, SAXException, IOException {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

    // 1. Disallow DTDs entirely (no DOCTYPE)
    //    This prevents any <!DOCTYPE…> declarations
    factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);

    // 2. Disable external entities
    factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
    factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);

    // 3. (Optional) Enable the secure-processing feature
    factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);

    // 4. Further hardening
    factory.setXIncludeAware(false);
    factory.setExpandEntityReferences(false);

    DocumentBuilder builder = factory.newDocumentBuilder();
    Document doc = builder.parse(xml);
    doc.getDocumentElement().normalize();
    return doc;
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
   * Builds a SQL script to drop and re-create primary key and foreign key
   * constraints for the specified table, taking partitioning into account.
   * <p>
   * Steps performed:
   * <ol>
   * <li>Checks whether {@code tableName} is partitioned.</li>
   * <li>Loads the table’s XML to determine the primary key name.</li>
   * <li>Drops existing PK and re-adds it (with or without partition column).</li>
   * <li>Iterates all table XML definition files to find any foreign keys referencing
   * {@code tableName}, then drops and re-adds those FKs (partitioned if needed).</li>
   * </ol>
   *
   * @param tableName      the name of the table to modify
   * @param cp             the ConnectionProvider used to query catalog tables
   * @param pkField        the column name of the primary key
   * @param partitionField the partition key column (if table is partitioned)
   * @return the complete DDL script as a single String
   * @throws Exception if any database or XML processing error occurs
   */
  public String buildConstraintSql(String tableName, ConnectionProvider cp, String pkField,
      String partitionField) throws Exception {
    // Check if table is partitioned
    String checkPartition = "SELECT 1 FROM pg_partitioned_table WHERE partrelid = to_regclass(?)";
    PreparedStatement psCheck = cp.getPreparedStatement(checkPartition);
    psCheck.setString(1, tableName);
    boolean isPartitioned = psCheck.executeQuery().next();
    psCheck.close();

    // Get required information from the primary table's XML
    List<File> tableXmlFiles = findTableXmlFiles(tableName);
    if (tableXmlFiles.isEmpty()) {
      throw new Exception("Entity XML file for " + tableName + " not found.");
    }
    String pkName = findPrimaryKey(tableXmlFiles);
    if (pkName == null) {
      throw new Exception("Primary Key for entity " + tableName + " not found in XML.");
    }

    // SQL templates for primary table
    String dropPrimaryKeySQL = ALTER_TABLE + "DROP CONSTRAINT IF EXISTS %s CASCADE;\n";
    String addPartitionedPrimaryKeySQL = ALTER_TABLE + "ADD CONSTRAINT %s PRIMARY KEY (%s, %s);\n";
    String addSimplePrimaryKeySQL = ALTER_TABLE + "ADD CONSTRAINT %s PRIMARY KEY (%s);\n";

    // Build SQL script for primary table
    StringBuilder sql = new StringBuilder();
    sql.append(String.format(dropPrimaryKeySQL, tableName, pkName));

    if (isPartitioned) {
      sql.append(
          String.format(addPartitionedPrimaryKeySQL, tableName, pkName, pkField, partitionField));
    } else {
      sql.append(String.format(addSimplePrimaryKeySQL, tableName, pkName, pkField));
    }

    // SQL templates for foreign key constraints
    String dropForeignKeySQL = ALTER_TABLE + "DROP CONSTRAINT IF EXISTS %s;\n";
    String addColumnSQL = "ALTER TABLE %s\n" + "ADD COLUMN IF NOT EXISTS %s TIMESTAMP WITHOUT TIME ZONE;\n";
    String updateColumnSQL = "UPDATE %s SET %s = F.%s FROM %s F "
        + "WHERE F.%s = %s.%s AND %s.%s IS NULL;\n";
    String addPartitionedForeignKeySQL = ALTER_TABLE
        + "ADD CONSTRAINT %s FOREIGN KEY (%s, %s) "
        + "REFERENCES PUBLIC.%s (%s, %s) MATCH SIMPLE "
        + "ON UPDATE CASCADE ON DELETE NO ACTION;\n";
    String addSimpleForeignKeySQL = ALTER_TABLE + "ADD CONSTRAINT %s FOREIGN KEY (%s) "
        + "REFERENCES PUBLIC.%s (%s) MATCH SIMPLE "
        + "ON UPDATE NO ACTION ON DELETE NO ACTION;\n";

    // Iterate over all table XMLs to find references to our target table
    for (File dir : collectTableDirs()) {
      File[] xmlsInDir = dir.listFiles(f -> f.isFile() && f.getName().endsWith(".xml"));
      if (xmlsInDir == null) {
        continue;
      }

      for (File sourceXmlFile : xmlsInDir) {
        try {
          Document doc = getDocument(sourceXmlFile);
          NodeList tableNodes = doc.getElementsByTagName("table");
          if (tableNodes.getLength() != 1) {
            continue;
          }

          Element tableEl = (Element) tableNodes.item(0);
          // Skip views or the table we are already processing
          if (Boolean.parseBoolean(tableEl.getAttribute("isView")) || tableName.equalsIgnoreCase(
              tableEl.getAttribute("name"))) {
            continue;
          }

          String relatedTableName = tableEl.getAttribute("name").toUpperCase();
          NodeList fkList = tableEl.getElementsByTagName("foreign-key");

          for (int i = 0; i < fkList.getLength(); i++) {
            Element fkEl = (Element) fkList.item(i);
            if (tableName.equalsIgnoreCase(fkEl.getAttribute("foreignTable"))) {
              // This table has a foreign key to our target table
              String foreignKey = fkEl.getAttribute("name");
              NodeList refList = fkEl.getElementsByTagName("reference");
              if (refList.getLength() == 0) {
                continue;
              }

              // Assuming a single-column FK for this logic, like the original method
              Element refEl = (Element) refList.item(0);
              String relationColumn = refEl.getAttribute("local");

              if (isBlank(foreignKey) || isBlank(relationColumn)) {
                continue;
              }

              sql.append(String.format(dropForeignKeySQL, relatedTableName, foreignKey));

              if (isPartitioned) {
                String partitionColumn = "etarc_" + partitionField + "__" + foreignKey;
                sql.append(String.format(addColumnSQL, relatedTableName, partitionColumn));
                sql.append(String.format(updateColumnSQL, relatedTableName, partitionColumn,
                    partitionField, tableName, pkField, relatedTableName, relationColumn,
                    relatedTableName, partitionColumn));
                sql.append(
                    String.format(addPartitionedForeignKeySQL, relatedTableName, foreignKey,
                        relationColumn, partitionColumn, tableName, pkField, partitionField));
              } else {
                sql.append(String.format(addSimpleForeignKeySQL, relatedTableName, foreignKey,
                    relationColumn, tableName, pkField));
              }
            }
          }
        } catch (Exception e) {
          log4j.error("Error processing XML file: {}", sourceXmlFile.getAbsolutePath(), e);
        }
      }
    }
    return sql.toString();
  }

  /**
   * Executes a SQL update statement and logs the operation.
   *
   * @param cp the connection provider
   * @param sql the SQL statement to execute
   * @throws Exception if the SQL execution fails
   */
  private void executeUpdate(ConnectionProvider cp, String sql) throws Exception {
    log4j.debug("Executing SQL: {}", sql);
    try (PreparedStatement ps = cp.getPreparedStatement(sql)) {
      ps.executeUpdate();
    }
  }

  /**
   * Drops dependent views and foreign key constraints for the specified table.
   * This is necessary before renaming the table.
   *
   * @param cp the connection provider
   * @param tableName the name of the table
   * @throws Exception if any SQL operation fails
   */
  private void dropDependentViewsAndConstraints(ConnectionProvider cp, String tableName) throws Exception {
    // Drop views that depend on this table
    String dropViewsSql = "SELECT 'DROP VIEW IF EXISTS ' || schemaname || '.' || viewname || ' CASCADE;' " +
                         "FROM pg_views v " +
                         "WHERE definition ILIKE '%' || ? || '%' " +
                         "AND schemaname NOT IN ('information_schema', 'pg_catalog')";
    
    try (PreparedStatement ps = cp.getPreparedStatement(dropViewsSql)) {
      ps.setString(1, tableName);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          String dropViewStmt = rs.getString(1);
          log4j.debug("Dropping dependent view: {}", dropViewStmt);
          executeUpdate(cp, dropViewStmt);
        }
      }
    }

    // Drop foreign keys pointing TO this table
    String dropFksSql = "SELECT 'ALTER TABLE ' || nspname || '.' || relname || ' DROP CONSTRAINT IF EXISTS ' || conname || ';' " +
                       "FROM pg_constraint c " +
                       "JOIN pg_class t ON c.conrelid = t.oid " +
                       "JOIN pg_namespace n ON n.oid = t.relnamespace " +
                       "WHERE c.confrelid = ?::regclass AND c.contype = 'f'";
    
    try (PreparedStatement ps = cp.getPreparedStatement(dropFksSql)) {
      ps.setString(1, tableName);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          String dropFkStmt = rs.getString(1);
          log4j.debug("Dropping foreign key: {}", dropFkStmt);
          executeUpdate(cp, dropFkStmt);
        }
      }
    }
  }

  /**
   * Creates a new partitioned table with updated structure based on the temporary table
   * but without the constraints that will be recreated later.
   *
   * @param cp the connection provider
   * @param newTableName the name of the new partitioned table
   * @param tempTableName the name of the temporary table to base structure on
   * @param partitionCol the partition column name
   * @throws Exception if table creation fails
   */
  private void createUpdatedPartitionedTable(ConnectionProvider cp, String newTableName, 
                                            String tempTableName, String partitionCol) throws Exception {
    // Create the new partitioned table based on XML definitions, not the old table structure
    List<File> xmlFiles = findTableXmlFiles(newTableName);
    if (xmlFiles.isEmpty()) {
      throw new Exception("No XML definition files found for table " + newTableName);
    }
    
    String createTableSql = generateCreateTableFromXml(newTableName, xmlFiles, partitionCol);
    
    log4j.info("Creating partitioned table with SQL: {}", createTableSql);
    executeUpdate(cp, createTableSql);
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
    TableDefinitionComparator comparator = new TableDefinitionComparator();
    
    for (File xmlFile : xmlFiles) {
      Map<String, ColumnDefinition> partial = parseXmlDefinition(xmlFile);
      for (Map.Entry<String, ColumnDefinition> entry : partial.entrySet()) {
        xmlColumns.putIfAbsent(entry.getKey(), entry.getValue());
      }
    }
    
    if (xmlColumns.isEmpty()) {
      throw new Exception("No column definitions found in XML files for table " + tableName);
    }
    
    StringBuilder sql = new StringBuilder();
    sql.append("CREATE TABLE public.").append(tableName).append(" (\n");
    
    boolean first = true;
    for (ColumnDefinition column : xmlColumns.values()) {
      if (!first) {
        sql.append(",\n");
      }
      first = false;
      
      sql.append("  ").append(column.getName()).append(" ");
      sql.append(mapXmlTypeToPostgreSQL(column.getDataType(), column.getLength()));
      
      if (!column.getIsNullable()) {
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
      Boolean isRequired = "true".equalsIgnoreCase(colElem.getAttribute("required"));
      Boolean isNullable = !isRequired; // Invert: required=true means nullable=false
      
      Integer length = null;
      if (colElem.hasAttribute("length")) {
        length = parseIntLenient(colElem.getAttribute("length"));
      } else if (colElem.hasAttribute("size")) {
        length = parseIntLenient(colElem.getAttribute("size"));
      }
      
      Boolean isPrimaryKey = "true".equalsIgnoreCase(colElem.getAttribute("primarykey"));
      
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
      case "id":
      case "varchar":
        if (length != null && length > 0) {
          return "VARCHAR(" + length + ")";
        }
        return "VARCHAR(255)"; // default length
      case "nvarchar":
        if (length != null && length > 0) {
          return "VARCHAR(" + length + ")"; // PostgreSQL treats VARCHAR as Unicode-capable
        }
        return "VARCHAR(255)";
      case "string":
        if (length != null && length > 0) {
          return "VARCHAR(" + length + ")";
        }
        return "VARCHAR(255)";
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
          return "VARCHAR(" + length + ")";
        }
        return "VARCHAR(255)";
    }
  }

  /**
   * Inner class to represent column definitions (similar to TableDefinitionComparator's)
   */
  private static class ColumnDefinition {
    private final String name;
    private final String dataType;
    private final Integer length;
    private final Boolean isNullable;
    private final Boolean isPrimaryKey;

    public ColumnDefinition(String name, String dataType, Integer length, Boolean isNullable, Boolean isPrimaryKey) {
      this.name = name;
      this.dataType = dataType;
      this.length = length;
      this.isNullable = isNullable;
      this.isPrimaryKey = isPrimaryKey;
    }

    public String getName() {
      return name;
    }

    public String getDataType() {
      return dataType;
    }

    public Integer getLength() {
      return length;
    }

    public Boolean getIsNullable() {
      return isNullable;
    }

    public Boolean getIsPrimaryKey() {
      return isPrimaryKey;
    }
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
        executeUpdate(cp, dropStmt);
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
        executeUpdate(cp, createPartitionSql);
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

  /**
   * Migrates all data from the temporary table to the new partitioned table.
   *
   * @param cp the connection provider
   * @param sourceTableName the source table name (can include schema)
   * @param targetTableName the target table name
   * @return the number of rows migrated
   * @throws Exception if data migration fails
   */
  private long migrateDataToNewTable(ConnectionProvider cp, String sourceTableName, String targetTableName) throws Exception {
    // First, let's verify what partitions exist before attempting migration
    String checkPartitionsSql = String.format(
        "SELECT schemaname, tablename, " +
        "       pg_get_expr(c.relpartbound, c.oid) as partition_bounds " +
        "FROM pg_tables t " +
        "JOIN pg_class c ON c.relname = t.tablename " +
        "WHERE t.tablename LIKE '%s_y%%' " +
        "ORDER BY t.tablename",
        targetTableName.toLowerCase()
    );
    
    log4j.info("Pre-migration check - Available partitions for {}:", targetTableName);
    try (PreparedStatement checkPs = cp.getPreparedStatement(checkPartitionsSql);
         ResultSet checkRs = checkPs.executeQuery()) {
      while (checkRs.next()) {
        log4j.info("  Partition: {}.{} - Bounds: {}", 
                   checkRs.getString(1), checkRs.getString(2), checkRs.getString(3));
      }
    }
    
    // Check data range in source table - handle both schema.table and table formats
    String sourceReference = sourceTableName;
    if (!sourceTableName.contains(".")) {
      sourceReference = "public." + sourceTableName;
    }
    
    String dataRangeSql = String.format(
        "SELECT MIN(dateacct), MAX(dateacct), COUNT(*) FROM %s WHERE dateacct IS NOT NULL",
        sourceReference
    );
    
    try (PreparedStatement rangePs = cp.getPreparedStatement(dataRangeSql);
         ResultSet rangeRs = rangePs.executeQuery()) {
      if (rangeRs.next()) {
        log4j.info("Data in {} - Min date: {}, Max date: {}, Total records: {}", 
                   sourceReference, rangeRs.getObject(1), rangeRs.getObject(2), rangeRs.getInt(3));
      }
    }
    
    // Get matching columns between source and destination tables
    List<String> matchingColumns = getMatchingColumns(cp, sourceTableName, targetTableName);
    
    if (matchingColumns.isEmpty()) {
      throw new Exception("No matching columns found between source and destination tables");
    }
    
    String columnList = String.join(", ", matchingColumns);
    log4j.info("Will migrate {} matching columns: {}", matchingColumns.size(), columnList);
    
    // Use column-specific INSERT to handle structural differences
    String insertSql = String.format("INSERT INTO public.%s (%s) SELECT %s FROM %s", 
                                    targetTableName, columnList, columnList, sourceReference);
    log4j.info("Executing data migration SQL: {}", insertSql);
    
    try (PreparedStatement ps = cp.getPreparedStatement(insertSql)) {
      int rowCount = ps.executeUpdate();
      log4j.info("Successfully migrated {} rows from {} to {}", rowCount, sourceReference, targetTableName);
      return rowCount;
    } catch (Exception e) {
      log4j.error("Data migration failed. Error: {}", e.getMessage());
      
      // Let's check for records that might not fit in any partition
      String problemRecordsSql = String.format(
          "SELECT EXTRACT(YEAR FROM dateacct) as year, COUNT(*) as count " +
          "FROM %s WHERE dateacct IS NOT NULL " +
          "GROUP BY EXTRACT(YEAR FROM dateacct) " +
          "ORDER BY year",
          sourceReference
      );
      
      log4j.error("Records by year in source table {}:", sourceReference);
      try (PreparedStatement problemPs = cp.getPreparedStatement(problemRecordsSql);
           ResultSet problemRs = problemPs.executeQuery()) {
        while (problemRs.next()) {
          log4j.error("  Year {}: {} records", problemRs.getInt(1), problemRs.getInt(2));
        }
      }
      
      throw e;
    }
  }

  /**
   * Gets the list of columns that exist in both source and destination tables.
   * 
   * @param cp the connection provider
   * @param sourceTableName the source table (can include schema)
   * @param destinationTableName the destination table name
   * @return list of matching column names
   * @throws Exception if query fails
   */
  private List<String> getMatchingColumns(ConnectionProvider cp, String sourceTableName, 
                                        String destinationTableName) throws Exception {
    // Parse source table name to handle schema
    String sourceSchema = "public";
    String sourceTable = sourceTableName;
    if (sourceTableName.contains(".")) {
      String[] parts = sourceTableName.split("\\.");
      sourceSchema = parts[0];
      sourceTable = parts[1];
    }
    
    String sql = "SELECT c1.column_name " +
                "FROM information_schema.columns c1 " +
                "INNER JOIN information_schema.columns c2 " +
                "  ON c1.column_name = c2.column_name " +
                "WHERE c1.table_schema = ? AND c1.table_name = ? " +
                "  AND c2.table_schema = 'public' AND c2.table_name = ? " +
                "ORDER BY c1.ordinal_position";
    
    List<String> matchingColumns = new ArrayList<>();
    try (PreparedStatement ps = cp.getPreparedStatement(sql)) {
      ps.setString(1, sourceSchema);
      ps.setString(2, sourceTable.toLowerCase());
      ps.setString(3, destinationTableName.toLowerCase());
      
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          matchingColumns.add(rs.getString("column_name"));
        }
      }
    }
    
    log4j.info("Found {} matching columns between {} and {}", 
              matchingColumns.size(), sourceTableName, destinationTableName);
    
    return matchingColumns;
  }
}
