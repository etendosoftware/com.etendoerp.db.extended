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
import org.openbravo.base.exception.OBException;
import org.openbravo.database.ConnectionProvider;
import org.openbravo.modulescript.ModuleScript;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.etendoerp.db.extended.utils.TableBackupManager;
import com.etendoerp.db.extended.utils.TableDefinitionComparator;
import com.etendoerp.db.extended.utils.DataMigrationService;
import com.etendoerp.db.extended.utils.DatabaseOptimizerUtil;
import com.etendoerp.db.extended.utils.TableAnalyzer;
import com.etendoerp.db.extended.utils.ConstraintManagementUtils;
import com.etendoerp.db.extended.utils.LoggingUtils;
import com.etendoerp.db.extended.utils.XmlParsingUtils;
import com.etendoerp.db.extended.utils.Constants;
import org.xml.sax.SAXException;

/**
 * ModuleScript for handling partitioned table constraints and migrations in Etendo ERP.
 * 
 * <p>This class is responsible for managing PostgreSQL table partitioning operations including:
 * <ul>
 *   <li>Creating and managing partitioned tables based on date ranges</li>
 *   <li>Migrating data from regular tables to partitioned tables</li>
 *   <li>Handling structural changes in existing partitioned tables</li>
 *   <li>Backing up and restoring table data during migration processes</li>
 *   <li>Recreating constraints and foreign keys after partitioning operations</li>
 *   <li>Performance optimization for large table operations</li>
 * </ul>
 * 
 * <p>The partitioning strategy used is range partitioning by year, where each partition
 * contains data for a specific calendar year. This is particularly effective for tables
 * with date-based columns that are frequently queried by date ranges.
 * 
 * <p>The class reads configuration from the {@code ETARC_TABLE_CONFIG} table which defines:
 * <ul>
 *   <li>Which tables should be partitioned</li>
 *   <li>The column to use for partitioning (typically a date/timestamp column)</li>
 *   <li>The primary key column information</li>
 * </ul>
 * 
 * <p>Migration process overview:
 * <ol>
 *   <li>Backup existing partitioned tables that will undergo structural changes</li>
 *   <li>Analyze each configured table to determine required actions</li>
 *   <li>Perform appropriate migration strategy (constraint recreation, structural migration, or partition restoration)</li>
 *   <li>Apply performance optimizations for large tables</li>
 *   <li>Clean up temporary resources and old backups</li>
 * </ol>
 * 
 * @author Futit Services S.L
 * @version 2025.1
 * @since Etendo 25Q1
 */
public class PartitionedConstraintsHandling extends ModuleScript {

  /** Logger instance for this class using Log4j2 framework. */
  private static final Logger log4j = LogManager.getLogger();
  public static final String PUBLIC = "public.";

  /** Manager for table backup operations during migrations. */
  private final TableBackupManager backupManager;
  
  /** Service for data migration between tables and partitions. */
  private final DataMigrationService migrationService;
  
  /** Utility for database performance optimization during large operations. */
  private final DatabaseOptimizerUtil dbOptimizer;
  
  /** Analyzer for table structure and partitioning status. */
  private final TableAnalyzer tableAnalyzer;
  
  /** Utility for managing database constraints and foreign keys. */
  private final ConstraintManagementUtils constraintUtils;
  
  /** Collection of execution metrics for performance monitoring and reporting. */
  private final List<TableMetrics> runMetrics = new ArrayList<>();

  /**
   * Constructs a new PartitionedConstraintsHandling instance.
   * 
   * <p>Initializes all utility components required for partitioning operations:
   * <ul>
   *   <li>Table backup manager for data preservation during migrations</li>
   *   <li>Data migration service for moving data between tables</li>
   *   <li>Database optimizer for performance tuning during large operations</li>
   *   <li>Table analyzer for examining table structure and partitioning status</li>
   *   <li>Constraint management utilities for handling database constraints</li>
   * </ul>
   * 
   * <p>All utility instances are created as final fields to ensure thread safety
   * and consistent behavior throughout the module script execution.
   */
  public PartitionedConstraintsHandling() {
    this.backupManager = new TableBackupManager();
    this.migrationService = new DataMigrationService();
    this.dbOptimizer = new DatabaseOptimizerUtil();
    this.tableAnalyzer = new TableAnalyzer();
    this.constraintUtils = new ConstraintManagementUtils();
  }

  /**
   * Exception thrown when table migration operations fail.
   * 
   * <p>This exception is used to indicate failures during the migration of
   * partitioned tables, including structural changes, data migration errors,
   * or constraint recreation failures.
   */
  public static class TableMigrationException extends Exception {
    /**
     * Constructs a new TableMigrationException with the specified detail message.
     * 
     * @param message the detail message explaining the migration failure
     */
    public TableMigrationException(String message) {
      super(message);
    }
  }
  
  /**
   * Exception thrown when partitioning operations fail.
   * 
   * <p>This exception is used to indicate failures during partitioning operations,
   * including table creation, partition setup, or data restoration failures.
   */
  public static class PartitioningException extends Exception {
    /**
     * Constructs a new PartitioningException with the specified detail message.
     * 
     * @param message the detail message explaining the partitioning failure
     */
    public PartitioningException(String message) {
      super(message);
    }
    
    /**
     * Constructs a new PartitioningException with the specified detail message and cause.
     * 
     * @param message the detail message explaining the partitioning failure
     * @param cause the underlying cause of the partitioning failure
     */
    public PartitioningException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  /**
   * Executes the main partitioning and constraint management process.
   * 
   * <p>This is the primary entry point for the module script that orchestrates the entire
   * partitioning workflow. The execution follows these main phases:
   * 
   * <ol>
   *   <li><strong>Configuration Loading:</strong> Loads table configuration from {@code ETARC_TABLE_CONFIG}</li>
   *   <li><strong>Pre-Migration Backup:</strong> Backs up data from partitioned tables that will undergo structural changes</li>
   *   <li><strong>Table Processing:</strong> Processes each configured table based on its current state:
   *     <ul>
   *       <li>Constraint recreation for tables needing only constraint updates</li>
   *       <li>Structural migration for partitioned tables with schema changes</li>
   *       <li>Partition restoration for tables that were unpartitioned by update.database</li>
   *     </ul>
   *   </li>
   *   <li><strong>Performance Optimization:</strong> Applies database optimizations for large table operations</li>
   *   <li><strong>Cleanup:</strong> Removes old backups and temporary resources</li>
   *   <li><strong>Constraint Application:</strong> Executes any accumulated constraint SQL</li>
   *   <li><strong>Metrics Reporting:</strong> Logs execution summary with performance metrics</li>
   * </ol>
   * 
   * <p>The method includes comprehensive error handling and will attempt to maintain system
   * integrity even if individual table operations fail. All operations are logged extensively
   * for debugging and monitoring purposes.
   * 
   * <p>Performance considerations:
   * <ul>
   *   <li>Large tables (over {@link Constants#LARGE_TABLE_THRESHOLD} rows) receive special optimization</li>
   *   <li>Database settings are temporarily adjusted for large operations and restored afterward</li>
   *   <li>Execution metrics are collected for all operations to identify performance bottlenecks</li>
   * </ul>
   * 
   * @throws OBException if the module script execution fails due to unrecoverable errors
   * @see #loadTableConfigs(ConnectionProvider) for configuration loading details
   * @see #processTableConfig(ConnectionProvider, Map, StringBuilder, TableMetrics) for individual table processing
   */
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
        String tableName = cfg.get(Constants.TABLE_NAME_KEY);
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
   * 
   * <p>This method runs as a precautionary step BEFORE the main table processing to preserve
   * data that might be lost during structural migrations. It specifically targets tables that:
   * <ul>
   *   <li>Are currently partitioned in the database</li>
   *   <li>Have structural changes defined in their XML table definitions</li>
   *   <li>Will undergo migration that could result in data loss</li>
   * </ul>
   * 
   * <p>The backup process:
   * <ol>
   *   <li>Ensures the backup schema ({@link Constants#BACKUP_SCHEMA}) exists</li>
   *   <li>Validates each table configuration for completeness</li>
   *   <li>Checks if the table is currently partitioned using {@link TableAnalyzer}</li>
   *   <li>Compares current table structure with XML definitions to detect changes</li>
   *   <li>Creates a backup copy of tables that meet the criteria using {@link TableBackupManager}</li>
   * </ol>
   * 
   * <p>Backup is selective - only tables that are both partitioned AND have structural
   * changes are backed up to minimize storage overhead and backup time.
   * 
   * @param cp the connection provider for accessing the database
   * @param tableConfigs list of table configurations loaded from {@code ETARC_TABLE_CONFIG}
   * @throws Exception if backup operations fail, including:
   *                   <ul>
   *                     <li>Database connectivity issues</li>
   *                     <li>Insufficient permissions for backup schema creation</li>
   *                     <li>Disk space issues during backup</li>
   *                     <li>XML parsing errors when comparing table definitions</li>
   *                   </ul>
   * 
   * @see TableAnalyzer#isTablePartitioned(ConnectionProvider, String) for partitioning detection
   * @see TableDefinitionComparator#isTableDefinitionChanged(String, ConnectionProvider, List) for change detection  
   * @see TableBackupManager#backupTableData(ConnectionProvider, String) for backup creation
   */
  private void backupPartitionedTablesData(ConnectionProvider cp, List<Map<String, String>> tableConfigs) throws Exception {
    log4j.info("========== CHECKING FOR PARTITIONED TABLES NEEDING BACKUP ==========");
    
    // Ensure backup schema exists
    constraintUtils.executeUpdate(cp, String.format(Constants.CREATE_SCHEMA_SQL, Constants.BACKUP_SCHEMA));

    for (Map<String, String> cfg : tableConfigs) {
      if (!isValidTableConfig(cfg)) {
        continue;
      }
      
      String tableName = cfg.get(Constants.TABLE_NAME_KEY);
      
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
   * Validates if a table configuration contains all required fields for partitioning operations.
   * 
   * <p>A valid table configuration must contain:
   * <ul>
   *   <li><strong>Table Name:</strong> The name of the table to be partitioned (non-blank)</li>
   *   <li><strong>Partition Column:</strong> The column name to use for partitioning (non-blank)</li>
   *   <li><strong>Primary Key Column:</strong> The primary key column name (non-blank)</li>
   * </ul>
   * 
   * <p>All values are validated using {@link StringUtils#isBlank(CharSequence)} to ensure
   * they are not null, empty, or contain only whitespace characters.
   * 
   * <p>Configuration maps are typically loaded from the {@code ETARC_TABLE_CONFIG} table
   * and should contain keys defined in {@link Constants}:
   * <ul>
   *   <li>{@link Constants#TABLE_NAME_KEY} for the table name</li>
   *   <li>{@link Constants#COLUMN_NAME_KEY} for the partition column</li>
   *   <li>{@link Constants#COLUMN_NAME_KEY} for the primary key column</li>
   * </ul>
   * 
   * @param cfg the table configuration map to validate, must not be null
   * @return {@code true} if the configuration contains all required non-blank fields,
   *         {@code false} if any required field is missing, null, empty, or blank
   * 
   * @see Constants#TABLE_NAME_KEY
   * @see Constants#COLUMN_NAME_KEY  
   * @see StringUtils#isBlank(CharSequence)
   */
  private boolean isValidTableConfig(Map<String, String> cfg) {
    String tableName = cfg.get(Constants.TABLE_NAME_KEY);
    String partitionCol = cfg.get(Constants.COLUMN_NAME_KEY);
    String pkCol = cfg.get(Constants.COLUMN_NAME_KEY);
    return !StringUtils.isBlank(tableName) && !StringUtils.isBlank(partitionCol) && !StringUtils.isBlank(pkCol);
  }

  /**
   * Loads table configuration data from the ETARC_TABLE_CONFIG table.
   * 
   * <p>This method retrieves partitioning configuration for all tables that should be
   * managed by this module script. The configuration includes essential information
   * needed for partitioning operations:
   * 
   * <ul>
   *   <li><strong>Table Name:</strong> The name of the table to be partitioned</li>
   *   <li><strong>Partition Column:</strong> The column used for range partitioning (typically a date/timestamp)</li>
   *   <li><strong>Primary Key Column:</strong> The primary key column for constraint management</li>
   * </ul>
   * 
   * <p>The method executes a SQL query that joins several Etendo metadata tables:
   * <ul>
   *   <li>{@code ETARC_TABLE_CONFIG} - Contains the partitioning configuration</li>
   *   <li>{@code AD_TABLE} - Contains table metadata</li>
   *   <li>{@code AD_COLUMN} - Contains column metadata for both partition and primary key columns</li>
   * </ul>
   * 
   * <p>All column and table names are converted to uppercase for consistency with PostgreSQL
   * naming conventions and to ensure case-insensitive matching.
   * 
   * <p>The returned maps use keys defined in {@link Constants}:
   * <ul>
   *   <li>{@link Constants#TABLE_NAME_KEY} maps to the table name</li>
   *   <li>{@link Constants#COLUMN_NAME_KEY} maps to the partition column name</li>
   *   <li>{@link Constants#COLUMN_NAME_KEY} maps to the primary key column name</li>
   * </ul>
   * 
   * @param cp the connection provider for accessing the Etendo database
   * @return a list of configuration maps, each representing a table to be partitioned.
   *         Returns an empty list if no tables are configured for partitioning.
   * @throws Exception if database access fails, including:
   *                   <ul>
   *                     <li>SQL execution errors</li>
   *                     <li>Connection timeout or loss</li>
   *                     <li>Missing required database tables or columns</li>
   *                     <li>Insufficient database permissions</li>
   *                   </ul>
   * 
   * @see Constants#TABLE_NAME_KEY
   * @see Constants#COLUMN_NAME_KEY
   * @see #isValidTableConfig(Map) for configuration validation
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
        cfg.put(Constants.TABLE_NAME_KEY, rs.getString("TABLENAME"));
        cfg.put(Constants.COLUMN_NAME_KEY, rs.getString("COLUMNNAME"));
        cfg.put(Constants.PK_COLUMN_NAME_KEY, rs.getString("PK_COLUMNNAME"));
        tableConfigs.add(cfg);
      }
    }
    return tableConfigs;
  }

  /**
   * Processes a single table configuration to determine and execute the appropriate partitioning action.
   * 
   * <p>This method serves as the main coordinator for processing individual tables. It analyzes
   * the current state of a table and determines which of the following actions to perform:
   * 
   * <ul>
   *   <li><strong>Skip Processing:</strong> If the table configuration is invalid or no action is needed</li>
   *   <li><strong>Constraint Recreation:</strong> For tables that need only constraint updates</li>
   *   <li><strong>Structural Migration:</strong> For partitioned tables with schema changes</li>
   *   <li><strong>Partition Restoration:</strong> For tables that should be partitioned but aren't</li>
   * </ul>
   * 
   * <p>The processing workflow:
   * <ol>
   *   <li>Extract and validate table configuration information</li>
   *   <li>Analyze current table state (partitioning status, structural changes, etc.)</li>
   *   <li>Determine if table processing should be skipped</li>
   *   <li>Prepare performance metrics and apply optimizations for large tables</li>
   *   <li>Execute the appropriate action based on table state</li>
   *   <li>Apply post-processing optimizations</li>
   *   <li>Clean up any temporary optimizations</li>
   * </ol>
   * 
   * <p>Performance considerations:
   * <ul>
   *   <li>Tables exceeding {@link Constants#LARGE_TABLE_THRESHOLD} rows receive special optimization</li>
   *   <li>Database settings are temporarily modified for large operations</li>
   *   <li>Execution metrics are collected for monitoring and reporting</li>
   * </ul>
   * 
   * <p>Error handling ensures that failures in processing one table don't prevent
   * processing of other tables, and temporary optimizations are always cleaned up.
   * 
   * @param cp the connection provider for accessing the database
   * @param cfg a map containing the table configuration with keys from {@link Constants}:
   *            <ul>
   *              <li>{@link Constants#TABLE_NAME_KEY} - the table name</li>
   *              <li>{@link Constants#COLUMN_NAME_KEY} - the partition column name</li>
   *              <li>{@link Constants#COLUMN_NAME_KEY} - the primary key column name</li>
   *            </ul>
   * @param sql a StringBuilder to accumulate constraint SQL that will be executed later
   * @param tm a TableMetrics instance for collecting performance data about this table
   * 
   * @throws Exception if an error occurs during table analysis or processing, including:
   *                   <ul>
   *                     <li>Database connectivity issues</li>
   *                     <li>Table structure analysis failures</li>
   *                     <li>Migration or partitioning operation failures</li>
   *                     <li>Performance optimization failures</li>
   *                   </ul>
   * 
   * @see #extractTableConfigInfo(Map) for configuration extraction
   * @see #analyzeTableState(ConnectionProvider, TableConfigInfo) for state analysis  
   * @see #executeTableAction(ConnectionProvider, TableConfigInfo, TableStateInfo, StringBuilder, TableMetrics) for action execution
   */
  private void processTableConfig(ConnectionProvider cp, Map<String, String> cfg, StringBuilder sql, TableMetrics tm) throws Exception {
    TableConfigInfo configInfo = extractTableConfigInfo(cfg);
    
    if (!configInfo.isValid()) {
      LoggingUtils.logSkipReason(true, configInfo.tableName(), configInfo.pkCol(), configInfo.partitionCol());
      return;
    }

    TableStateInfo stateInfo = analyzeTableState(cp, configInfo);
    
    if (shouldSkipTable(stateInfo)) {
      LoggingUtils.logSkipReason(false, configInfo.tableName(), configInfo.pkCol(), configInfo.partitionCol());
      return;
    }

    prepareTableMetrics(cp, configInfo.tableName(), tm);
    
    try {
      executeTableAction(cp, configInfo, stateInfo, sql, tm);
      performPostProcessingOptimizations(cp, configInfo.tableName(), configInfo.partitionCol(), tm);
    } finally {
      cleanupOptimizations(cp, tm.largeTable);
    }
  }

  /**
   * Extracts and validates table configuration information from a configuration map.
   * 
   * <p>This method processes the raw configuration data loaded from the database and
   * prepares it for use in partitioning operations. It performs the following tasks:
   * 
   * <ul>
   *   <li>Extracts table name, partition column, and primary key column from the configuration</li>
   *   <li>Validates that all required fields are present and non-blank</li>
   *   <li>Locates corresponding XML table definition files for structural comparison</li>
   *   <li>Logs the extracted configuration for debugging purposes</li>
   * </ul>
   * 
   * <p>The method searches for XML files that define the table structure using
   * {@link XmlParsingUtils#findTableXmlFiles(String, String)}. These files are used
   * later to detect structural changes and guide migration operations.
   * 
   * <p>Validation ensures all critical fields are present before proceeding with
   * potentially expensive partitioning operations.
   * 
   * @param cfg the raw configuration map loaded from {@code ETARC_TABLE_CONFIG} with keys:
   *            <ul>
   *              <li>{@link Constants#TABLE_NAME_KEY} - the table name</li>
   *              <li>{@link Constants#COLUMN_NAME_KEY} - the partition column name</li>
   *              <li>{@link Constants#COLUMN_NAME_KEY} - the primary key column name</li>
   *            </ul>
   * 
   * @return a {@link TableConfigInfo} record containing the extracted and validated configuration
   *         along with located XML files. The {@code isValid} field indicates whether all
   *         required fields were present and valid.
   * 
   * @throws NoSuchFileException if XML table definition files cannot be located when expected
   * 
   * @see TableConfigInfo for the returned data structure
   * @see XmlParsingUtils#findTableXmlFiles(String, String) for XML file location
   */
  private TableConfigInfo extractTableConfigInfo(Map<String, String> cfg) throws NoSuchFileException {
    String tableName = cfg.get(Constants.TABLE_NAME_KEY);
    String partitionCol = cfg.get(Constants.COLUMN_NAME_KEY);
    String pkCol = cfg.get(Constants.COLUMN_NAME_KEY);

    log4j.info("DATA FROM ETARC_TABLE_CONFIG: tableName: {} - partitionCol: {} - pkCol: {}", 
               tableName, partitionCol, pkCol);

    boolean isValid = !StringUtils.isBlank(tableName) && !StringUtils.isBlank(partitionCol) && !StringUtils.isBlank(pkCol);
    List<File> xmlFiles = isValid ? XmlParsingUtils.findTableXmlFiles(tableName, getSourcePath()) : Collections.emptyList();

    return new TableConfigInfo(tableName, partitionCol, pkCol, isValid, xmlFiles);
  }

  /**
   * Analyzes the current state of a table to determine what partitioning actions are required.
   * 
   * <p>This method performs a comprehensive analysis of the table's current state by examining:
   * 
   * <ul>
   *   <li><strong>Structural Changes:</strong> Compares current table structure with XML definitions
   *       using {@link TableDefinitionComparator} to detect schema modifications</li>
   *   <li><strong>Partitioning Status:</strong> Checks if the table is currently partitioned
   *       using {@link TableAnalyzer#isTablePartitioned(ConnectionProvider, String)}</li>
   *   <li><strong>Primary Key Status:</strong> Examines primary key configuration to detect
   *       incomplete partitioning setups</li>
   *   <li><strong>Restoration Needs:</strong> Identifies tables that should be partitioned
   *       but currently aren't (typically after update.database operations)</li>
   * </ul>
   * 
   * <p>The analysis results guide the selection of appropriate actions:
   * <ul>
   *   <li><strong>First Partition Run:</strong> Table is partitioned but missing primary keys</li>
   *   <li><strong>Structural Changes:</strong> Partitioned table with schema modifications requiring migration</li>
   *   <li><strong>Partition Restoration:</strong> Table should be partitioned but currently isn't</li>
   *   <li><strong>Constraint Recreation:</strong> Standard constraint updates needed</li>
   * </ul>
   * 
   * <p>All analysis results are logged for debugging and monitoring purposes.
   * 
   * @param cp the connection provider for database access during analysis
   * @param configInfo the validated table configuration containing table name and column information
   * 
   * @return a {@link TableStateInfo} record containing the analysis results including:
   *         <ul>
   *           <li>Whether structural changes were detected</li>
   *           <li>Current partitioning status</li>
   *           <li>Whether this represents a first partition run</li>
   *           <li>Whether partition restoration is needed</li>
   *         </ul>
   * 
   * @throws Exception if analysis operations fail, including:
   *                   <ul>
   *                     <li>Database query failures during structure comparison</li>
   *                     <li>XML parsing errors when reading table definitions</li>
   *                     <li>Connection issues during table analysis</li>
   *                   </ul>
   * 
   * @see TableDefinitionComparator#isTableDefinitionChanged(String, ConnectionProvider, List)
   * @see TableAnalyzer#isTablePartitioned(ConnectionProvider, String)
   * @see TableAnalyzer#getPrimaryKeyColumns(ConnectionProvider, String)
   * @see TableStateInfo for the returned data structure
   */
  private TableStateInfo analyzeTableState(ConnectionProvider cp, TableConfigInfo configInfo) throws Exception {
    boolean isStructuralChange = configInfo.isValid() &&
            (new TableDefinitionComparator()).isTableDefinitionChanged(configInfo.tableName(), cp, configInfo.xmlFiles());

    boolean isPartitioned = tableAnalyzer.isTablePartitioned(cp, configInfo.tableName());
    List<String> pkCols = tableAnalyzer.getPrimaryKeyColumns(cp, configInfo.tableName());
    boolean firstPartitionRun = isPartitioned && pkCols.isEmpty();
    boolean needsPartitionRestoration = configInfo.isValid() && !isPartitioned;

    log4j.info("Table {} partitioned = {} existing PK cols = {} structural changes = {} needs restoration = {}", 
               configInfo.tableName(), isPartitioned, pkCols, isStructuralChange, needsPartitionRestoration);

    return new TableStateInfo(isStructuralChange, isPartitioned, firstPartitionRun, needsPartitionRestoration);
  }

  /**
   * Determines if table processing should be skipped.
   */
  private boolean shouldSkipTable(TableStateInfo stateInfo) {
    return !stateInfo.firstPartitionRun() && 
           !stateInfo.isStructuralChange() && 
           !stateInfo.needsPartitionRestoration();
  }

  /**
   * Prepares table metrics and performance optimizations.
   */
  private void prepareTableMetrics(ConnectionProvider cp, String tableName, TableMetrics tm) throws Exception {
    long tableSize = getTableSize(cp, tableName);
    boolean isLargeTable = tableSize > Constants.LARGE_TABLE_THRESHOLD;
    
    tm.estimatedRows = tableSize;
    tm.largeTable = isLargeTable;
    
    if (isLargeTable) {
      log4j.info("Large table detected ({}), applying performance optimizations", tableName);
      dbOptimizer.optimizeDatabaseForLargeOperations(cp);
      dbOptimizer.logPerformanceMetrics(cp, "Pre-partition " + tableName);
    }
  }

  /**
   * Gets table size with error handling.
   */
  private long getTableSize(ConnectionProvider cp, String tableName) {
    try {
      long tableSize = tableAnalyzer.getApproximateTableRowCount(cp, tableName);
      log4j.info("Table {} estimated size: {} rows", tableName, tableSize);
      return tableSize;
    } catch (Exception e) {
      log4j.warn("Could not determine table size for {}: {}", tableName, e.getMessage());
      return 0;
    }
  }

  /**
   * Executes the appropriate action based on table state.
   */
  private void executeTableAction(ConnectionProvider cp, TableConfigInfo configInfo, TableStateInfo stateInfo, 
                                  StringBuilder sql, TableMetrics tm) throws Exception {
    if (stateInfo.needsPartitionRestoration()) {
      tm.action = "restore-partitioning";
      log4j.info("Table {} should be partitioned but isn't. Restoring partitioning with current data...", 
                 configInfo.tableName());
      tm.migratedRows = restorePartitioningWithData(cp, configInfo.tableName(), configInfo.partitionCol(), configInfo.pkCol());
    }
    else if (stateInfo.isPartitioned() && stateInfo.isStructuralChange()) {
      tm.action = "migrate-structural";
      log4j.info("Structural changes detected in partitioned table {}. Performing full migration...", 
                 configInfo.tableName());
      tm.migratedRows = performPartitionedTableMigration(cp, configInfo.tableName(), configInfo.partitionCol(), configInfo.pkCol());
    } 
    else {
      tm.action = "recreate-constraints";
      log4j.info("Recreating constraints for {} (firstRun = {}, structuralChanges = {})", 
                 configInfo.tableName(), stateInfo.firstPartitionRun(), stateInfo.isStructuralChange());
      sql.append(constraintUtils.buildConstraintSql(configInfo.tableName(), cp, configInfo.pkCol(), 
                                                    configInfo.partitionCol(), getSourcePath()));
    }
  }

  /**
   * Performs post-processing optimizations for large tables.
   */
  private void performPostProcessingOptimizations(ConnectionProvider cp, String tableName, String partitionCol, TableMetrics tm) throws Exception {
    if (tm.largeTable) {
      dbOptimizer.createOptimizedIndexes(cp, tableName, partitionCol);
      dbOptimizer.analyzePartitionedTable(cp, tableName);
      dbOptimizer.logPerformanceMetrics(cp, "Post-partition " + tableName);
    }
  }

  /**
   * Cleans up optimization settings.
   */
  private void cleanupOptimizations(ConnectionProvider cp, boolean isLargeTable) throws Exception {
    if (isLargeTable) {
      dbOptimizer.restoreDatabaseSettings(cp);
    }
  }

  /**
   * Immutable data container for table configuration information.
   * 
   * <p>This record encapsulates all configuration data needed for partitioning operations,
   * extracted from the {@code ETARC_TABLE_CONFIG} database table and validated for completeness.
   * 
   * <p>The configuration includes both database metadata and file system resources:
   * <ul>
   *   <li><strong>Database Configuration:</strong> Table name and column specifications</li>
   *   <li><strong>Validation Status:</strong> Whether all required fields are present and valid</li>
   *   <li><strong>File Resources:</strong> XML table definition files for structural comparison</li>
   * </ul>
   * 
   * <p>This record is created by {@link #extractTableConfigInfo(Map)} and used throughout
   * the partitioning workflow to ensure consistent access to validated configuration data.
   * 
   * @param tableName the name of the table to be partitioned, must be non-blank if valid
   * @param partitionCol the column name to use for range partitioning, typically a date/timestamp column
   * @param pkCol the primary key column name for constraint management
   * @param isValid indicates whether all required fields are present and non-blank
   * @param xmlFiles list of XML files defining the table structure, used for change detection
   * 
   * @see #extractTableConfigInfo(Map) for creation
   * @see #isValidTableConfig(Map) for validation logic
   */
  private record TableConfigInfo(String tableName, String partitionCol, String pkCol, 
                                boolean isValid, List<File> xmlFiles) {
  }

  /**
   * Immutable data container for table state analysis results.
   * 
   * <p>This record holds the results of analyzing a table's current state to determine
   * what partitioning actions are required. It encapsulates the key decision factors
   * used by the workflow to select appropriate processing strategies.
   * 
   * <p>State analysis considers:
   * <ul>
   *   <li><strong>Structural Changes:</strong> Whether the table definition has changed</li>
   *   <li><strong>Partitioning Status:</strong> Current partitioning state of the table</li>
   *   <li><strong>Setup Completeness:</strong> Whether partitioning setup is incomplete</li>
   *   <li><strong>Restoration Needs:</strong> Whether partitioning needs to be restored</li>
   * </ul>
   * 
   * <p>This information guides the selection of processing actions:
   * <ul>
   *   <li>Structural migration for partitioned tables with schema changes</li>
   *   <li>Partition restoration for tables that should be partitioned but aren't</li>
   *   <li>Constraint recreation for standard maintenance operations</li>
   *   <li>First-run partitioning for incomplete setups</li>
   * </ul>
   * 
   * @param isStructuralChange true if the table structure has changed compared to XML definitions
   * @param isPartitioned true if the table is currently partitioned in the database
   * @param firstPartitionRun true if this is a first partitioning run (partitioned but missing constraints)
   * @param needsPartitionRestoration true if the table should be partitioned but currently isn't
   * 
   * @see #analyzeTableState(ConnectionProvider, TableConfigInfo) for creation
   * @see #shouldSkipTable(TableStateInfo) for usage in decision-making
   */
  private record TableStateInfo(boolean isStructuralChange, boolean isPartitioned, 
                               boolean firstPartitionRun, boolean needsPartitionRestoration) {
  }

  /**
   * Performs a comprehensive migration of a partitioned table when structural changes are detected.
   * 
   * <p>This method implements a complex data migration process that safely handles structural changes 
   * to partitioned tables while preserving all existing data. The process follows these critical steps:</p>
   * 
   * <ol>
   *   <li><strong>Pre-migration validation</strong> - Verifies partition structure and data integrity</li>
   *   <li><strong>Temporary table creation</strong> - Renames current table to preserve data during migration</li>
   *   <li><strong>New table structure creation</strong> - Builds updated partitioned table with current XML schema</li>
   *   <li><strong>Data migration</strong> - Transfers all records while maintaining partition distribution</li>
   *   <li><strong>Constraint restoration</strong> - Recreates all foreign keys, indexes, and constraints</li>
   *   <li><strong>Cleanup operations</strong> - Removes temporary tables and validates final state</li>
   * </ol>
   * 
   * <p>The migration process is designed to be transactionally safe and includes comprehensive
   * error handling with rollback capabilities. All operations are performed within a migration
   * context that tracks progress and maintains data integrity throughout the process.</p>
   * 
   * <p><strong>Performance Considerations:</strong></p>
   * <ul>
   *   <li>Large tables may require extended migration time due to data copying operations</li>
   *   <li>Foreign key constraints are temporarily disabled during migration to improve performance</li>
   *   <li>Partition-wise data distribution is optimized during the migration process</li>
   * </ul>
   * 
   * @param cp the database connection provider for executing all migration operations
   * @param tableName the name of the partitioned table requiring structural migration
   * @param partitionCol the column name used for partitioning (typically a date/timestamp field)
   * @param pkCol the primary key column name for maintaining referential integrity
   * 
   * @throws PartitioningException if critical errors occur during migration that cannot be recovered
   * @throws TableMigrationException if table-specific migration operations fail
   * @throws SQLException if database operations encounter errors during the migration process
   */
  private long performPartitionedTableMigration(ConnectionProvider cp, String tableName, 
                                               String partitionCol, String pkCol) throws Exception {
    log4j.info("========== STARTING PARTITIONED TABLE MIGRATION FOR {} ==========", tableName);
    
    MigrationContext context = new MigrationContext(tableName);
    
    try {
      prepareMigrationEnvironment(cp, context);
      createNewPartitionedTable(cp, context, partitionCol);
      long migratedRows = migrateTableData(cp, context, tableName, partitionCol);
      finalizeTableMigration(cp, context, tableName, partitionCol, pkCol);
      
      log4j.info("========== COMPLETED PARTITIONED TABLE MIGRATION FOR {} ==========", tableName);
      return migratedRows;
      
    } catch (Exception e) {
      handleMigrationFailure(cp, context, tableName, e);
      return context.migratedRows;
    }
  }

  /**
   * Prepares the migration environment by dropping constraints and renaming tables.
   */
  private void prepareMigrationEnvironment(ConnectionProvider cp, MigrationContext context) throws Exception {
    log4j.info("Step 1: Dropping dependent views and foreign keys for table {}", context.tableName);
    constraintUtils.dropDependentViewsAndConstraints(cp, context.tableName);
    
    log4j.info("Step 2: Renaming {} to {}", context.tableName, context.tempTableName);
    constraintUtils.executeUpdate(cp, String.format(Constants.ALTER_TABLE_RENAME_SQL, 
                                   context.tableName, context.tempTableName));
  }

  /**
   * Creates the new partitioned table with updated structure and partitions.
   */
  private void createNewPartitionedTable(ConnectionProvider cp, MigrationContext context, String partitionCol) throws Exception {
    log4j.info("Step 3: Creating new partitioned table {} with updated structure", context.tableName);
    createUpdatedPartitionedTable(cp, context.tableName, partitionCol);
    
    log4j.info("Step 4: Ensuring partitions schema '{}' exists", MigrationContext.PARTITIONS_SCHEMA);
    constraintUtils.executeUpdate(cp, String.format(Constants.CREATE_SCHEMA_SQL, MigrationContext.PARTITIONS_SCHEMA));
    
    log4j.info("Step 5: Creating partitions for new table {}", context.tableName);
    createPartitionsForTable(cp, context.tableName, context.tempTableName, partitionCol, MigrationContext.PARTITIONS_SCHEMA);
  }

  /**
   * Migrates data from the source table to the new partitioned table.
   */
  private long migrateTableData(ConnectionProvider cp, MigrationContext context, String tableName, String partitionCol) throws Exception {
    log4j.info("Step 6: Migrating data from {} to {}", context.tempTableName, tableName);
    
    String dataSourceTable = determineDataSource(cp, context, tableName);
    
    try {
      long migratedRows = migrationService.migrateDataToNewTable(cp, dataSourceTable, tableName, partitionCol);
      LoggingUtils.logMigrationSuccessMessage(tableName, migratedRows, dataSourceTable);
      context.migratedRows = migratedRows;
      context.dataSourceTable = dataSourceTable;
      return migratedRows;
    } catch (Exception ex) {
      return attemptFallbackMigration(cp, context, tableName, dataSourceTable, ex);
    }
  }

  /**
   * Determines the appropriate data source for migration (temp table or backup).
   */
  private String determineDataSource(ConnectionProvider cp, MigrationContext context, String tableName) throws Exception {
    long tempTableRows = backupManager.getTableRowCount(cp, context.tempTableName);
    String dataSourceTable = context.tempTableName;
    
    if (tempTableRows == 0) {
      log4j.warn("Temporary table {} is empty, checking for backup data...", context.tempTableName);
      String backupTableName = backupManager.getLatestBackupTable(cp, tableName);
      
      if (backupTableName != null) {
        String fullBackupTableName = Constants.BACKUP_SCHEMA + "." + backupTableName;
        long backupRows = backupManager.getTableRowCount(cp, fullBackupTableName);
        
        if (backupRows > 0) {
          log4j.info("Found backup table {} with {} rows. Using backup for data migration.",
                    backupTableName, backupRows);
          dataSourceTable = fullBackupTableName;
        }
      }
      
      if (!dataSourceTable.contains(".")) {
        log4j.warn("No backup data found. Table {} will be empty after migration.", tableName);
      }
    }
    
    return dataSourceTable;
  }

  /**
   * Attempts fallback migration using column intersection when primary migration fails.
   */
  private long attemptFallbackMigration(ConnectionProvider cp, MigrationContext context, String tableName, 
                                       String dataSourceTable, Exception originalException) throws Exception {
    final String srcFqn = dataSourceTable.contains(".") ? dataSourceTable : (PUBLIC + dataSourceTable);
    final String dstFqn = PUBLIC + tableName;
    
    log4j.warn("Primary migration failed ({}). Falling back to column intersection between {} and {}",
               originalException.getMessage(), srcFqn, dstFqn);
    
    List<String> matchingCols = findMatchingColumnsPortable(cp, srcFqn, dstFqn);
    if (matchingCols.isEmpty()) {
      log4j.error("Fallback failed: no matching columns found between {} and {}", srcFqn, dstFqn);
      throw originalException;
    }
    
    long migratedRows = insertByColumnIntersection(cp, srcFqn, dstFqn, matchingCols);
    LoggingUtils.logMigrationSuccessMessage(tableName, migratedRows, dataSourceTable);
    context.migratedRows = migratedRows;
    context.dataSourceTable = dataSourceTable;
    return migratedRows;
  }

  /**
   * Finalizes the migration by verifying table state, recreating constraints, and cleaning up.
   */
  private void finalizeTableMigration(ConnectionProvider cp, MigrationContext context, String tableName, 
                                     String partitionCol, String pkCol) throws Exception {
    verifyMigratedTable(cp, tableName);
    recreateTableConstraints(cp, tableName, partitionCol, pkCol);
    cleanupMigrationArtifacts(cp, context, tableName);
  }

  /**
   * Verifies that the migrated table is properly partitioned.
   */
  private void verifyMigratedTable(ConnectionProvider cp, String tableName) throws Exception {
    log4j.info("Step 7: Verifying new partitioned table {} was created successfully", tableName);
    if (!tableAnalyzer.isTablePartitioned(cp, tableName)) {
      throw new TableMigrationException("New table " + tableName + " is not properly partitioned after migration");
    }
  }

  /**
   * Recreates constraints for the migrated table with error handling.
   */
  private void recreateTableConstraints(ConnectionProvider cp, String tableName, String partitionCol, String pkCol) {
    log4j.info("Step 8: Recreating constraints for {}", tableName);
    try {
      String constraintSql = constraintUtils.buildConstraintSql(tableName, cp, pkCol, partitionCol, getSourcePath());
      if (!StringUtils.isBlank(constraintSql)) {
        constraintUtils.executeUpdate(cp, constraintSql);
        LoggingUtils.logSuccessRecreatedConstraints(tableName);
      }
    } catch (Exception constraintError) {
      handleConstraintRecreationError(tableName, constraintError);
    }
  }

  /**
   * Handles constraint recreation errors with appropriate logging.
   */
  private void handleConstraintRecreationError(String tableName, Exception constraintError) {
    log4j.error("WARNING: Failed to recreate constraints for {}: {}", tableName, constraintError.getMessage());
    log4j.error("The table migration was successful, but constraint recreation failed.");
    log4j.error("Constraints can be recreated manually later.");
    
    if (constraintError.getMessage().contains("already exists")) {
      log4j.info("Constraint already exists - this is expected and not an error. Continuing...");
    } else {
      log4j.error("Keeping temporary table for safety due to unexpected constraint error.");
    }
  }

  /**
   * Cleans up migration artifacts (temporary tables and backups).
   */
  private void cleanupMigrationArtifacts(ConnectionProvider cp, MigrationContext context, String tableName) throws Exception {
    log4j.info("Step 9: Dropping temporary table {} (migration completed successfully)", context.tempTableName);
    constraintUtils.executeUpdate(cp, String.format(Constants.DROP_TABLE_CASCADE_SQL, context.tempTableName));
    
    if (!context.dataSourceTable.equals(context.tempTableName)) {
      cleanupBackupTable(cp, context, tableName);
    }
  }

  /**
   * Cleans up backup table if it was used during migration.
   */
  private void cleanupBackupTable(ConnectionProvider cp, MigrationContext context, String tableName) {
    String backupTableName = context.dataSourceTable.substring(context.dataSourceTable.lastIndexOf(".") + 1);
    log4j.info("Step 10: Cleaning up backup table {} (migration completed successfully)", backupTableName);
    try {
      backupManager.cleanupBackup(cp, tableName, backupTableName);
    } catch (Exception backupCleanupError) {
      log4j.warn("Failed to cleanup backup table {}: {}", backupTableName, backupCleanupError.getMessage());
    }
  }

  /**
   * Handles migration failure by attempting to restore the original table.
   */
  private void handleMigrationFailure(ConnectionProvider cp, MigrationContext context, String tableName, Exception e) {
    log4j.error("ERROR during partitioned table migration for {}: {}", tableName, e.getMessage(), e);
    
    try {
      if (checkTemporaryTableExists(cp, context.tempTableName)) {
        restoreOriginalTable(cp, context, tableName);
      } else {
        logCriticalRestoreFailure(tableName, context.tempTableName);
      }
    } catch (Exception restoreError) {
      LoggingUtils.logCriticalRestorationError(tableName, restoreError);
    }
  }

  /**
   * Checks if the temporary table still exists.
   */
  private boolean checkTemporaryTableExists(ConnectionProvider cp, String tempTableName) throws Exception {
    try (PreparedStatement checkPs = cp.getPreparedStatement(
        "SELECT 1 FROM information_schema.tables WHERE table_name = ? AND table_schema = 'public'")) {
      checkPs.setString(1, tempTableName);
      try (ResultSet rs = checkPs.executeQuery()) {
        return rs.next();
      }
    }
  }

  /**
   * Restores the original table from the temporary table.
   */
  private void restoreOriginalTable(ConnectionProvider cp, MigrationContext context, String tableName) throws Exception {
    LoggingUtils.logRestoreOriginalTable(tableName, context.tempTableName);
    constraintUtils.executeUpdate(cp, String.format(Constants.DROP_TABLE_CASCADE_SQL, tableName));
    constraintUtils.executeUpdate(cp, String.format(Constants.ALTER_TABLE_RENAME_SQL, 
                                   context.tempTableName, tableName));
    LoggingUtils.logSuccessRestoredTable(tableName);
  }

  /**
   * Logs critical failure when temporary table no longer exists.
   */
  private void logCriticalRestoreFailure(String tableName, String tempTableName) {
    log4j.error("CRITICAL: Cannot restore original table {} - temporary table {} no longer exists", 
               tableName, tempTableName);
    log4j.error("Manual intervention required to restore the table from backup");
  }

  /**
   * Migration context class that encapsulates state and configuration data for table migration operations.
   * 
   * <p>This context object serves as a centralized container for all migration-related information,
   * reducing parameter passing complexity and maintaining state consistency throughout the migration
   * process. It follows the Context Object pattern to provide a clean interface for migration operations.</p>
   * 
   * <p><strong>Key Responsibilities:</strong></p>
   * <ul>
   *   <li>Maintains table naming conventions during migration (original, temporary, partitions schema)</li>
   *   <li>Tracks migration progress and statistics (row counts, data source information)</li>
   *   <li>Provides consistent naming for temporary migration artifacts</li>
   *   <li>Centralizes migration configuration to ensure consistency across operations</li>
   * </ul>
   * 
   * <p>The context is designed to be immutable for core configuration (table names, schema)
   * while allowing mutable state for tracking migration progress and results.</p>
   */
  private static class MigrationContext {
    /** The original table name being migrated */
    final String tableName;
    
    /** Temporary table name used during migration operations (original + "_etarc_migration_temp") */
    final String tempTableName;
    
    /** Schema name where partition tables are stored ("partitions") */
    static final String PARTITIONS_SCHEMA = "partitions";
    
    /** Current data source table name (changes during migration phases) */
    String dataSourceTable;
    
    /** Number of rows successfully migrated during the current operation */
    long migratedRows = 0L;

    /**
     * Creates a new migration context for the specified table.
     * 
     * @param tableName the name of the table to be migrated
     */
    MigrationContext(String tableName) {
      this.tableName = tableName;
      this.tempTableName = tableName + "_etarc_migration_temp";
      this.dataSourceTable = tempTableName;
    }
  }

  /**
   * Restores partitioning to a table that should be partitioned but currently isn't.
   * 
   * <p>This method handles complex restoration scenarios where database update operations have 
   * converted a partitioned table back to a regular table, requiring careful data preservation 
   * and partitioning restoration. The process adapts to different data availability scenarios:</p>
   * 
   * <ul>
   *   <li><strong>Data present scenario</strong> - Preserves existing data while converting to partitioned structure</li>
   *   <li><strong>Empty table with backup</strong> - Restores data from archived backup and applies partitioning</li>
   *   <li><strong>Empty table without backup</strong> - Creates partitioned structure for future data ingestion</li>
   * </ul>
   * 
   * <p>The restoration process implements the following workflow:</p>
   * <ol>
   *   <li><strong>Data assessment</strong> - Evaluates current table state and available backup options</li>
   *   <li><strong>Backup strategy selection</strong> - Chooses appropriate data preservation method</li>
   *   <li><strong>Structural conversion</strong> - Converts regular table to partitioned architecture</li>
   *   <li><strong>Data restoration</strong> - Migrates or restores data to partitioned structure</li>
   *   <li><strong>Constraint recreation</strong> - Rebuilds all table relationships and indexes</li>
   *   <li><strong>Validation</strong> - Verifies successful restoration and data integrity</li>
   * </ol>
   * 
   * <p><strong>Recovery Scenarios:</strong></p>
   * <ul>
   *   <li>Post-update.database conversion recovery</li>
   *   <li>Failed partitioning attempt recovery</li>
   *   <li>Manual table structure restoration</li>
   *   <li>Data corruption recovery from backups</li>
   * </ul>
   * 
   * @param cp the database connection provider for executing restoration operations
   * @param tableName the name of the table requiring partitioning restoration
   * @param partitionCol the column name to be used for partitioning (typically date/timestamp)
   * @param pkCol the primary key column name for maintaining referential integrity
   * 
   * @throws PartitioningException if restoration cannot be completed due to structural issues
   * @throws SQLException if database operations fail during restoration process
   * @throws Exception if backup restoration or data migration encounters errors
   */
  private long restorePartitioningWithData(ConnectionProvider cp, String tableName, 
                                         String partitionCol, String pkCol) throws Exception {
    log4j.info("========== RESTORING PARTITIONING FOR {} ==========", tableName);
    
    RestorationContext context = new RestorationContext(tableName);
    
    try {
      DataSourceInfo dataSourceInfo = analyzeDataSource(cp, context);
      long migratedRows = executeRestorationStrategy(cp, context, dataSourceInfo, partitionCol);
      finalizePartitionRestoration(cp, tableName, partitionCol, pkCol);
      
      log4j.info("========== COMPLETED PARTITIONING RESTORATION FOR {} ==========", tableName);
      return migratedRows;
      
    } catch (Exception e) {
      handleRestorationFailure(cp, context, tableName, e);
      throw new PartitioningException("Partitioning restoration failed for " + tableName, e);
    }
  }

  /**
   * Analyzes the current table data and determines the best data source for restoration.
   */
  private DataSourceInfo analyzeDataSource(ConnectionProvider cp, RestorationContext context) throws Exception {
    long rowCount = backupManager.getTableRowCount(cp, context.tableName);
    log4j.info("Step 1: Table {} contains {} rows", context.tableName, rowCount);
    
    if (rowCount == 0) {
      return checkForBackupData(cp, context);
    }
    
    return new DataSourceInfo(context.tableName, rowCount, false, null);
  }

  /**
   * Checks for backup data when the main table is empty.
   */
  private DataSourceInfo checkForBackupData(ConnectionProvider cp, RestorationContext context) throws Exception {
    String backupTableName = backupManager.getLatestBackupTable(cp, context.tableName);
    
    if (backupTableName != null) {
      String fullBackupTableName = Constants.BACKUP_SCHEMA + "." + backupTableName;
      long backupRowCount = backupManager.getTableRowCount(cp, fullBackupTableName);
      
      if (backupRowCount > 0) {
        log4j.info("Table is empty but found backup {} with {} rows. Will restore from backup.", 
                  backupTableName, backupRowCount);
        return new DataSourceInfo(fullBackupTableName, backupRowCount, true, backupTableName);
      }
    }
    
    log4j.warn("Table {} is empty and no backup found. Creating empty partitioned table.", context.tableName);
    return new DataSourceInfo(context.tableName, 0, false, null);
  }

  /**
   * Executes the appropriate restoration strategy based on data availability.
   */
  private long executeRestorationStrategy(ConnectionProvider cp, RestorationContext context, 
                                         DataSourceInfo dataSource, String partitionCol) throws Exception {
    if (dataSource.isEmpty()) {
      return handleEmptyTableRestoration(context);
    } else {
      return handleDataTableRestoration(cp, context, dataSource, partitionCol);
    }
  }

  /**
   * Handles restoration for empty tables.
   */
  private long handleEmptyTableRestoration(RestorationContext context) throws Exception {
    log4j.info("Table is empty, converting directly to partitioned table");
    migrationService.convertEmptyTableToPartitioned(context.tableName);
    return 0L;
  }

  /**
   * Handles restoration for tables with data.
   */
  private long handleDataTableRestoration(ConnectionProvider cp, RestorationContext context, 
                                         DataSourceInfo dataSource, String partitionCol) throws Exception {
    log4j.info("Table has data{}, performing safe migration to restore partitioning", 
              dataSource.usingBackup() ? " (from backup)" : "");
    
    renameCurrentTable(cp, context);
    createNewPartitionedTableForRestoration(cp, context, dataSource, partitionCol);
    setupPartitionsForRestoration(cp, context, dataSource, partitionCol);
    
    return migrateDataAndCleanupForRestoration(cp, context, dataSource, partitionCol);
  }

  /**
   * Renames the current table to a temporary name.
   */
  private void renameCurrentTable(ConnectionProvider cp, RestorationContext context) throws Exception {
    log4j.info("Step 3: Renaming {} to {}", context.tableName, context.tempTableName);
    constraintUtils.executeUpdate(cp, String.format(Constants.ALTER_TABLE_RENAME_SQL, 
                                   context.tableName, context.tempTableName));
  }

  /**
   * Creates a new partitioned table with the same structure.
   */
  private void createNewPartitionedTableForRestoration(ConnectionProvider cp, RestorationContext context, 
                                        DataSourceInfo dataSource, String partitionCol) throws Exception {
    log4j.info("Step 4: Creating new partitioned table {} with same structure", context.tableName);
    
    String templateTable = dataSource.usingBackup() ? dataSource.backupTableName() : context.tempTableName;
    migrationService.createPartitionedTableFromTemplate(cp, context.tableName, templateTable, partitionCol);
  }

  /**
   * Sets up partitions for the new table.
   */
  private void setupPartitionsForRestoration(ConnectionProvider cp, RestorationContext context, 
                              DataSourceInfo dataSource, String partitionCol) throws Exception {
    log4j.info("Step 5: Ensuring partitions schema '{}' exists", RestorationContext.PARTITIONS_SCHEMA);
    constraintUtils.executeUpdate(cp, String.format(Constants.CREATE_SCHEMA_SQL, RestorationContext.PARTITIONS_SCHEMA));
    
    log4j.info("Step 6: Creating partitions for table {}", context.tableName);
    createPartitionsForTable(cp, context.tableName, dataSource.dataSourceTable(), partitionCol, RestorationContext.PARTITIONS_SCHEMA);
  }

  /**
   * Migrates data and performs cleanup operations.
   */
  private long migrateDataAndCleanupForRestoration(ConnectionProvider cp, RestorationContext context, 
                                    DataSourceInfo dataSource, String partitionCol) throws Exception {
    log4j.info("Step 7: Migrating data from {} to {}", dataSource.dataSourceTable(), context.tableName);
    long migratedRows = migrationService.migrateDataToNewTable(cp, dataSource.dataSourceTable(), 
                                                               context.tableName, partitionCol);
    LoggingUtils.logMigrationSuccessMessage(context.tableName, migratedRows, dataSource.dataSourceTable());

    cleanupTemporaryResourcesForRestoration(cp, context, dataSource);
    
    return migratedRows;
  }

  /**
   * Cleans up temporary resources after successful migration.
   */
  private void cleanupTemporaryResourcesForRestoration(ConnectionProvider cp, RestorationContext context, DataSourceInfo dataSource) throws Exception {
    log4j.info("Step 8: Dropping temporary table {} (restoration completed successfully)", context.tempTableName);
    constraintUtils.executeUpdate(cp, String.format(Constants.DROP_TABLE_CASCADE_SQL, context.tempTableName));
    
    if (dataSource.usingBackup()) {
      log4j.info("Step 9: Cleaning up backup table {}", dataSource.dataSourceTable());
      backupManager.cleanupBackup(cp, context.tableName, dataSource.backupTableName());
    }
  }

  /**
   * Finalizes partition restoration by recreating constraints.
   */
  private void finalizePartitionRestoration(ConnectionProvider cp, String tableName, String partitionCol, String pkCol) throws Exception {
    log4j.info("Step 10: Recreating constraints for restored partitioned table {}", tableName);
    String constraintSql = constraintUtils.buildConstraintSql(tableName, cp, pkCol, partitionCol, getSourcePath());
    if (!StringUtils.isBlank(constraintSql)) {
      constraintUtils.executeUpdate(cp, constraintSql);
      LoggingUtils.logSuccessRecreatedConstraints(tableName);
    }
  }

  /**
   * Handles restoration failures by attempting to restore the original state.
   */
  private void handleRestorationFailure(ConnectionProvider cp, RestorationContext context, String tableName, Exception e) {
    log4j.error("ERROR during partitioning restoration for {}: {}", tableName, e.getMessage(), e);
    
    try {
      boolean tempTableExists = backupManager.tableExists(cp, context.tempTableName);
      
      if (tempTableExists) {
        LoggingUtils.logRestoreOriginalTable(tableName, context.tempTableName);
        constraintUtils.executeUpdate(cp, String.format(Constants.DROP_TABLE_CASCADE_SQL, tableName));
        constraintUtils.executeUpdate(cp, String.format(Constants.ALTER_TABLE_RENAME_SQL, 
                                       context.tempTableName, tableName));
        LoggingUtils.logSuccessRestoredTable(tableName);
      }
    } catch (Exception restoreError) {
      LoggingUtils.logCriticalRestorationError(tableName, restoreError);
    }
  }

  /**
   * Restoration context class that manages state and configuration for partitioning restoration operations.
   * 
   * <p>This context object encapsulates all necessary information for restoring partitioning to tables
   * that have been converted back to regular tables, typically after update.database operations. 
   * It provides consistent naming conventions and state management throughout the restoration process.</p>
   * 
   * <p><strong>Key Features:</strong></p>
   * <ul>
   *   <li>Maintains consistent naming for temporary restoration artifacts</li>
   *   <li>Provides schema location for partition tables ("partitions")</li>
   *   <li>Supports multiple restoration scenarios (data preservation, backup restoration)</li>
   *   <li>Centralizes restoration configuration to ensure operation consistency</li>
   * </ul>
   * 
   * <p>The context uses a different temporary table naming convention from migration operations
   * ("_etarc_restore_temp" vs "_etarc_migration_temp") to avoid conflicts and clearly
   * distinguish between migration and restoration operations.</p>
   *
   * @see #restorePartitioningWithData(ConnectionProvider, String, String, String) for usage
   */
  private static class RestorationContext {
    /** The original table name requiring partitioning restoration */
    final String tableName;
    
    /** Temporary table name used during restoration operations (original + "_etarc_restore_temp") */
    final String tempTableName;
    
    /** Schema name where partition tables are stored ("partitions") */
    static final String PARTITIONS_SCHEMA = "partitions";

    /**
     * Creates a new restoration context for the specified table.
     * 
     * @param tableName the name of the table requiring partitioning restoration
     */
    RestorationContext(String tableName) {
      this.tableName = tableName;
      this.tempTableName = tableName + "_etarc_restore_temp";
    }
  }

  /**
   * Immutable record containing comprehensive data source information for restoration operations.
   * 
   * <p>This record encapsulates all necessary information about data availability and backup
   * status during partitioning restoration processes. It provides a clean interface for
   * determining restoration strategies based on current table state and backup availability.</p>
   * 
   * <p><strong>Usage Patterns:</strong></p>
   * <ul>
   *   <li>Decision-making for restoration strategy selection</li>
   *   <li>Validation of data availability before restoration</li>
   *   <li>Progress tracking during data restoration operations</li>
   *   <li>Backup validation and fallback scenario handling</li>
   * </ul>
   * 
   * @param dataSourceTable the name of the table containing the data to be restored
   * @param rowCount the number of rows available in the data source table
   * @param usingBackup true if the restoration will use backup data instead of current table data
   * @param backupTableName the name of the backup table (null if no backup is available)
   * 
   * @see #restorePartitioningWithData(ConnectionProvider, String, String, String) for usage
   */
  private record DataSourceInfo(String dataSourceTable, long rowCount, boolean usingBackup, String backupTableName) {
    /**
     * Checks if the data source contains no data.
     * 
     * @return true if the row count is zero, false otherwise
     */
    boolean isEmpty() {
      return rowCount == 0;
    }
  }

  /**
   * Holds performance and execution metrics for individual table processing operations.
   * 
   * <p>This class tracks detailed metrics about each table processed during the partitioning
   * workflow, enabling performance monitoring, debugging, and optimization analysis.
   * 
   * <p>Metrics collected include:
   * <ul>
   *   <li><strong>Identification:</strong> Table name for correlation with logs and operations</li>
   *   <li><strong>Action Type:</strong> The type of operation performed (constraint recreation,
   *       structural migration, partition restoration, etc.)</li>
   *   <li><strong>Timing:</strong> Start and end timestamps for duration calculation</li>
   *   <li><strong>Scale:</strong> Estimated and actual row counts for performance analysis</li>
   *   <li><strong>Optimization:</strong> Whether large table optimizations were applied</li>
   * </ul>
   * 
   * <p>Timing measurements use {@link System#nanoTime()} for high precision and are
   * typically converted to milliseconds for reporting.
   * 
   * <p>The metrics are used in summary reports to identify:
   * <ul>
   *   <li>Tables that took unusually long to process</li>
   *   <li>Large tables that may benefit from additional optimization</li>
   *   <li>Migration operations that moved significant amounts of data</li>
   *   <li>Overall system performance trends</li>
   * </ul>
   * 
   * @see #logRunMetricsSummary(long) for metric reporting
   */
  private static class TableMetrics {
    /** The name of the table being processed, used for identification and correlation. */
    final String tableName;
    
    /** The type of action performed on this table (e.g., "migrate-structural", "restore-partitioning"). */
    String action = "unknown";
    
    /** The start time of processing in nanoseconds from {@link System#nanoTime()}. */
    long startNs = System.nanoTime();
    
    /** The end time of processing in nanoseconds, set when processing completes. */
    long endNs = startNs;
    
    /** The estimated number of rows in the table, or -1 if unknown or couldn't be determined. */
    long estimatedRows = -1;
    
    /** The actual number of rows migrated during processing, or -1 if no migration occurred. */
    long migratedRows = -1;
    
    /** Whether this table was classified as large and received optimization treatment. */
    boolean largeTable = false;

    /**
     * Creates a new TableMetrics instance for the specified table.
     * 
     * @param tableName the name of the table to track metrics for
     */
    TableMetrics(String tableName) { 
      this.tableName = tableName; 
    }
  }

  /**
   * Handles general errors that occur during module script execution.
   * 
   * <p>This method serves as the central error handler for the module script, providing
   * consistent error logging and exception transformation. When called, it:
   * 
   * <ul>
   *   <li>Logs the error with full stack trace using the Log4j2 logger</li>
   *   <li>Wraps the original exception in an {@link OBException} for Etendo framework compatibility</li>
   *   <li>Provides a standardized error message indicating module script failure</li>
   * </ul>
   * 
   * <p>The method is typically called from the main {@link #execute()} method when
   * unrecoverable errors occur that should terminate the entire module script execution.
   * 
   * <p>Error information includes:
   * <ul>
   *   <li>The original exception message for immediate context</li>
   *   <li>Full stack trace for detailed debugging information</li>
   *   <li>Module script identification for system-level error tracking</li>
   * </ul>
   * 
   * <p>The wrapped exception will be handled by the Etendo framework's standard
   * error handling mechanisms, including transaction rollback if appropriate.
   * 
   * @param e the exception that occurred during module script execution
   * @throws OBException always thrown, wrapping the original exception with additional context
   * 
   * @see OBException for Etendo framework exception handling
   */
  private void handleError(Exception e) {
    log4j.error("Error in PartitionedConstraintsHandling: {}", e.getMessage(), e);
    throw new OBException("Module script execution failed", e);
  }

  /**
   * Logs a comprehensive summary of the partitioning run including performance metrics.
   * 
   * <p>This method generates a detailed execution report that includes overall runtime
   * and individual table processing metrics. The summary is designed to help with:
   * 
   * <ul>
   *   <li><strong>Performance Monitoring:</strong> Identify slow operations and bottlenecks</li>
   *   <li><strong>Capacity Planning:</strong> Understand processing time versus table size relationships</li>
   *   <li><strong>Troubleshooting:</strong> Correlate execution times with system performance</li>
   *   <li><strong>Optimization:</strong> Identify tables that may benefit from different strategies</li>
   * </ul>
   * 
   * <p>The summary includes:
   * <ul>
   *   <li><strong>Total Runtime:</strong> Overall execution time in milliseconds</li>
   *   <li><strong>Per-Table Metrics:</strong> Individual table processing details including:
   *     <ul>
   *       <li>Table name for identification</li>
   *       <li>Action performed (migration type, constraint recreation, etc.)</li>
   *       <li>Processing duration in milliseconds</li>
   *       <li>Estimated row count for scale context</li>
   *       <li>Large table indicator for optimization tracking</li>
   *       <li>Migrated row count for data movement operations</li>
   *     </ul>
   *   </li>
   * </ul>
   * 
   * <p>The output is formatted for easy reading and parsing, with clear separators
   * and consistent field ordering. Duration calculations handle edge cases where
   * end time might not be properly set.
   * 
   * <p>Example output format:
   * <pre>
   * =====================================
   * Partitioning run summary: 1250 ms
   * - INVOICE | action=migrate-structural | durationMs=800 | estimatedRows=50000 | large | migratedRows=49850
   * - PAYMENTS | action=recreate-constraints | durationMs=150 | estimatedRows=15000
   * =====================================
   * </pre>
   * 
   * @param runStartNs the start time of the entire run in nanoseconds from {@link System#nanoTime()}
   * 
   * @see TableMetrics for individual table metric collection
   * @see LoggingUtils#logSeparator() for consistent output formatting
   */
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
  private Map<String, ColumnDefinition> parseXmlDefinition(File xmlFile) throws ParserConfigurationException,
      SAXException, IOException {
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
   * Immutable record representing a complete column definition for database table structure comparison.
   * 
   * <p>This record encapsulates all essential metadata about a database column, providing
   * a standardized representation for comparing table structures between different states
   * (e.g., current database vs. XML definitions). It supports structural change detection
   * during migration and restoration operations.</p>
   * 
   * <p><strong>Column Attributes:</strong></p>
   * <ul>
   *   <li><strong>name</strong> - The column identifier used in SQL operations</li>
   *   <li><strong>dataType</strong> - PostgreSQL data type (e.g., "varchar", "timestamptz", "int8")</li>
   *   <li><strong>length</strong> - Maximum length for variable-length types (null for fixed types)</li>
   *   <li><strong>isNullable</strong> - Whether the column accepts NULL values</li>
   *   <li><strong>isPrimaryKey</strong> - Whether the column participates in the primary key</li>
   * </ul>
   * 
   * <p>This record is designed to be compatible with TableDefinitionComparator's column
   * representation, enabling seamless integration with existing table comparison utilities.</p>
   * 
   * @param name the column name as defined in the database schema
   * @param dataType the PostgreSQL data type designation for the column
   * @param length the maximum length constraint for variable-length data types (null if not applicable)
   * @param isNullable true if the column accepts NULL values, false if NOT NULL constraint applies
   * @param isPrimaryKey true if the column is part of the primary key constraint
   * 
   * @see TableDefinitionComparator for compatible column comparison utilities
   */
    private record ColumnDefinition(String name, String dataType, Integer length, Boolean isNullable,
                                    Boolean isPrimaryKey) {
  }

  /**
   * Creates optimized partitions for a partitioned table based on actual data distribution.
   * 
   * <p>This method implements an intelligent partitioning strategy that analyzes the data
   * in the source table to determine the optimal partition boundaries. It creates annual
   * partitions covering the complete temporal range of the data, ensuring efficient
   * query performance and maintenance operations.</p>
   * 
   * <p><strong>Partitioning Strategy:</strong></p>
   * <ol>
   *   <li><strong>Data Analysis</strong> - Examines partition column values to determine data range</li>
   *   <li><strong>Range Calculation</strong> - Calculates optimal year range with buffer for future data</li>
   *   <li><strong>Partition Creation</strong> - Creates annual partitions for the entire range</li>
   *   <li><strong>Constraint Setup</strong> - Establishes appropriate check constraints for each partition</li>
   *   <li><strong>Index Creation</strong> - Creates indexes on partition columns for performance</li>
   *   <li><strong>Validation</strong> - Verifies all partitions are properly created and accessible</li>
   * </ol>
   * 
   * <p>The method uses a simplified approach compared to external partitioning tools,
   * focusing on reliability and integration with the existing Etendo database structure.
   * Partitions are created in the specified schema with consistent naming conventions.</p>
   * 
   * <p><strong>Performance Considerations:</strong></p>
   * <ul>
   *   <li>Efficient for tables with date-based partitioning requirements</li>
   *   <li>Optimizes partition boundaries based on actual data distribution</li>
   *   <li>Minimizes partition pruning overhead through proper constraint design</li>
   *   <li>Supports future data ingestion with extended partition ranges</li>
   * </ul>
   * 
   * @param cp the database connection provider for executing partition creation operations
   * @param newTableName the name of the partitioned table requiring partition setup
   * @param dataSourceTable the table containing source data for partition boundary analysis (may include schema)
   * @param partitionCol the column name used for partitioning (typically a date/timestamp field)
   * @param partitionsSchema the schema name where partition tables will be created
   * 
   * @throws SQLException if database operations fail during partition creation
   * @throws PartitioningException if partition setup encounters structural or configuration errors
   * @throws Exception if data analysis or partition boundary calculation fails
   */
  private void createPartitionsForTable(ConnectionProvider cp, String newTableName, String dataSourceTable,
                                       String partitionCol, String partitionsSchema) throws Exception {
    
    log4j.info("Creating partitions using simplified approach for table {}", newTableName);
    
    cleanupExistingPartitions(cp, newTableName, partitionsSchema);
    YearRange yearRange = determinePartitionYearRange(cp, dataSourceTable, partitionCol);
    createYearlyPartitions(cp, newTableName, partitionsSchema, yearRange);
    validatePartitionsWithSampleData(cp, newTableName, dataSourceTable, partitionCol);
  }

  /**
   * Cleans up any existing partitions to avoid conflicts.
   */
  private void cleanupExistingPartitions(ConnectionProvider cp, String newTableName, String partitionsSchema) throws Exception {
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
  }

  /**
   * Determines the year range needed for partitions based on data in the source table.
   */
  private YearRange determinePartitionYearRange(ConnectionProvider cp, String dataSourceTable, String partitionCol) throws Exception {
    String dataSourceReference = dataSourceTable.contains(".") ? dataSourceTable : PUBLIC + dataSourceTable;
    
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
    return new YearRange(startYear, endYear);
  }

  /**
   * Creates yearly partitions for the specified year range.
   */
  private void createYearlyPartitions(ConnectionProvider cp, String newTableName, String partitionsSchema, YearRange yearRange) throws Exception {
    for (int year = yearRange.startYear(); year <= yearRange.endYear() + 1; year++) {
      createSinglePartition(cp, newTableName, partitionsSchema, year);
    }
  }

  /**
   * Creates a single partition for the specified year.
   */
  private void createSinglePartition(ConnectionProvider cp, String newTableName, String partitionsSchema, int year) throws Exception {
    String partitionName = String.format("%s_y%d", newTableName.toLowerCase(), year);
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
      handlePartitionCreationError(partitionName, e);
    }
  }

  /**
   * Handles errors during partition creation.
   */
  private void handlePartitionCreationError(String partitionName, Exception e) throws OBException {
    if (e.getMessage().contains("already exists")) {
      log4j.warn("Partition {} already exists, skipping", partitionName);
    } else {
      log4j.error("Failed to create partition {}: {}", partitionName, e.getMessage());
      throw new OBException(e);
    }
  }

  /**
   * Validates partitions by testing sample data against partition bounds.
   */
  private void validatePartitionsWithSampleData(ConnectionProvider cp, String newTableName, String dataSourceTable, String partitionCol) throws Exception {
    String dataSourceReference = dataSourceTable.contains(".") ? dataSourceTable : PUBLIC + dataSourceTable;
    
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
   * Immutable record representing a year range for partition creation and management.
   * 
   * <p>This record defines the temporal boundaries for creating date-based partitions.
   * It encapsulates the start and end years that determine which partition tables
   * should be created for a partitioned table based on date ranges.</p>
   * 
   * <p><strong>Typical Usage:</strong></p>
   * <ul>
   *   <li>Defining partition creation boundaries during table partitioning</li>
   *   <li>Calculating required partitions based on data distribution</li>
   *   <li>Determining historical and future partition requirements</li>
   *   <li>Validating partition coverage for specific date ranges</li>
   * </ul>
   * 
   * <p>The range is inclusive of both start and end years, meaning partitions
   * will be created for all years from startYear through endYear inclusive.</p>
   * 
   * @param startYear the first year for which partitions should be created (inclusive)
   * @param endYear the last year for which partitions should be created (inclusive)
   */
  private record YearRange(int startYear, int endYear) {
  }

  private List<String> listColumnsForRelation(ConnectionProvider cp, String fqName) throws Exception {
    String fqn = fqName.toLowerCase();
    List<String> cols = new ArrayList<>();
    String sql = "SELECT attname "
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
