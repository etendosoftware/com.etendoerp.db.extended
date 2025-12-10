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

package com.etendoerp.db.extended.utils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openbravo.database.ConnectionProvider;

/**
 * Manages database backup operations for partitioned table constraint processing.
 *
 * <p>This class provides comprehensive backup functionality to ensure data safety during
 * constraint modifications on partitioned tables. It implements a robust backup strategy
 * with automatic cleanup and retention policies.
 *
 * <h3>Key Features:</h3>
 * <ul>
 *   <li><strong>Automatic Infrastructure:</strong> Creates backup schema and metadata tables as needed</li>
 *   <li><strong>Smart Backup Creation:</strong> Only creates backups when actually needed</li>
 *   <li><strong>Retention Management:</strong> Automatically cleans up old backups based on policies</li>
 *   <li><strong>Metadata Tracking:</strong> Maintains detailed records of all backup operations</li>
 *   <li><strong>Manual Restore:</strong> Provides backup information for manual restoration when operations fail</li>
 * </ul>
 *
 * <h3>Backup Strategy:</h3>
 * <ul>
 *   <li><strong>Schema:</strong> All backups are stored in the {@code etarc_backups} schema</li>
 *   <li><strong>Naming:</strong> Backup tables follow the pattern {@code backup_<tablename>_<timestamp>}</li>
 *   <li><strong>Retention:</strong> Maximum of {@value #MAX_BACKUPS_PER_TABLE} backups per table</li>
 *   <li><strong>Age Policy:</strong> Backups older than {@value #BACKUP_RETENTION_DAYS} days are eligible for cleanup</li>
 * </ul>
 *
 * <h3>Backup Infrastructure:</h3>
 * <p>The backup system uses the following database objects:
 * <ul>
 *   <li>{@code etarc_backups} schema - Contains all backup tables and metadata</li>
 *   <li>{@code backup_metadata} table - Tracks backup creation times and relationships</li>
 *   <li>{@code processing_metadata} table - Tracks processing timestamps for tables</li>
 * </ul>
 *
 * <h3>Error Handling:</h3>
 * <p>When SQL execution fails after a backup has been created, the system does NOT automatically
 * restore the backup. Instead, it throws an exception with detailed instructions for manual
 * restoration.
 *
 * <h3>Usage Pattern:</h3>
 * <pre>{@code
 * BackupManager backupManager = new BackupManager();
 * backupManager.ensureBackupInfrastructure(connectionProvider);
 * backupManager.cleanupExcessBackups(connectionProvider);
 *
 * // Execute SQL with automatic backup (manual restore on failure)
 * backupManager.executeSqlWithBackup(cp, "my_table", true, "ALTER TABLE ...");
 * }</pre>
 *
 * @author Futit Services S.L.
 * @see com.etendoerp.db.extended.modulescript.PartitionedConstraintsHandling
 * @since ETP-2450
 */
public class BackupManager {
  private static final Logger log4j = LogManager.getLogger();

  // Backup infrastructure
  private static final String BACKUP_SCHEMA = "etarc_backups";
  private static final String BACKUP_METADATA_TABLE = "backup_metadata"; // in BACKUP_SCHEMA
  private static final int MAX_BACKUPS_PER_TABLE = 5;
  private static final int BACKUP_RETENTION_DAYS = 7;
  public static final String SEPARATOR = "========================================================";

  /**
   * Creates backup schema/tables if missing. Best-effort, non-throwing.
   *
   * @param cp
   *     the ConnectionProvider for database access
   */
  public void ensureBackupInfrastructure(ConnectionProvider cp) {
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
   *
   * @param cp
   *     the ConnectionProvider for database access
   * @param tableName
   *     the table name to query for processing timestamp
   * @return the last processed timestamp, or null if not found or on error
   */
  public Timestamp getLastProcessed(ConnectionProvider cp, String tableName) {
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
   *
   * @param cp
   *     the ConnectionProvider for database access
   * @param tableName
   *     the table name to update processing timestamp for
   */
  public void setLastProcessed(ConnectionProvider cp, String tableName) {
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
   *
   * @param cp
   *     the ConnectionProvider for database access
   */
  public void cleanupExcessBackups(ConnectionProvider cp) {
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
   * Creates a snapshot table in {@code BACKUP_SCHEMA} and records it in metadata.
   *
   * @param cp
   *     the ConnectionProvider for database access
   * @param tableName
   *     the table name to create a backup for
   * @return the backup table name that was created
   * @throws Exception
   *     if backup creation fails
   */
  public String createTableBackup(ConnectionProvider cp, String tableName) throws Exception {
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
   * Executes the given SQL against the table. If the table is partitioned, creates a backup
   * beforehand. On failure, throws an exception with backup restoration instructions.
   *
   * @param cp
   *     the ConnectionProvider for database access
   * @param tableName
   *     the table name to execute SQL against
   * @param isPartitioned
   *     whether the table is partitioned (determines if backup is needed)
   * @param sql
   *     the SQL statement to execute
   * @throws BackupOperationException
   *     if SQL execution fails, with information about available backup
   */
  public void executeSqlWithBackup(ConnectionProvider cp, String tableName, boolean isPartitioned, String sql)
      throws BackupOperationException {
    String backupName = null;
    try {
      if (isPartitioned) {
        backupName = createTableBackup(cp, tableName);
        log4j.info("Created backup {} for table {}", backupName, tableName);
      }
      if (StringUtils.isBlank(sql)) {
        log4j.info("No SQL to execute for table {}", tableName);
        return;
      }
      try (PreparedStatement ps = cp.getPreparedStatement(sql)) {
        ps.executeUpdate();
      }
    } catch (Exception e) {
      String errorMsg = String.format(
          "Error executing SQL for table %s: %s",
          tableName, e.getMessage());
      log4j.error(errorMsg, e);

      if (backupName != null) {
        log4j.error(SEPARATOR);
        log4j.error("BACKUP AVAILABLE FOR RESTORATION");
        log4j.error(SEPARATOR);
        log4j.error("Table: {}", tableName);
        log4j.error("Backup: {}.{}", BACKUP_SCHEMA, backupName);
        log4j.error(SEPARATOR);
        
        String backupInfo = String.format(
            "\n========================================================\n" +
            "BACKUP AVAILABLE FOR RESTORATION\n" +
            "========================================================\n" +
            "Table: %s\n" +
            "Backup: %s.%s\n" +
                SEPARATOR,
            tableName, BACKUP_SCHEMA, backupName);
        throw new BackupOperationException(errorMsg + backupInfo, e);
      }
      throw new BackupOperationException(errorMsg, e);
    }
  }

  private void logStartCleanup() {
    log4j.info(
        "Starting backup cleanup - will keep {} most recent backups per table and delete backups older than {} days",
        MAX_BACKUPS_PER_TABLE, BACKUP_RETENTION_DAYS);
  }

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

  private void dropBackupTableQuietly(ConnectionProvider cp, String backupName) {
    String dropSql = "DROP TABLE IF EXISTS " + BACKUP_SCHEMA + "." + backupName;
    try (PreparedStatement drop = cp.getPreparedStatement(dropSql)) {
      drop.executeUpdate();
      log4j.debug("Dropped backup table: {}", backupName);
    } catch (Exception e) {
      log4j.warn("Failed to drop backup table {}: {}", backupName, e.getMessage());
    }
  }

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

  private void logSummary(int totalDeleted) {
    if (totalDeleted > 0) {
      log4j.info("Total cleanup: removed {} old/excess backup tables", totalDeleted);
    } else {
      log4j.info("No excess or old backups found to clean up");
    }
  }

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
            dropBackupTable(cp, tableName, backupName);
            deleteBackupMetadata(cp, backupName, deleteMetaSql);
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

  private void dropBackupTable(ConnectionProvider cp, String tableName, String backupName) {
    try (PreparedStatement drop = cp.getPreparedStatement(
        "DROP TABLE IF EXISTS " + BACKUP_SCHEMA + "." + backupName)) {
      drop.executeUpdate();
      log4j.info("Dropped backup table for {}: {} (excess or > {} days old)",
          tableName, backupName, BACKUP_RETENTION_DAYS);
    } catch (Exception e) {
      log4j.warn("Failed to drop backup table {}: {}", backupName, e.getMessage());
    }
  }

  private void deleteBackupMetadata(ConnectionProvider cp, String backupName, String deleteMetaSql) {
    try (PreparedStatement del = cp.getPreparedStatement(deleteMetaSql)) {
      del.setString(1, backupName);
      del.executeUpdate();
    } catch (Exception e) {
      log4j.warn("Failed to delete metadata for backup {}: {}", backupName, e.getMessage());
    }
  }
}
