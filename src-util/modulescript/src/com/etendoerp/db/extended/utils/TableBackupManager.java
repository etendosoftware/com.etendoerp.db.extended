package com.etendoerp.db.extended.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openbravo.database.ConnectionProvider;

/**
 * Utility class for managing table backups with performance optimizations for large tables.
 * Handles backup creation, restoration, and cleanup operations.
 */
public class TableBackupManager {
  
  private static final Logger log4j = LogManager.getLogger();
  
  /**
   * Creates backup of table data with performance optimizations for large tables.
   * 
   * @param cp the connection provider
   * @param tableName the name of the table to backup
   * @throws Exception if backup fails
   */
  public void backupTableData(ConnectionProvider cp, String tableName) throws Exception {
    String backupTableName = tableName.toLowerCase() + "_backup_" + System.currentTimeMillis();
    
    try {
      // Use approximate row count for performance on large tables
      long rowCount = getApproximateTableRowCount(cp, tableName);
      log4j.info("Creating backup for table {} (~{} rows) as {}.{}", tableName, rowCount, Constants.BACKUP_SCHEMA, backupTableName);
      
      if (rowCount == 0) {
        log4j.info("Table {} is empty, skipping data backup", tableName);
        return;
      }
      
      // For large tables, use optimized backup strategy
      if (rowCount > Constants.LARGE_TABLE_THRESHOLD) {
        createLargeTableBackup(cp, tableName, backupTableName, rowCount);
      } else {
        createStandardBackup(cp, tableName, backupTableName);
      }
      
      // Store backup info for later restoration
      storeBackupInfo(cp, tableName, backupTableName);
      
    } catch (Exception e) {
      log4j.error("Failed to backup table {}: {}", tableName, e.getMessage(), e);
      // Don't throw exception here - we want to continue processing other tables
    }
  }

  /**
   * Creates backup for large tables using batch processing to avoid memory issues.
   */
  private void createLargeTableBackup(ConnectionProvider cp, String tableName, String backupTableName, long estimatedRows) throws Exception {
    log4j.info("Using optimized backup strategy for large table {} ({} rows)", tableName, estimatedRows);
    
    // First, create empty backup table structure
    String createStructureSql = String.format(
        "CREATE TABLE %s.%s (LIKE public.%s INCLUDING ALL)",
        Constants.BACKUP_SCHEMA, backupTableName, tableName
    );
    executeUpdate(cp, createStructureSql);
    
    // Configure connection for large operations
    Connection conn = cp.getConnection();
    boolean originalAutoCommit = conn.getAutoCommit();
    
    try {
      conn.setAutoCommit(false);
      
      // Use batch processing for data copying
      int batchSize = calculateOptimalBatchSize(estimatedRows);
      long totalCopied = 0;
      long offset = 0;
      
      log4j.info("Starting batch backup with batch size: {}", batchSize);
      
      while (true) {
        String batchSql = String.format(
            "INSERT INTO %s.%s SELECT * FROM public.%s LIMIT %d OFFSET %d",
            Constants.BACKUP_SCHEMA, backupTableName, tableName, batchSize, offset
        );
        
        int rowsCopied = executeUpdateWithRowCount(cp, batchSql);
        
        if (rowsCopied == 0) {
          break; // No more rows to copy
        }
        
        totalCopied += rowsCopied;
        offset += batchSize;
        
        // Commit batch and log progress
        conn.commit();
        
        if (totalCopied % (batchSize * 10) == 0) {
          log4j.info("Backup progress: {} rows copied to {}.{}", totalCopied, Constants.BACKUP_SCHEMA, backupTableName);
        }
        
        // Break if we copied fewer rows than batch size (last batch)
        if (rowsCopied < batchSize) {
          break;
        }
      }
      
      log4j.info("Successfully backed up {} rows from {} to {}.{}", totalCopied, tableName, Constants.BACKUP_SCHEMA, backupTableName);
      
    } finally {
      conn.setAutoCommit(originalAutoCommit);
    }
  }

  /**
   * Creates standard backup for smaller tables.
   */
  private void createStandardBackup(ConnectionProvider cp, String tableName, String backupTableName) throws Exception {
    String backupSql = String.format(
        "CREATE TABLE %s.%s AS SELECT * FROM public.%s",
        Constants.BACKUP_SCHEMA, backupTableName, tableName
    );
    
    executeUpdate(cp, backupSql);
    
    // Get actual row count after backup
    long backupRowCount = getTableRowCount(cp, Constants.BACKUP_SCHEMA + "." + backupTableName);
    log4j.info("Successfully backed up {} rows from {} to {}.{}", backupRowCount, tableName, Constants.BACKUP_SCHEMA, backupTableName);
  }

  /**
   * Calculates optimal batch size based on table size and available memory.
   */
  private int calculateOptimalBatchSize(long estimatedRows) {
    if (estimatedRows < 100000) {
      return 10000;
    } else if (estimatedRows < 1000000) {
      return 25000;
    } else if (estimatedRows < 10000000) {
      return 50000;
    } else {
      return 100000; // For very large tables
    }
  }

  /**
   * Gets approximate row count using table statistics for better performance.
   */
  private long getApproximateTableRowCount(ConnectionProvider cp, String tableName) throws Exception {
    // Try to get approximate count from statistics first (much faster for large tables)
    String statsSql = String.format(
        "SELECT reltuples::BIGINT FROM pg_class WHERE relname = '%s' AND relkind = 'r'",
        tableName.toLowerCase()
    );
    
    try (PreparedStatement ps = cp.getPreparedStatement(statsSql);
         ResultSet rs = ps.executeQuery()) {
      if (rs.next()) {
        long approxCount = rs.getLong(1);
        if (approxCount > 0) {
          return approxCount;
        }
      }
    }
    
    // Fallback to exact count for smaller tables or when stats are not available
    return getTableRowCount(cp, tableName);
  }

  /**
   * Stores backup information for later restoration.
   */
  private void storeBackupInfo(ConnectionProvider cp, String originalTableName, String backupTableName) throws Exception {
    // Ensure backup schema exists
    executeUpdate(cp, String.format(Constants.CREATE_SCHEMA_SQL, Constants.BACKUP_SCHEMA));
    
    // Create a simple table to track backups if it doesn't exist
    String createBackupTrackingTable = String.format(
        "CREATE TABLE IF NOT EXISTS %s.backup_tracking (" +
        "original_table " + Constants.VARCHAR_255 + ", " +
        "backup_table " + Constants.VARCHAR_255 + ", " +
        "backup_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
        ")", Constants.BACKUP_SCHEMA
    );
    
    executeUpdate(cp, createBackupTrackingTable);
    
    // Store backup info
    String insertBackupInfo = String.format(
        "INSERT INTO %s.backup_tracking (original_table, backup_table) VALUES (?, ?)",
        Constants.BACKUP_SCHEMA
    );
    
    try (PreparedStatement ps = cp.getPreparedStatement(insertBackupInfo)) {
      ps.setString(1, originalTableName.toUpperCase());
      ps.setString(2, backupTableName);
      ps.executeUpdate();
    }
  }

  /**
   * Gets the latest backup table name for a given original table.
   */
  public String getLatestBackupTable(ConnectionProvider cp, String originalTableName) throws Exception {
    String query = String.format(
        "SELECT backup_table FROM %s.backup_tracking " +
        "WHERE original_table = ? " +
        "ORDER BY backup_timestamp DESC LIMIT 1",
        Constants.BACKUP_SCHEMA
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
   */
  public void cleanupBackup(ConnectionProvider cp, String originalTableName, String backupTableName) throws Exception {
    try {
      // Drop the backup table
      executeUpdate(cp, String.format("DROP TABLE IF EXISTS %s.%s CASCADE", Constants.BACKUP_SCHEMA, backupTableName));
      
      // Remove from tracking table
      String deleteTracking = String.format(
          "DELETE FROM %s.backup_tracking WHERE original_table = ? AND backup_table = ?",
          Constants.BACKUP_SCHEMA
      );
      
      try (PreparedStatement ps = cp.getPreparedStatement(deleteTracking)) {
        ps.setString(1, originalTableName.toUpperCase());
        ps.setString(2, backupTableName);
        ps.executeUpdate();
      }
      
      log4j.info("Successfully cleaned up backup {}.{}", Constants.BACKUP_SCHEMA, backupTableName);
      
    } catch (Exception e) {
      log4j.warn("Warning: Failed to cleanup backup {}.{}: {}", Constants.BACKUP_SCHEMA, backupTableName, e.getMessage());
      // Don't throw exception for cleanup failures
    }
  }

  /**
   * Cleans up old backup tables that are older than 7 days.
   */
  public void cleanupOldBackups(ConnectionProvider cp) throws Exception {
    try {
      log4j.info("Cleaning up old backup tables (older than 7 days)...");
      
      // Ensure backup schema and tracking table exist before attempting cleanup
      ensureBackupSchemaAndTrackingTableExist(cp);
      
      // Find old backup tables
      String findOldBackupsSql = String.format(
          "SELECT backup_table FROM %s.backup_tracking " +
          "WHERE backup_timestamp < CURRENT_TIMESTAMP - INTERVAL '7 days'",
          Constants.BACKUP_SCHEMA
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
          executeUpdate(cp, String.format("DROP TABLE IF EXISTS %s.%s CASCADE", Constants.BACKUP_SCHEMA, backupTable));
          log4j.info("Dropped old backup table: {}.{}", Constants.BACKUP_SCHEMA, backupTable);
        } catch (Exception e) {
          log4j.warn("Warning: Failed to drop old backup table {}.{}: {}", Constants.BACKUP_SCHEMA, backupTable, e.getMessage());
        }
      }
      
      // Clean up tracking records
      String cleanupTrackingSql = String.format(
          "DELETE FROM %s.backup_tracking " +
          "WHERE backup_timestamp < CURRENT_TIMESTAMP - INTERVAL '7 days'",
          Constants.BACKUP_SCHEMA
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
   */
  private void ensureBackupSchemaAndTrackingTableExist(ConnectionProvider cp) throws Exception {
    try {
      // Create backup schema if it doesn't exist
      executeUpdate(cp, String.format(Constants.CREATE_SCHEMA_SQL, Constants.BACKUP_SCHEMA));
      
      // Create backup tracking table if it doesn't exist
      String createBackupTrackingTable = String.format(
          "CREATE TABLE IF NOT EXISTS %s.backup_tracking (" +
          "original_table " + Constants.VARCHAR_255 + ", " +
          "backup_table " + Constants.VARCHAR_255 + ", " +
          "backup_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
          ")", Constants.BACKUP_SCHEMA
      );
      
      executeUpdate(cp, createBackupTrackingTable);
      log4j.debug("Ensured backup schema '{}' and tracking table exist", Constants.BACKUP_SCHEMA);
      
    } catch (Exception e) {
      log4j.warn("Warning: Failed to ensure backup schema and tracking table exist: {}", e.getMessage());
      // Don't throw exception - this is just preparation for cleanup
    }
  }

  // Helper methods
  private void executeUpdate(ConnectionProvider cp, String sql) throws Exception {
    try (PreparedStatement ps = cp.getPreparedStatement(sql)) {
      ps.executeUpdate();
    }
  }

  private int executeUpdateWithRowCount(ConnectionProvider cp, String sql) throws Exception {
    try (PreparedStatement ps = cp.getPreparedStatement(sql)) {
      return ps.executeUpdate();
    }
  }

  /**
   * Gets the row count for a table.
   *
   * @param cp the connection provider
   * @param tableName the name of the table (can include schema)
   * @return the number of rows in the table
   * @throws Exception if the query fails
   */
  public long getTableRowCount(ConnectionProvider cp, String tableName) throws Exception {
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
   * Checks if a table exists.
   *
   * @param cp the connection provider
   * @param tableName the name of the table to check
   * @return true if the table exists, false otherwise
   * @throws Exception if the query fails
   */
  public boolean tableExists(ConnectionProvider cp, String tableName) throws Exception {
    try (PreparedStatement ps = cp.getPreparedStatement(
        "SELECT 1 FROM information_schema.tables WHERE table_name = ? AND table_schema = 'public'")) {
      ps.setString(1, tableName.toLowerCase());
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    }
  }
}
