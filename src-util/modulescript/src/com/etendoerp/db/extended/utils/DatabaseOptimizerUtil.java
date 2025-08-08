package com.etendoerp.db.extended.utils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openbravo.database.ConnectionProvider;

/**
 * Utility class for database optimizations and performance tuning.
 * Handles PostgreSQL-specific optimizations for large table operations.
 */
public class DatabaseOptimizerUtil {
  
  private static final Logger log4j = LogManager.getLogger();

  /**
   * Optimizes database settings and indexes for large table operations.
   */
  public void optimizeDatabaseForLargeOperations(ConnectionProvider cp) throws Exception {
    log4j.info("Applying database optimizations for large table operations");
    
    try {
      // Increase memory settings for bulk operations
      executeUpdate(cp, "SET work_mem = '256MB'");
      executeUpdate(cp, "SET maintenance_work_mem = '1GB'");
      executeUpdate(cp, "SET shared_buffers = '256MB'");
      
      // Optimize for bulk loading
      executeUpdate(cp, "SET synchronous_commit = OFF");
      executeUpdate(cp, "SET wal_buffers = '16MB'");
      executeUpdate(cp, "SET checkpoint_completion_target = 0.9");
      
      // Disable autovacuum during bulk operations
      executeUpdate(cp, "SET autovacuum = OFF");
      
      log4j.info("Database optimizations applied successfully");
    } catch (Exception e) {
      log4j.warn("Some database optimizations failed: {}", e.getMessage());
    }
  }

  /**
   * Restores original database settings after large operations.
   */
  public void restoreDatabaseSettings(ConnectionProvider cp) throws Exception {
    log4j.info("Restoring original database settings");
    
    try {
      executeUpdate(cp, "RESET work_mem");
      executeUpdate(cp, "RESET maintenance_work_mem");
      executeUpdate(cp, "RESET shared_buffers");
      executeUpdate(cp, "RESET synchronous_commit");
      executeUpdate(cp, "RESET wal_buffers");
      executeUpdate(cp, "RESET checkpoint_completion_target");
      executeUpdate(cp, "RESET autovacuum");
      
      log4j.info("Database settings restored successfully");
    } catch (Exception e) {
      log4j.warn("Some settings restoration failed: {}", e.getMessage());
    }
  }

  /**
   * Creates indexes on partitioned table after data migration for optimal performance.
   */
  public void createOptimizedIndexes(ConnectionProvider cp, String tableName, String partitionCol) throws Exception {
    log4j.info("Creating optimized indexes for partitioned table: {}", tableName);
    
    try {
      // Create index on partition key
      String partitionKeyIndex = String.format(
          "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_%s_%s ON public.%s (%s)",
          tableName.toLowerCase(), partitionCol, tableName, partitionCol
      );
      executeUpdate(cp, partitionKeyIndex);
      
      // Create index on primary key if it exists
      String pkIndexSql = String.format(
          "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_%s_pk ON public.%s (COALESCE((SELECT column_name FROM information_schema.key_column_usage WHERE table_name = '%s' AND constraint_name LIKE '%%_pkey'), 'id'))",
          tableName.toLowerCase(), tableName, tableName.toLowerCase()
      );
      
      try {
        executeUpdate(cp, pkIndexSql);
      } catch (Exception e) {
        log4j.warn("Could not create primary key index: {}", e.getMessage());
      }
      
      log4j.info("Optimized indexes created successfully for {}", tableName);
    } catch (Exception e) {
      log4j.warn("Index creation failed for {}: {}", tableName, e.getMessage());
    }
  }

  /**
   * Analyzes table to update statistics after partitioning for optimal query planning.
   */
  public void analyzePartitionedTable(ConnectionProvider cp, String tableName) throws Exception {
    log4j.info("Analyzing partitioned table for optimal query planning: {}", tableName);
    
    try {
      String analyzeSql = String.format("ANALYZE public.%s", tableName);
      executeUpdate(cp, analyzeSql);
      
      log4j.info("Table analysis completed for {}", tableName);
    } catch (Exception e) {
      log4j.warn("Table analysis failed for {}: {}", tableName, e.getMessage());
    }
  }

  /**
   * Monitors and logs performance metrics during large operations.
   */
  public void logPerformanceMetrics(ConnectionProvider cp, String operation) throws Exception {
    try {
      String metricsSql = 
          "SELECT " +
          "  current_setting('work_mem') as work_mem, " +
          "  current_setting('maintenance_work_mem') as maintenance_work_mem, " +
          "  pg_size_pretty(pg_database_size(current_database())) as db_size";
      
      try (PreparedStatement ps = cp.getPreparedStatement(metricsSql);
           ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          log4j.info("Performance metrics for {}: work_mem={}, maintenance_work_mem={}, db_size={}", 
                     operation, rs.getString(1), rs.getString(2), rs.getString(3));
        }
      }
    } catch (Exception e) {
      log4j.debug("Could not retrieve performance metrics: {}", e.getMessage());
    }
  }

  // Helper method
  private void executeUpdate(ConnectionProvider cp, String sql) throws Exception {
    try (PreparedStatement ps = cp.getPreparedStatement(sql)) {
      ps.executeUpdate();
    }
  }
}
