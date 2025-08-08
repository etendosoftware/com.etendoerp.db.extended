package com.etendoerp.db.extended.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openbravo.database.ConnectionProvider;

/**
 * Service for handling data migration with performance optimizations for large datasets.
 * Provides intelligent batch processing and database optimization strategies.
 */
public class DataMigrationService {
  
  private static final Logger log4j = LogManager.getLogger();
  
  // Performance optimization constants
  private static final int DEFAULT_BATCH_SIZE = 50000;
  private static final int LARGE_TABLE_THRESHOLD = 1000000;
  private static final String MIGRATION_SUCCESS_MSG = "Successfully migrated {} rows from {} to {}";
  
  private final DatabaseOptimizerUtil dbOptimizer;
  private final TableAnalyzer tableAnalyzer;
  
  public DataMigrationService() {
    this.dbOptimizer = new DatabaseOptimizerUtil();
    this.tableAnalyzer = new TableAnalyzer();
  }

  /**
   * Migrates all data from the temporary table to the new partitioned table with performance optimizations.
   *
   * @param cp the connection provider
   * @param sourceTableName the source table name (can include schema)
   * @param targetTableName the target table name
   * @return the number of rows migrated
   * @throws Exception if data migration fails
   */
  public long migrateDataToNewTable(ConnectionProvider cp, String sourceTableName,
                                     String targetTableName, String partitionCol) throws Exception {
    // Check data range in source table - handle both schema.table and table formats
    String sourceReference = sourceTableName;
    if (!sourceTableName.contains(".")) {
      sourceReference = "public." + sourceTableName;
    }
    
    // Use approximate count for performance analysis
    DataMigrationStats stats = analyzeDataForMigration(cp, sourceReference, partitionCol);
    log4j.info("Data in {} - Min date: {}, Max date: {}, Total records: {}", 
               sourceReference, stats.minDate(), stats.maxDate(), stats.rowCount());
    
    if (stats.rowCount() == 0) {
      log4j.info("No data to migrate from {}", sourceReference);
      return 0;
    }
    
    // Get matching columns between source and destination tables
    List<String> matchingColumns = tableAnalyzer.getMatchingColumns(cp, sourceTableName, targetTableName);
    
    if (matchingColumns.isEmpty()) {
      throw new Exception("No matching columns found between source and destination tables");
    }
    
    String columnList = String.join(", ", matchingColumns);
    log4j.info("Will migrate {} matching columns: {}", matchingColumns.size(), columnList);
    
    // Choose migration strategy based on data size
    if (stats.rowCount() > LARGE_TABLE_THRESHOLD) {
      return migrateLargeDataset(cp, sourceReference, targetTableName, partitionCol, columnList, stats);
    } else {
      return migrateStandardDataset(cp, sourceReference, targetTableName, columnList);
    }
  }

  /**
   * Analyzes data before migration to determine optimal strategy.
   */
  private DataMigrationStats analyzeDataForMigration(ConnectionProvider cp, String sourceReference, String partitionCol) throws Exception {
    String analysisSql = String.format(
        "SELECT MIN(%1$s), MAX(%1$s), COUNT(*) FROM %2$s WHERE %1$s IS NOT NULL",
        partitionCol, sourceReference
    );
    
    try (PreparedStatement ps = cp.getPreparedStatement(analysisSql);
         ResultSet rs = ps.executeQuery()) {
      if (rs.next()) {
        return new DataMigrationStats(
            rs.getDate(1),
            rs.getDate(2),
            rs.getLong(3)
        );
      }
    }
    
    return new DataMigrationStats(null, null, 0);
  }

  /**
   * Migrates large datasets using batch processing and optimized settings.
   */
  private long migrateLargeDataset(ConnectionProvider cp, String sourceReference, String targetTableName,
                                   String partitionCol, String columnList, DataMigrationStats stats) throws Exception {
    log4j.info("Using optimized migration strategy for large dataset ({} rows)", stats.rowCount());
    
    Connection conn = cp.getConnection();
    boolean originalAutoCommit = conn.getAutoCommit();
    
    try {
      // Configure connection for large operations
      conn.setAutoCommit(false);
      
      // Apply database optimizations
      dbOptimizer.optimizeDatabaseForLargeOperations(cp);
      dbOptimizer.logPerformanceMetrics(cp, "Pre-migration " + targetTableName);
      
      int batchSize = calculateOptimalBatchSize(stats.rowCount());
      long totalMigrated = 0;
      long offset = 0;
      
      log4j.info("Starting batch migration with batch size: {}", batchSize);
      
      while (true) {
        String batchSql = String.format(
            "INSERT INTO public.%s (%s) SELECT %s FROM %s ORDER BY %s LIMIT %d OFFSET %d",
            targetTableName, columnList, columnList, sourceReference, partitionCol, batchSize, offset
        );
        
        int rowsMigrated = executeUpdateWithRowCount(cp, batchSql);
        
        if (rowsMigrated == 0) {
          break; // No more rows to migrate
        }
        
        totalMigrated += rowsMigrated;
        offset += batchSize;
        
        // Commit batch and log progress
        conn.commit();
        
        if (totalMigrated % (batchSize * 5) == 0) {
          log4j.info("Migration progress: {} rows migrated to {}", totalMigrated, targetTableName);
        }
        
        // Break if we migrated fewer rows than batch size (last batch)
        if (rowsMigrated < batchSize) {
          break;
        }
      }
      
      // Restore database settings
      dbOptimizer.restoreDatabaseSettings(cp);
      dbOptimizer.logPerformanceMetrics(cp, "Post-migration " + targetTableName);
      
      logMigrationSuccessMessage(targetTableName, totalMigrated, sourceReference);
      return totalMigrated;
      
    } catch (SQLException e) {
      log4j.error("Large dataset migration failed. Error: {}", e.getMessage());
      handleMigrationError(cp, sourceReference, e);
      throw e;
    } finally {
      conn.setAutoCommit(originalAutoCommit);
      // Ensure database settings are restored even if migration fails
      try {
        dbOptimizer.restoreDatabaseSettings(cp);
      } catch (Exception restoreError) {
        log4j.warn("Failed to restore database settings: {}", restoreError.getMessage());
      }
    }
  }

  /**
   * Migrates standard datasets using single operation.
   */
  private long migrateStandardDataset(ConnectionProvider cp, String sourceReference, String targetTableName, String columnList) throws Exception {
    String insertSql = String.format("INSERT INTO public.%s (%s) SELECT %s FROM %s", 
                                    targetTableName, columnList, columnList, sourceReference);
    log4j.info("Executing standard data migration SQL: {}", insertSql);
    
    try (PreparedStatement ps = cp.getPreparedStatement(insertSql)) {
      int rowCount = ps.executeUpdate();
      logMigrationSuccessMessage(targetTableName, rowCount, sourceReference);
      return rowCount;
    } catch (SQLException e) {
      log4j.error("Standard data migration failed. Error: {}", e.getMessage());
      handleMigrationError(cp, sourceReference, e);
      throw e;
    }
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
      return DEFAULT_BATCH_SIZE;
    } else {
      return 100000; // For very large tables
    }
  }

  private void logMigrationSuccessMessage(String targetTableName, long migratedRows, String sourceReference) {
    log4j.info(MIGRATION_SUCCESS_MSG, migratedRows, sourceReference, targetTableName);
  }

  // Helper methods
  private int executeUpdateWithRowCount(ConnectionProvider cp, String sql) throws Exception {
    try (PreparedStatement ps = cp.getPreparedStatement(sql)) {
      return ps.executeUpdate();
    }
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
  public void createPartitionedTableFromTemplate(ConnectionProvider cp, String newTableName, 
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
    executeUpdateWithRowCount(cp, createTableSql);
    log4j.info("Created new partitioned table: public.{}", newTableName);
  }

  /**
   * Converts an empty table to a partitioned table.
   *
   * @param tableName the name of the table to convert
   * @throws Exception if conversion fails
   */
  public void convertEmptyTableToPartitioned(String tableName) throws Exception {
    // For empty tables, we can use a simpler approach
    // This would require more complex DDL manipulation in a real implementation
    log4j.warn("Empty table conversion not fully implemented - table {} will need manual partitioning", tableName);
    // TODO: Implement direct table conversion without data migration
    throw new Exception("Empty table conversion not yet implemented for " + tableName);
  }

  /**
   * Handles migration errors with appropriate logging and data preservation.
   *
   * @param cp the connection provider
   * @param sourceReference the source table reference
   * @param e the SQLException that occurred
   * @throws Exception if error handling fails
   */
  public void handleMigrationError(ConnectionProvider cp, String sourceReference, SQLException e) throws Exception {
    log4j.error("Migration error from source {}: {}", sourceReference, e.getMessage());
    
    // Check if it's a constraint violation or other recoverable error
    String sqlState = e.getSQLState();
    if (sqlState != null) {
      switch (sqlState) {
        case "23505": // Unique violation
          log4j.error("Unique constraint violation - possible duplicate data in source");
          break;
        case "23503": // Foreign key violation
          log4j.error("Foreign key constraint violation - referential integrity issue");
          break;
        case "23514": // Check constraint violation
          log4j.error("Check constraint violation - data doesn't meet table constraints");
          break;
        default:
          log4j.error("SQL error with state: {}", sqlState);
      }
    }
    
    // For now, re-throw the exception
    // In a more sophisticated implementation, we might attempt data repair or partial migration
    throw new Exception("Data migration failed from " + sourceReference + ": " + e.getMessage(), e);
  }

  /**
   * Data class to hold migration statistics.
   */
  public record DataMigrationStats(java.sql.Date minDate, java.sql.Date maxDate, long rowCount) {}
}
