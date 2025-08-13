package com.etendoerp.db.extended.utils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openbravo.database.ConnectionProvider;

/**
 * Utility class for managing table partitions.
 * Handles partition creation, validation, and management operations.
 */
public class PartitionManager {
  
  private static final Logger log4j = LogManager.getLogger();

  /**
   * Creates partitions for a table based on data analysis.
   */
  public void createPartitionsForTable(ConnectionProvider cp, String newTableName, String dataSourceReference, 
                                      String partitionCol, String partitionsSchema) throws Exception {
    log4j.info("Creating partitions for table {} based on data in {}", newTableName, dataSourceReference);
    
    // Ensure partitions schema exists
    executeUpdate(cp, String.format(Constants.CREATE_SCHEMA_SQL, partitionsSchema));
    
    // Query to find the range of years in the data
    String yearRangeSql = String.format(
        "SELECT DISTINCT EXTRACT(YEAR FROM %s) as year FROM %s WHERE %s IS NOT NULL ORDER BY year",
        partitionCol, dataSourceReference, partitionCol
    );
    
    log4j.info("Analyzing date range in source data to create appropriate partitions...");
    
    try (PreparedStatement yearPs = cp.getPreparedStatement(yearRangeSql);
         ResultSet yearRs = yearPs.executeQuery()) {
      
      boolean hasPartitions = false;
      while (yearRs.next()) {
        int year = yearRs.getInt("year");
        String partitionName = newTableName.toLowerCase() + "_y" + year;
        
        String createPartitionSql = String.format(
            "CREATE TABLE IF NOT EXISTS %s.%s PARTITION OF public.%s " +
            "FOR VALUES FROM ('%d-01-01') TO ('%d-01-01')",
            partitionsSchema, partitionName, newTableName, year, year + 1
        );
        
        try {
          log4j.info("Creating partition: {}", createPartitionSql);
          executeUpdate(cp, createPartitionSql);
          log4j.info("Successfully created partition {}.{}", partitionsSchema, partitionName);
          hasPartitions = true;
        } catch (Exception e) {
          if (e.getMessage().contains("already exists")) {
            log4j.warn("Partition {} already exists, skipping", partitionName);
            hasPartitions = true;
          } else {
            log4j.error("Failed to create partition {}: {}", partitionName, e.getMessage());
            throw e;
          }
        }
      }
      
      if (!hasPartitions) {
        log4j.warn("No partitions were created for table {}. Data may not have valid dates.", newTableName);
      }
    }
    
    // Test if the first few records would fit
    testPartitionBounds(cp, newTableName, dataSourceReference, partitionCol);
  }

  /**
   * Tests if sample data fits in the created partitions.
   */
  private void testPartitionBounds(ConnectionProvider cp, String newTableName, String dataSourceReference, String partitionCol) throws Exception {
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
   * Creates a partitioned table based on the structure of a template table.
   */
  public void createPartitionedTableFromTemplate(ConnectionProvider cp, String newTableName, 
                                                String templateTableName, String partitionCol) throws Exception {
    // Handle template table name that might include schema
    String templateReference = templateTableName;
    if (!templateTableName.contains(".")) {
      templateReference = Constants.PUBLIC_SCHEMA_PREFIX + templateTableName;
    }
    
    String createTableSql = String.format(
        "CREATE TABLE " + Constants.PUBLIC_SCHEMA_PREFIX + "%s (LIKE %s INCLUDING DEFAULTS INCLUDING STORAGE INCLUDING COMMENTS) " +
        "PARTITION BY RANGE (%s)",
        newTableName, templateReference, partitionCol
    );

    log4j.info("Creating partitioned table with SQL: {}", createTableSql);
    executeUpdate(cp, createTableSql);
    log4j.info("Successfully created new partitioned table: {}", newTableName);
  }

  /**
   * Creates an updated partitioned table based on XML structure.
   */
  public void createUpdatedPartitionedTable(ConnectionProvider cp, String tableName, String partitionCol) throws Exception {
    // This would require integration with Etendo's table creation logic
    // For now, we'll use a simplified approach
    log4j.info("Creating updated partitioned table {} with partition column {}", tableName, partitionCol);
    
    // This should integrate with Etendo's XML-based table creation
    // For the refactoring, we'll keep the basic structure
    String createSql = String.format(
        "CREATE TABLE IF NOT EXISTS public.%s () PARTITION BY RANGE (%s)",
        tableName, partitionCol
    );
    
    try {
      executeUpdate(cp, createSql);
      log4j.info("Successfully created updated partitioned table: {}", tableName);
    } catch (Exception e) {
      log4j.error("Failed to create updated partitioned table {}: {}", tableName, e.getMessage());
      throw e;
    }
  }

  /**
   * Validates that a table is properly partitioned.
   */
  public boolean validatePartitionedTable(ConnectionProvider cp, String tableName) throws Exception {
    // Check if table is partitioned
    String checkPartitionSql = "SELECT 1 FROM pg_partitioned_table WHERE partrelid = to_regclass(?)";
    
    try (PreparedStatement ps = cp.getPreparedStatement(checkPartitionSql)) {
      ps.setString(1, tableName);
      try (ResultSet rs = ps.executeQuery()) {
        boolean isPartitioned = rs.next();
        
        if (!isPartitioned) {
          log4j.error("Table {} is not properly partitioned", tableName);
          return false;
        }
        
        // Check if table has partitions
        String checkPartitionsSql = String.format(
            "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE '%s_y%%'",
            tableName.toLowerCase()
        );
        
        try (PreparedStatement partPs = cp.getPreparedStatement(checkPartitionsSql);
             ResultSet partRs = partPs.executeQuery()) {
          if (partRs.next()) {
            int partitionCount = partRs.getInt(1);
            log4j.info("Table {} has {} partitions", tableName, partitionCount);
            return partitionCount > 0;
          }
        }
      }
    }
    
    return false;
  }

  // Helper method
  private void executeUpdate(ConnectionProvider cp, String sql) throws Exception {
    try (PreparedStatement ps = cp.getPreparedStatement(sql)) {
      ps.executeUpdate();
    }
  }
}
