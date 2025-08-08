package com.etendoerp.db.extended.utils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openbravo.database.ConnectionProvider;

/**
 * Utility class for analyzing table structures and properties.
 * Provides methods to inspect tables, columns, and partitioning information.
 */
public class TableAnalyzer {
  
  private static final Logger log4j = LogManager.getLogger();

  /**
   * Gets the list of columns that exist in both source and destination tables.
   * 
   * @param cp the connection provider
   * @param sourceTableName the source table (can include schema)
   * @param destinationTableName the destination table name
   * @return list of matching column names
   * @throws Exception if query fails
   */
  public List<String> getMatchingColumns(ConnectionProvider cp, String sourceTableName, String destinationTableName) throws Exception {
    List<String> matchingColumns = new ArrayList<>();
    
    // Handle source table name that might include schema
    String sourceSchema = "public";
    String sourceTable = sourceTableName;
    if (sourceTableName.contains(".")) {
      String[] parts = sourceTableName.split("\\.");
      sourceSchema = parts[0];
      sourceTable = parts[1];
    }
    
    String sql = "SELECT DISTINCT c1.column_name " +
                 "FROM information_schema.columns c1 " +
                 "INNER JOIN information_schema.columns c2 " +
                 "ON c1.column_name = c2.column_name " +
                 "AND c1.data_type = c2.data_type " +
                 "WHERE c1.table_schema = ? AND c1.table_name = ? " +
                 "AND c2.table_schema = 'public' AND c2.table_name = ? " +
                 "ORDER BY c1.column_name";
    
    try (PreparedStatement ps = cp.getPreparedStatement(sql)) {
      ps.setString(1, sourceSchema);
      ps.setString(2, sourceTable.toUpperCase());
      ps.setString(3, destinationTableName.toUpperCase());
      
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          matchingColumns.add(rs.getString(1));
        }
      }
    }
    
    log4j.info("Found {} matching columns between {} and {}", 
              matchingColumns.size(), sourceTableName, destinationTableName);
    
    return matchingColumns;
  }

  /**
   * Checks whether the given table is currently partitioned in the PostgreSQL database.
   */
  public boolean isTablePartitioned(ConnectionProvider cp, String tableName) throws Exception {
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
   */
  public List<String> getPrimaryKeyColumns(ConnectionProvider cp, String tableName) throws Exception {
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
   * Gets the row count for a table.
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
   * Gets approximate row count using table statistics for better performance.
   */
  public long getApproximateTableRowCount(ConnectionProvider cp, String tableName) throws Exception {
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
   * Checks if a table exists.
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

  /**
   * Gets table size information for performance analysis.
   */
  public TableSizeInfo getTableSizeInfo(ConnectionProvider cp, String tableName) throws Exception {
    String sizeSql = "SELECT " +
                     "  pg_size_pretty(pg_total_relation_size(?)) as total_size, " +
                     "  pg_size_pretty(pg_relation_size(?)) as table_size, " +
                     "  pg_size_pretty(pg_indexes_size(?)) as indexes_size";
    
    try (PreparedStatement ps = cp.getPreparedStatement(sizeSql)) {
      ps.setString(1, tableName);
      ps.setString(2, tableName);
      ps.setString(3, tableName);
      
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          return new TableSizeInfo(
              rs.getString("total_size"),
              rs.getString("table_size"),
              rs.getString("indexes_size")
          );
        }
      }
    }
    
    return new TableSizeInfo("0 bytes", "0 bytes", "0 bytes");
  }

  /**
   * Data class to hold table size information.
   */
  public record TableSizeInfo(String totalSize, String tableSize, String indexesSize) {}
}
