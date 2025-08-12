package com.etendoerp.db.extended.utils;

import java.io.File;
import java.nio.file.NoSuchFileException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openbravo.database.ConnectionProvider;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.etendoerp.db.extended.modulescript.PartitionedConstraintsHandling;
import com.etendoerp.db.extended.utils.XmlParsingUtils;

/**
 * Utility class for managing database constraints in partitioned tables.
 * Handles constraint creation, deletion, and SQL generation.
 */
public class ConstraintManagementUtils {

  private static final Logger log4j = LogManager.getLogger();
  private static final String ALTER_TABLE = "ALTER TABLE IF EXISTS PUBLIC.%s\n";

  /**
   * Executes the constraint SQL if it is not blank.
   * This typically includes adding or modifying table constraints after analyzing configurations.
   *
   * @param cp the connection provider for accessing the database.
   * @param sql the SQL string to execute.
   * @throws Exception if a database access error occurs.
   */
  public void executeConstraintSqlIfNeeded(ConnectionProvider cp, String sql) throws Exception {
    if (PartitionedConstraintsHandling.isBlank(sql)) {
      log4j.info("No constraints to handle for the provided configurations.");
      return;
    }
    try (PreparedStatement ps = cp.getPreparedStatement(sql)) {
      ps.executeUpdate();
    }
  }

  /**
   * Builds a SQL script to drop and re-create primary key and foreign key
   * constraints for the specified table, taking partitioning into account.
   * <p>
   * Steps performed:
   * <ol>
   * <li>Checks whether {@code tableName} is partitioned.</li>
   * <li>Loads the table's XML to determine the primary key name.</li>
   * <li>Drops existing PK and re-adds it (with or without partition column).</li>
   * <li>Iterates all table XML definition files to find any foreign keys referencing
   * {@code tableName}, then drops and re-adds those FKs (partitioned if needed).</li>
   * </ol>
   *
   * @param tableName      the name of the table to modify
   * @param cp             the ConnectionProvider used to query catalog tables
   * @param pkField        the column name of the primary key
   * @param partitionField the partition key column (if table is partitioned)
   * @param sourcePath     the source path of the project
   * @return the complete DDL script as a single String
   * @throws Exception if any database or XML processing error occurs
   */
  public String buildConstraintSql(String tableName, ConnectionProvider cp, String pkField,
                                   String partitionField, String sourcePath) throws Exception {
    // Check if table is partitioned
    String checkPartition = "SELECT 1 FROM pg_partitioned_table WHERE partrelid = to_regclass(?)";
    PreparedStatement psCheck = cp.getPreparedStatement(checkPartition);
    psCheck.setString(1, tableName);
    boolean isPartitioned = psCheck.executeQuery().next();
    psCheck.close();

    // Get required information from the primary table's XML
    List<File> tableXmlFiles = XmlParsingUtils.findTableXmlFiles(tableName, sourcePath);
    if (tableXmlFiles.isEmpty()) {
      throw new Exception("Entity XML file for " + tableName + " not found.");
    }
    String pkName = XmlParsingUtils.findPrimaryKey(tableXmlFiles);
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
      sql.append(String.format(addPartitionedPrimaryKeySQL, tableName, pkName, pkField, partitionField));
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
    for (File dir : XmlParsingUtils.collectTableDirs(sourcePath)) {
      File[] xmlsInDir = dir.listFiles(f -> f.isFile() && f.getName().endsWith(".xml"));
      if (xmlsInDir == null) {
        continue;
      }

      for (File sourceXmlFile : xmlsInDir) {
        try {
          if (!sourceXmlFile.exists()) {
            continue;
          }

          Document doc = XmlParsingUtils.getDocument(sourceXmlFile);
          NodeList columnNodes = doc.getElementsByTagName("column");

          for (int i = 0; i < columnNodes.getLength(); i++) {
            Element column = (Element) columnNodes.item(i);

            if (column.hasAttribute("reference") &&
                PartitionedConstraintsHandling.isEqualsIgnoreCase(column.getAttribute("reference"), tableName)) {

              // Found a foreign key reference to our target table
              NodeList tableNodes = doc.getElementsByTagName("table");
              if (tableNodes.getLength() == 1) {
                Element tableElement = (Element) tableNodes.item(0);
                String referencingTableName = tableElement.getAttribute("name");
                String referencingColumnName = column.getAttribute("name");

                if (!PartitionedConstraintsHandling.isBlank(referencingTableName) &&
                    !PartitionedConstraintsHandling.isBlank(referencingColumnName)) {

                  String constraintName = referencingTableName + "_" + referencingColumnName + "_fkey";

                  sql.append(String.format(dropForeignKeySQL, referencingTableName, constraintName));

                  // Check if the referencing table is also partitioned
                  String checkReferencingPartition = "SELECT 1 FROM pg_partitioned_table WHERE partrelid = to_regclass(?)";
                  PreparedStatement psCheckRef = cp.getPreparedStatement(checkReferencingPartition);
                  psCheckRef.setString(1, referencingTableName);
                  boolean isReferencingPartitioned = psCheckRef.executeQuery().next();
                  psCheckRef.close();

                  if (isPartitioned && isReferencingPartitioned) {
                    // Add the partition column to the referencing table if needed
                    sql.append(String.format(addColumnSQL, referencingTableName, partitionField));
                    sql.append(String.format(updateColumnSQL, referencingTableName, partitionField,
                        partitionField, tableName, pkField, referencingTableName, referencingColumnName,
                        referencingTableName, partitionField));
                    sql.append(String.format(addPartitionedForeignKeySQL, referencingTableName, constraintName,
                        referencingColumnName, partitionField, tableName, pkField, partitionField));
                  } else {
                    sql.append(String.format(addSimpleForeignKeySQL, referencingTableName, constraintName,
                        referencingColumnName, tableName, pkField));
                  }
                }
              }
            }
          }
        } catch (Exception e) {
          log4j.warn("Warning: Could not process XML file {}: {}", sourceXmlFile.getAbsolutePath(), e.getMessage());
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
  public void executeUpdate(ConnectionProvider cp, String sql) throws Exception {
    log4j.debug("Executing SQL: {}", sql);
    try (PreparedStatement ps = cp.getPreparedStatement(sql)) {
      ps.executeUpdate();
    }
  }

  /**
   * Executes an SQL update statement and returns the number of affected rows.
   * Used for data migration operations where row count is important.
   */
  public int executeUpdateWithRowCount(ConnectionProvider cp, String sql) throws Exception {
    log4j.debug("Executing SQL: {}", sql);
    try (PreparedStatement ps = cp.getPreparedStatement(sql)) {
      return ps.executeUpdate();
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
  public void dropDependentViewsAndConstraints(ConnectionProvider cp, String tableName) throws Exception {
    // Drop views that depend on this table
    String dropViewsSql = "SELECT 'DROP VIEW IF EXISTS ' || schemaname || '.' || viewname || ' CASCADE;' " +
        "FROM pg_views v " +
        "WHERE definition ILIKE '%' || ? || '%' " +
        "AND schemaname NOT IN ('information_schema', 'pg_catalog')";

    try (PreparedStatement ps = cp.getPreparedStatement(dropViewsSql)) {
      ps.setString(1, tableName);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          String dropViewSql = rs.getString(1);
          log4j.info("Dropping dependent view: {}", dropViewSql);
          executeUpdate(cp, dropViewSql);
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
      ps.setString(1, "public." + tableName);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          String dropFkSql = rs.getString(1);
          log4j.info("Dropping foreign key: {}", dropFkSql);
          executeUpdate(cp, dropFkSql);
        }
      }
    }
  }
}
