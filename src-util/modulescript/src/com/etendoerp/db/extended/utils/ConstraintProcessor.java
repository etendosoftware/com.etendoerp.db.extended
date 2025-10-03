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

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openbravo.database.ConnectionProvider;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.etendoerp.db.extended.utils.ConstraintProcessingException.DatabaseConstraintException;
import com.etendoerp.db.extended.utils.ConstraintProcessingException.PrimaryKeyNotFoundException;
import com.etendoerp.db.extended.utils.ConstraintProcessingException.TableXmlNotFoundException;

/**
 * Orchestrates the complex processing of database constraints for partitioned tables.
 *
 * <p>This class serves as the central coordinator for constraint management in PostgreSQL
 * partitioned tables. It combines table analysis, XML processing, and SQL generation to
 * handle the intricate requirements of maintaining referential integrity in partitioned
 * table environments.
 *
 * <h3>Core Responsibilities:</h3>
 * <ul>
 *   <li><strong>Constraint Analysis:</strong> Determines current constraint states and requirements</li>
 *   <li><strong>Partitioning Detection:</strong> Identifies whether tables are partitioned in PostgreSQL</li>
 *   <li><strong>SQL Orchestration:</strong> Coordinates SQL generation for constraint recreation</li>
 *   <li><strong>Foreign Key Discovery:</strong> Finds child tables that reference the target table</li>
 *   <li><strong>Primary Key Management:</strong> Handles primary key constraint modifications</li>
 *   <li><strong>Trigger Integration:</strong> Incorporates trigger management into constraint processing</li>
 * </ul>
 *
 * <h3>PostgreSQL Partitioning Challenges:</h3>
 * <p>Partitioned tables in PostgreSQL have unique constraint requirements:
 * <ul>
 *   <li><strong>Primary Keys:</strong> Must include the partition key in addition to logical primary key</li>
 *   <li><strong>Foreign Keys:</strong> Must reference all columns in the target table's primary key</li>
 *   <li><strong>Uniqueness:</strong> Unique constraints must include the partition key</li>
 *   <li><strong>Inheritance:</strong> Child tables inherit certain constraints from parent tables</li>
 * </ul>
 *
 * <h3>Processing Strategy:</h3>
 * <ol>
 *   <li><strong>Analysis Phase:</strong> Determine table partitioning status and current constraints</li>
 *   <li><strong>Discovery Phase:</strong> Find all child tables that reference the target table</li>
 *   <li><strong>Planning Phase:</strong> Determine which constraints need to be dropped/recreated</li>
 *   <li><strong>Execution Phase:</strong> Generate SQL for constraint modifications</li>
 *   <li><strong>Integration Phase:</strong> Include trigger management as needed</li>
 * </ol>
 *
 * <h3>Component Integration:</h3>
 * <p>This processor coordinates with several other components:
 * <ul>
 *   <li>{@link XmlTableProcessor} - For XML table definition analysis</li>
 *   <li>{@link SqlBuilder} - For SQL statement generation</li>
 *   <li>{@link TriggerManager} - For database trigger management</li>
 * </ul>
 *
 * <h3>Error Handling:</h3>
 * <p>The processor implements defensive programming practices:
 * <ul>
 *   <li>Safe existence checks that don't fail on missing objects</li>
 *   <li>Graceful degradation when optional operations fail</li>
 *   <li>Detailed logging for debugging and monitoring</li>
 * </ul>
 *
 * <h3>Usage Example:</h3>
 * <pre>{@code
 * ConstraintProcessor processor = new ConstraintProcessor(
 *     xmlProcessor, sqlBuilder, triggerManager);
 *
 * // Check if table is partitioned
 * boolean isPartitioned = processor.isTablePartitioned(cp, "C_Order");
 *
 * // Generate constraint SQL
 * String sql = processor.buildConstraintSql("C_Order", cp, "C_Order_ID", "DateOrdered");
 *
 * // Get primary key columns
 * List<String> pkCols = processor.getPrimaryKeyColumns(cp, "C_Order");
 * }</pre>
 *
 * @author Futit Services S.L.
 * @see XmlTableProcessor
 * @see SqlBuilder
 * @see TriggerManager
 * @see com.etendoerp.db.extended.modulescript.PartitionedConstraintsHandling
 * @since ETP-2450
 */
public class ConstraintProcessor {
  private static final Logger log4j = LogManager.getLogger();
  private static final String FOREIGN_KEY = "foreign-key";

  private final XmlTableProcessor xmlProcessor;
  private final SqlBuilder sqlBuilder;
  private final TriggerManager triggerManager;

  /**
   * Constructs a new ConstraintProcessor with the specified dependencies.
   *
   * @param xmlProcessor
   *     the XmlTableProcessor for processing table definitions
   * @param sqlBuilder
   *     the SqlBuilder for generating SQL statements
   * @param triggerManager
   *     the TriggerManager for handling database triggers
   */
  public ConstraintProcessor(XmlTableProcessor xmlProcessor, SqlBuilder sqlBuilder, TriggerManager triggerManager) {
    this.xmlProcessor = xmlProcessor;
    this.sqlBuilder = sqlBuilder;
    this.triggerManager = triggerManager;
  }

  /**
   * Builds SQL to (re)create PK/FK constraints for {@code tableName}, handling partitioned vs non-partitioned.
   *
   * @param tableName
   *     the table name to build constraints for
   * @param cp
   *     the ConnectionProvider for database access
   * @param pkField
   *     the primary key field name
   * @param partitionField
   *     the partition field name (if applicable)
   * @return the complete SQL string for constraint creation
   */
  public String buildConstraintSql(String tableName, ConnectionProvider cp, String pkField,
      String partitionField) throws ConstraintProcessingException {

    boolean isPartitioned = isPartitioned(cp, tableName);
    String pkName = resolvePrimaryKeyName(tableName); // throws if not found

    StringBuilder sql = new StringBuilder(512);
    sqlBuilder.appendPrimaryTableSql(sql, tableName, pkName, pkField, partitionField, isPartitioned);

    FkContextImpl ctx = new FkContextImpl(cp, tableName, pkField, partitionField, isPartitioned);
    appendAllForeignKeySql(sql, ctx);

    // Add trigger creation for partitioned tables
    if (isPartitioned) {
      log4j.info("Creating triggers for partitioned table {}", tableName);
      triggerManager.appendTriggerSql(sql, ctx);
    }

    return sql.toString();
  }

  /**
   * True if the table is partitioned in PostgreSQL.
   *
   * @param cp
   *     the ConnectionProvider for database access
   * @param tableName
   *     the table name to check for partitioning
   * @return true if the table is partitioned, false otherwise
   * @throws ConstraintProcessingException
   *     if database query fails
   */
  public boolean isTablePartitioned(ConnectionProvider cp, String tableName) throws ConstraintProcessingException {
    try (PreparedStatement ps = cp.getPreparedStatement(
        "SELECT 1 FROM pg_partitioned_table WHERE partrelid = to_regclass(?)")) {
      ps.setString(1, tableName);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    } catch (Exception e) {
      throw new DatabaseConstraintException("Failed to check if table is partitioned: " + tableName, e);
    }
  }

  /**
   * Returns the list of PK columns for the given table.
   *
   * @param cp
   *     the ConnectionProvider for database access
   * @param tableName
   *     the table name to get primary key columns for
   * @return a list of primary key column names
   * @throws ConstraintProcessingException
   *     if database query fails
   */
  public List<String> getPrimaryKeyColumns(ConnectionProvider cp, String tableName) throws ConstraintProcessingException {
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
    } catch (Exception e) {
      throw new DatabaseConstraintException("Failed to get primary key columns for table: " + tableName, e);
    }
    return pkCols;
  }

  /**
   * True if a column exists on the given table.
   *
   * @param cp
   *     the ConnectionProvider for database access
   * @param tableName
   *     the table name to check for column existence
   * @param columnName
   *     the column name to check for existence
   * @return true if the column exists, false otherwise
   * @throws ConstraintProcessingException
   *     if database query fails
   */
  public boolean columnExists(ConnectionProvider cp, String tableName, String columnName) throws ConstraintProcessingException {
    String sql = "SELECT 1 FROM information_schema.columns " +
        "WHERE lower(table_name) = lower(?) AND lower(column_name) = lower(?)";
    try (PreparedStatement ps = cp.getPreparedStatement(sql)) {
      ps.setString(1, tableName);
      ps.setString(2, columnName);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    } catch (Exception e) {
      throw new DatabaseConstraintException("Failed to check column existence: " + columnName + " in table: " + tableName, e);
    }
  }

  /**
   * True if a constraint with the given name exists on the table.
   *
   * @param cp
   *     the ConnectionProvider for database access
   * @param tableName
   *     the table name to check for constraint existence
   * @param constraintName
   *     the constraint name to check for existence
   * @return true if the constraint exists, false otherwise
   * @throws ConstraintProcessingException
   *     if database query fails
   */
  public boolean constraintExists(ConnectionProvider cp, String tableName, String constraintName) throws ConstraintProcessingException {
    String sql = "SELECT 1 FROM information_schema.table_constraints " +
        "WHERE lower(table_name) = lower(?) AND lower(constraint_name) = lower(?)";
    try (PreparedStatement ps = cp.getPreparedStatement(sql)) {
      ps.setString(1, tableName);
      ps.setString(2, constraintName);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    } catch (Exception e) {
      throw new DatabaseConstraintException("Failed to check constraint existence: " + constraintName + " in table: " + tableName, e);
    }
  }

  private boolean isPartitioned(ConnectionProvider cp, String tableName) {
    String sql = "SELECT 1 FROM pg_partitioned_table WHERE partrelid = to_regclass(?)";
    try (PreparedStatement ps = cp.getPreparedStatement(sql)) {
      ps.setString(1, tableName);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    } catch (Exception e) {
      log4j.warn("Could not check partitioning for {}: {}", tableName, e.getMessage());
      return false;
    }
  }

  private String resolvePrimaryKeyName(String tableName) throws ConstraintProcessingException {
    List<File> xmls = xmlProcessor.findTableXmlFiles(tableName);
    if (xmls.isEmpty()) {
      throw new TableXmlNotFoundException(tableName);
    }
    String pkName = XmlTableProcessor.findPrimaryKey(xmls);
    if (pkName == null) {
      throw new PrimaryKeyNotFoundException(tableName);
    }
    return pkName;
  }

  private void appendAllForeignKeySql(StringBuilder sql, FkContextImpl ctx) {
    for (File dir : xmlProcessor.collectTableDirsSafe()) {
      for (File xml : xmlProcessor.listXmlFiles(dir)) {
        processXmlForForeignKeys(sql, ctx, xml);
      }
    }
  }

  private void processXmlForForeignKeys(StringBuilder sql, FkContextImpl ctx, File xml) {
    try {
      Document doc = XmlTableProcessor.getDocument(xml);
      Element tableEl = xmlProcessor.singleTableElementOrNull(doc);
      if (tableEl == null || xmlProcessor.shouldSkipTableElement(tableEl, ctx.getParentTable())) return;

      String childTable = tableEl.getAttribute("name").toUpperCase();
      NodeList fkList = tableEl.getElementsByTagName(FOREIGN_KEY);

      for (int i = 0; i < fkList.getLength(); i++) {
        Element fkEl = (Element) fkList.item(i);

        String fkName = fkEl.getAttribute("name");
        String localCol = xmlProcessor.firstLocalColumn(fkEl);

        boolean validReference = xmlProcessor.referencesTarget(fkEl, ctx.getParentTable());
        boolean validData = StringUtils.isNotBlank(fkName) && StringUtils.isNotBlank(localCol);

        if (validReference && validData) {
          sqlBuilder.appendFkSqlForChild(sql, ctx, new SqlBuilder.ChildRef(childTable, fkName, localCol));
        }
      }
    } catch (Exception e) {
      log4j.error("Error processing XML file: {}", xml.getAbsolutePath(), e);
    }
  }

  @FunctionalInterface
  private interface Check {
    boolean get() throws ConstraintProcessingException;
  }

  /**
   * Implementation of FkContext for SQL building.
   */
  private class FkContextImpl implements SqlBuilder.FkContext {
    private final ConnectionProvider cp;
    private final String parentTable;
    private final String pkField;
    private final String partitionField;
    private final boolean parentIsPartitioned;

    FkContextImpl(ConnectionProvider cp, String parentTable, String pkField,
        String partitionField, boolean parentIsPartitioned) {
      this.cp = cp;
      this.parentTable = parentTable;
      this.pkField = pkField;
      this.partitionField = partitionField;
      this.parentIsPartitioned = parentIsPartitioned;
    }

    @Override
    public String getParentTable() {
      return parentTable;
    }

    @Override
    public String getPkField() {
      return pkField;
    }

    @Override
    public String getPartitionField() {
      return partitionField;
    }

    @Override
    public boolean isParentPartitioned() {
      return parentIsPartitioned;
    }

    @Override
    public boolean columnExists(String tableName, String columnName) {
      return existsSafe(() -> {
            try {
              return ConstraintProcessor.this.columnExists(cp, tableName,
                  columnName);
            } catch (Exception e) {
              throw new DatabaseConstraintException(e);
            }
          },
          "columnExists", columnName, tableName);
    }

    @Override
    public boolean constraintExists(String tableName, String constraintName) {
      return existsSafe(() -> {
            try {
              return ConstraintProcessor.this.constraintExists(cp, tableName, constraintName);
            } catch (Exception e) {
              throw new DatabaseConstraintException(e);
            }
          },
          "constraintExists", constraintName, tableName);
    }

    /**
     * Wraps boolean existence checks that may throw; logs and returns {@code false} on failure.
     */
    private boolean existsSafe(Check op, String label, String name, String onTable) {
      try {
        return op.get();
      } catch (ConstraintProcessingException e) {
        log4j.warn("Could not run {} for {} on {}: {}", label, name, onTable, e.getMessage());
        return false;
      }
    }
  }
}
