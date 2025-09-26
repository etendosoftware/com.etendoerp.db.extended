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

import org.openbravo.base.exception.OBException;

/**
 * Processes constraints (primary keys and foreign keys) for partitioned tables.
 */
public class ConstraintProcessor {
  private static final Logger log4j = LogManager.getLogger();
  private static final String FOREIGN_KEY = "foreign-key";

  private final XmlTableProcessor xmlProcessor;
  private final SqlBuilder sqlBuilder;
  private final TriggerManager triggerManager;

  public ConstraintProcessor(XmlTableProcessor xmlProcessor, SqlBuilder sqlBuilder, TriggerManager triggerManager) {
    this.xmlProcessor = xmlProcessor;
    this.sqlBuilder = sqlBuilder;
    this.triggerManager = triggerManager;
  }

  /**
   * Builds SQL to (re)create PK/FK constraints for {@code tableName}, handling partitioned vs non-partitioned.
   */
  public String buildConstraintSql(String tableName, ConnectionProvider cp, String pkField,
      String partitionField) {

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
   * Returns the list of PK columns for the given table.
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
   * True if a column exists on the given table.
   */
  public boolean columnExists(ConnectionProvider cp, String tableName, String columnName) throws Exception {
    String sql = "SELECT 1 FROM information_schema.columns " +
        "WHERE lower(table_name) = lower(?) AND lower(column_name) = lower(?)";
    try (PreparedStatement ps = cp.getPreparedStatement(sql)) {
      ps.setString(1, tableName);
      ps.setString(2, columnName);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    }
  }

  /**
   * True if a constraint with the given name exists on the table.
   */
  public boolean constraintExists(ConnectionProvider cp, String tableName, String constraintName) throws Exception {
    String sql = "SELECT 1 FROM information_schema.table_constraints " +
        "WHERE lower(table_name) = lower(?) AND lower(constraint_name) = lower(?)";
    try (PreparedStatement ps = cp.getPreparedStatement(sql)) {
      ps.setString(1, tableName);
      ps.setString(2, constraintName);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
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

  private String resolvePrimaryKeyName(String tableName) {
    List<File> xmls = xmlProcessor.findTableXmlFiles(tableName);
    if (xmls.isEmpty()) {
      throw new OBException("Entity XML file for " + tableName + " not found.");
    }
    String pkName = XmlTableProcessor.findPrimaryKey(xmls);
    if (pkName == null) {
      throw new OBException("Primary Key for entity " + tableName + " not found in XML.");
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
    boolean get() throws OBException;
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
              throw new OBException(e);
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
              throw new OBException(e);
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
      } catch (Exception e) {
        log4j.warn("Could not run {} for {} on {}: {}", label, name, onTable, e.getMessage());
        return false;
      }
    }
  }
}