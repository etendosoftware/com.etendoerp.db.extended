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

/**
 * Constructs SQL statements for database schema modifications related to partitioned tables.
 * 
 * <p>This utility class generates the complex SQL statements needed to manage database
 * constraints when working with PostgreSQL table partitioning. It handles the intricate
 * differences between partitioned and non-partitioned table constraints, particularly
 * around primary keys and foreign keys.
 * 
 * <h3>Key Capabilities:</h3>
 * <ul>
 *   <li><strong>Primary Key Management:</strong> Generates SQL for dropping and recreating primary keys</li>
 *   <li><strong>Foreign Key Handling:</strong> Constructs foreign key constraints for child tables</li>
 *   <li><strong>Partition-Aware Logic:</strong> Adjusts SQL generation based on table partitioning status</li>
 *   <li><strong>Helper Column Management:</strong> Creates and populates timestamp helper columns</li>
 *   <li><strong>ALTER Statement Generation:</strong> Builds table alteration statements from column diffs</li>
 * </ul>
 * 
 * <h3>Partitioned vs Non-Partitioned Differences:</h3>
 * <p>PostgreSQL partitioned tables have specific constraint requirements:
 * <ul>
 *   <li><strong>Partitioned PK:</strong> Must include both the natural PK and the partition key</li>
 *   <li><strong>Non-Partitioned PK:</strong> Uses only the natural primary key</li>
 *   <li><strong>Foreign Keys:</strong> Must reference all columns in the target table's primary key</li>
 * </ul>
 * 
 * <h3>SQL Template Strategy:</h3>
 * <p>The class uses parameterized SQL templates to ensure consistent and safe SQL generation:
 * <ul>
 *   <li>Templates use {@code String.format()} placeholders for type-safe parameter injection</li>
 *   <li>All templates include proper CASCADE and constraint existence checks</li>
 *   <li>Foreign key templates include appropriate referential actions</li>
 * </ul>
 * 
 * <h3>Context Interface:</h3>
 * <p>The {@link FkContext} interface abstracts the data access layer, allowing the SQL builder
 * to query for table and constraint information without direct database coupling.
 * 
 * <h3>Usage Example:</h3>
 * <pre>{@code
 * SqlBuilder builder = new SqlBuilder();
 * StringBuilder sql = new StringBuilder();
 * 
 * // Build primary key SQL
 * builder.appendPrimaryTableSql(sql, "C_Order", "C_Order_Key", "C_Order_ID", "DateOrdered", true);
 * 
 * // Build foreign key SQL for child tables
 * FkContext context = new MyFkContext(connectionProvider);
 * ChildRef child = new ChildRef("C_OrderLine", "C_Order_ID", "C_OrderLine_Order");
 * builder.appendFkSqlForChild(sql, context, child);
 * }</pre>
 * 
 * @author Futit Services S.L.
 * @since ETP-2450
 * @see ConstraintProcessor
 * @see TableDefinitionComparator
 */
public class SqlBuilder {

  // SQL templates
  private static final String DROP_PK =
      "ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s CASCADE;\n";
  private static final String ADD_PK_PARTITIONED =
      "ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY (%s, %s);\n";
  private static final String ADD_PK_SIMPLE =
      "ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY (%s);\n";
  private static final String DROP_FK =
      "ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s;\n";
  private static final String ADD_COL_IF_NOT_EXISTS =
      "ALTER TABLE %s\nADD COLUMN IF NOT EXISTS %s TIMESTAMP WITHOUT TIME ZONE;\n";
  private static final String UPDATE_HELPER_COL =
      "UPDATE %s SET %s = F.%s FROM %s F WHERE F.%s = %s.%s AND %s.%s IS NULL;\n";
  private static final String ADD_FK_PARTITIONED =
      "ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s, %s) " +
          "REFERENCES PUBLIC.%s (%s, %s) MATCH SIMPLE ON UPDATE CASCADE ON DELETE NO ACTION;\n";
  private static final String ADD_FK_SIMPLE =
      "ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) " +
          "REFERENCES PUBLIC.%s (%s) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION;\n";

  /**
   * Appends DROP/ADD PK for the target table.
   * 
   * @param sql the StringBuilder to append SQL statements to
   * @param tableName the table name to modify primary key for
   * @param pkName the primary key constraint name
   * @param pkField the primary key field name
   * @param partitionField the partition field name (if applicable)
   * @param isPartitioned whether the table is partitioned
   */
  public void appendPrimaryTableSql(StringBuilder sql, String tableName, String pkName,
      String pkField, String partitionField, boolean isPartitioned) {
    sql.append(String.format(DROP_PK, tableName, pkName));
    if (isPartitioned) {
      sql.append(String.format(ADD_PK_PARTITIONED, tableName, pkName, pkField, partitionField));
    } else {
      sql.append(String.format(ADD_PK_SIMPLE, tableName, pkName, pkField));
    }
  }

  /**
   * Appends SQL for a single child table referencing the target.
   * 
   * @param sql the StringBuilder to append SQL statements to
   * @param ctx the foreign key context containing parent table information
   * @param child the child table reference information
   */
  public void appendFkSqlForChild(StringBuilder sql, FkContext ctx, ChildRef child) {
    if (ctx.isParentPartitioned()) {
      String helperCol = "etarc_" + ctx.getPartitionField() + "__" + child.fkName;

      boolean colExists = ctx.columnExists(child.childTable, helperCol);
      boolean fkExists = ctx.constraintExists(child.childTable, child.fkName);

      if (colExists && fkExists) {
        return; // Skip if both already exist
      }
      if (!colExists) {
        sql.append(String.format(ADD_COL_IF_NOT_EXISTS, child.childTable, helperCol));
        sql.append(String.format(UPDATE_HELPER_COL, child.childTable, helperCol,
            ctx.getPartitionField(), ctx.getParentTable(), ctx.getPkField(),
            child.childTable, child.localCol, child.childTable, helperCol));
      }
      if (!fkExists) {
        sql.append(String.format(DROP_FK, child.childTable, child.fkName));
        sql.append(String.format(ADD_FK_PARTITIONED, child.childTable, child.fkName, child.localCol,
            helperCol, ctx.getParentTable(), ctx.getPkField(), ctx.getPartitionField()));
      }
    } else {
      sql.append(String.format(DROP_FK, child.childTable, child.fkName));
      sql.append(String.format(ADD_FK_SIMPLE, child.childTable, child.fkName,
          child.localCol, ctx.getParentTable(), ctx.getPkField()));
    }
  }

  /**
   * Builds ALTER statements from XML diff: adds missing columns and drops removed ones.
   * 
   * @param diff the column differences to process
   * @param tableName the table name to generate ALTER statements for
   * @return a StringBuilder containing the ALTER SQL statements
   */
  public StringBuilder getAlterSql(com.etendoerp.db.extended.utils.TableDefinitionComparator.ColumnDiff diff,
      String tableName) {
    StringBuilder alterSql = new StringBuilder();
    for (var e : diff.added.entrySet()) {
      String col = e.getKey();
      var def = e.getValue();
      String sqlType = mapXmlTypeToSql(def.getDataType(), def.getLength()); // best-effort
      String nullConstraint = def.isNullable() ? "" : "NOT NULL";
      alterSql.append(String.format(
          "ALTER TABLE public.%s ADD COLUMN IF NOT EXISTS %s %s %s;",
          tableName, col, sqlType, nullConstraint));
    }
    for (var e : diff.removed.entrySet()) {
      String col = e.getKey();
      alterSql.append(String.format(
          "ALTER TABLE public.%s DROP COLUMN IF EXISTS %s CASCADE;",
          tableName, col));
    }
    return alterSql;
  }

  /**
   * Best-effort mapping from XML datatype to PostgreSQL type.
   * 
   * @param xmlType the XML data type to convert
   * @param length the length specification for the type (if applicable)
   * @return the corresponding PostgreSQL data type
   */
  public String mapXmlTypeToSql(String xmlType, Integer length) {
    if (xmlType == null) {
      return "text";
    }
    String t = xmlType.toLowerCase();
    switch (t) {
      case "varchar":
      case "character varying":
      case "string":
        return (length != null && length > 0) ? "varchar(" + length + ")" : "text";
      case "int":
      case "integer":
        return "integer";
      case "bigint":
        return "bigint";
      case "timestamp":
      case "datetime":
        return "timestamp without time zone";
      case "boolean":
      case "bool":
        return "boolean";
      case "numeric":
      case "decimal":
        return "numeric";
      default:
        return t;
    }
  }

  /**
   * Context interface for checking existence of database objects and providing FK context information.
   */
  public interface FkContext {
    /**
     * Gets the parent table name for foreign key relationships.
     * 
     * @return the parent table name
     */
    String getParentTable();

    /**
     * Gets the primary key field name of the parent table.
     * 
     * @return the primary key field name
     */
    String getPkField();

    /**
     * Gets the partition field name used for table partitioning.
     * 
     * @return the partition field name, or null if not partitioned
     */
    String getPartitionField();

    /**
     * Checks if the parent table is partitioned.
     * 
     * @return true if the parent table is partitioned, false otherwise
     */
    boolean isParentPartitioned();

    /**
     * Checks if a column exists in the specified table.
     * 
     * @param tableName the table name to check
     * @param columnName the column name to check for existence
     * @return true if the column exists, false otherwise
     */
    boolean columnExists(String tableName, String columnName);

    /**
     * Checks if a constraint exists in the specified table.
     * 
     * @param tableName the table name to check
     * @param constraintName the constraint name to check for existence
     * @return true if the constraint exists, false otherwise
     */
    boolean constraintExists(String tableName, String constraintName);
  }

  /**
   * Immutable holder for a single child-table FK reference.
   */
  public static final class ChildRef {
    /** The child table name that contains the foreign key. */
    public final String childTable;
    /** The foreign key constraint name. */
    public final String fkName;
    /** The local column name in the child table. */
    public final String localCol;

    /**
     * Constructs a new ChildRef with the specified child table, foreign key name, and local column.
     * 
     * @param childTable the child table name
     * @param fkName the foreign key constraint name
     * @param localCol the local column name
     */
    public ChildRef(String childTable, String fkName, String localCol) {
      this.childTable = childTable;
      this.fkName = fkName;
      this.localCol = localCol;
    }
  }
}