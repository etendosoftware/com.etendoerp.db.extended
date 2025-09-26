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
 * Utility class for building SQL statements for partition-related operations.
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
   */
  public StringBuilder getAlterSql(com.etendoerp.db.extended.utils.TableDefinitionComparator.ColumnDiff diff,
      String tableName) {
    StringBuilder alterSql = new StringBuilder();
    for (var e : diff.added.entrySet()) {
      String col = e.getKey();
      var def = e.getValue();
      String sqlType = mapXmlTypeToSql(def.getDataType(), def.getLength()); // best-effort
      alterSql.append(String.format(
          "ALTER TABLE public.%s ADD COLUMN IF NOT EXISTS %s %s %s;",
          tableName, col, sqlType, def.isNullable() ? "" : "NOT NULL"));
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
   * Context interface for checking existence of database objects.
   */
  public interface FkContext {
    String getParentTable();

    String getPkField();

    String getPartitionField();

    boolean isParentPartitioned();

    boolean columnExists(String tableName, String columnName);

    boolean constraintExists(String tableName, String constraintName);
  }

  /**
   * Immutable holder for a single child-table FK reference.
   */
  public static final class ChildRef {
    public final String childTable;
    public final String fkName;
    public final String localCol;

    public ChildRef(String childTable, String fkName, String localCol) {
      this.childTable = childTable;
      this.fkName = fkName;
      this.localCol = localCol;
    }
  }
}