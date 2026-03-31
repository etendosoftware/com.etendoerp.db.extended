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

package com.etendoerp.db.extended.modulescript;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openbravo.database.ConnectionProvider;
import org.openbravo.modulescript.ModuleScript;

/**
 * Migration script that makes existing ETARC compound FKs {@code DEFERRABLE INITIALLY IMMEDIATE}.
 *
 * <p>Prior to ETP-3621, ETARC created compound foreign keys (those that reference a partitioned
 * table and include a helper column named {@code etarc_*}) as {@code NOT DEFERRABLE}. This made
 * the partition key column effectively immutable: PostgreSQL implements partition key updates as
 * an internal {@code DELETE + INSERT}, which is immediately blocked by non-deferrable constraints.
 *
 * <p>This script runs once during {@code update.database} and recreates all such non-deferrable
 * compound FKs as {@code DEFERRABLE INITIALLY IMMEDIATE}. The new setting preserves current
 * behaviour (constraints are still checked immediately by default) while allowing callers to
 * execute {@code SET CONSTRAINTS ALL DEFERRED} within a transaction when they need to update
 * the partition key.
 *
 * <p>The script is idempotent: it only touches FKs that are not yet deferrable, so re-running it
 * on an already-migrated database is a no-op.
 *
 * @author Futit Services S.L.
 * @since ETP-3621
 */
public class MakeEtarcFksDeferrable extends ModuleScript {

  private static final Logger log4j = LogManager.getLogger();

  /**
   * Finds every non-deferrable 2-column FK that:
   * <ul>
   *   <li>References a partitioned table (the FK target is in {@code pg_partitioned_table})</li>
   *   <li>Has at least one child column whose name starts with {@code etarc_} (the ETARC helper column)</li>
   * </ul>
   */
  private static final String FIND_FKS_SQL =
      "SELECT c.conname AS fk_name, "
          + "child_t.relname AS child_table, "
          + "parent_t.relname AS parent_table, "
          + "(SELECT string_agg(a.attname, ', ' ORDER BY x.ord) "
          + "   FROM unnest(c.conkey) WITH ORDINALITY AS x(num, ord) "
          + "   JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = x.num) AS child_cols, "
          + "(SELECT string_agg(a.attname, ', ' ORDER BY x.ord) "
          + "   FROM unnest(c.confkey) WITH ORDINALITY AS x(num, ord) "
          + "   JOIN pg_attribute a ON a.attrelid = c.confrelid AND a.attnum = x.num) AS parent_cols, "
          + "c.confdeltype "
          + "FROM pg_constraint c "
          + "JOIN pg_class child_t ON child_t.oid = c.conrelid "
          + "JOIN pg_class parent_t ON parent_t.oid = c.confrelid "
          + "JOIN pg_partitioned_table pt ON pt.partrelid = c.confrelid "
          + "WHERE c.contype = 'f' "
          + "  AND NOT c.condeferrable "
          + "  AND array_length(c.conkey, 1) = 2 "
          + "  AND EXISTS ("
          + "        SELECT 1 FROM pg_attribute a "
          + "        WHERE a.attrelid = c.conrelid "
          + "          AND a.attnum = ANY(c.conkey) "
          + "          AND a.attname LIKE 'etarc_%'"
          + "      )";

  @Override
  public void execute() {
    try {
      ConnectionProvider cp = getConnectionProvider();
      List<FkInfo> fks = findNonDeferrableFks(cp);

      if (fks.isEmpty()) {
        log4j.info("ETP-3621: No non-deferrable ETARC compound FKs found. Nothing to migrate.");
        return;
      }

      log4j.info("ETP-3621: Found {} non-deferrable ETARC compound FK(s) to make DEFERRABLE INITIALLY IMMEDIATE.",
          fks.size());

      StringBuilder migrationSql = new StringBuilder();
      for (FkInfo fk : fks) {
        log4j.info("ETP-3621: Migrating FK {} on table {}.", fk.fkName, fk.childTable);
        migrationSql
            .append("ALTER TABLE ").append(fk.childTable)
            .append(" DROP CONSTRAINT IF EXISTS ").append(fk.fkName).append(";\n")
            .append("ALTER TABLE ").append(fk.childTable)
            .append(" ADD CONSTRAINT ").append(fk.fkName)
            .append(" FOREIGN KEY (").append(fk.childCols).append(")")
            .append(" REFERENCES PUBLIC.").append(fk.parentTable)
            .append(" (").append(fk.parentCols).append(")")
            .append(" MATCH SIMPLE ON UPDATE CASCADE ON DELETE ").append(fk.onDeleteAction)
            .append(" DEFERRABLE INITIALLY IMMEDIATE;\n");
      }

      String sql = migrationSql.toString();
      try (PreparedStatement ps = cp.getPreparedStatement(sql)) {
        ps.executeUpdate();
      }

      log4j.info("ETP-3621: Successfully migrated {} ETARC compound FK(s) to DEFERRABLE INITIALLY IMMEDIATE.",
          fks.size());

    } catch (Exception e) {
      handleError(e);
    }
  }

  private List<FkInfo> findNonDeferrableFks(ConnectionProvider cp) throws Exception {
    List<FkInfo> result = new ArrayList<>();
    try (PreparedStatement ps = cp.getPreparedStatement(FIND_FKS_SQL);
         ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        FkInfo fk = new FkInfo();
        fk.fkName = rs.getString("fk_name");
        fk.childTable = rs.getString("child_table");
        fk.parentTable = rs.getString("parent_table");
        fk.childCols = rs.getString("child_cols");
        fk.parentCols = rs.getString("parent_cols");
        fk.onDeleteAction = mapConfDelType(rs.getString("confdeltype"));
        result.add(fk);
      }
    }
    return result;
  }

  /**
   * Converts PostgreSQL's single-char {@code confdeltype} to the SQL keyword used in DDL.
   *
   * @param confdeltype
   *     the character stored in {@code pg_constraint.confdeltype}
   * @return the corresponding SQL ON DELETE action keyword
   */
  private String mapConfDelType(String confdeltype) {
    if (confdeltype == null) {
      return "NO ACTION";
    }
    switch (confdeltype) {
      case "c": return "CASCADE";
      case "n": return "SET NULL";
      case "r": return "RESTRICT";
      case "a":
      default:  return "NO ACTION";
    }
  }

  private static final class FkInfo {
    String fkName;
    String childTable;
    String parentTable;
    String childCols;
    String parentCols;
    String onDeleteAction;
  }
}
