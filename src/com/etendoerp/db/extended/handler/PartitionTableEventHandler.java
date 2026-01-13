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

package com.etendoerp.db.extended.handler;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;
import java.util.regex.Pattern;

import javax.enterprise.event.Observes;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openbravo.base.exception.OBException;
import org.openbravo.base.model.Entity;
import org.openbravo.base.model.ModelProvider;
import org.openbravo.client.kernel.event.EntityDeleteEvent;
import org.openbravo.client.kernel.event.EntityNewEvent;
import org.openbravo.client.kernel.event.EntityPersistenceEventObserver;
import org.openbravo.dal.service.OBDal;
import org.openbravo.erpCommon.utility.OBMessageUtils;

import com.etendoerp.db.extended.data.TableConfig;

public class PartitionTableEventHandler extends EntityPersistenceEventObserver {
  private static final Logger logger = LogManager.getLogger();
  private static final Entity[] entities = { ModelProvider.getInstance().getEntity(TableConfig.ENTITY_NAME) };
  public static final String ETARC_COULD_NOT_RETRIEVE_TABLES = "ETARC_CouldNotRetrieveTables";

  private static final Pattern IDENTIFIER = Pattern.compile("^[A-Za-z_][A-Za-z0-9_]*$");
  private static final String DEFAULT_SCHEMA = "public";

  /**
   * Validates and safely quotes an SQL identifier (schema/table/column) for use in
   * dynamic SQL. The identifier must match the allowed {@code IDENTIFIER} pattern
   * (by default: {@code [A-Za-z_][A-Za-z0-9_]*}); otherwise an {@link OBException} is thrown.
   * <p>
   * The returned value is wrapped in double quotes, and any embedded double quotes
   * are escaped by doubling them, following ANSI/PostgreSQL rules.
   * <p>
   * <b>Note:</b> Use this only for identifiers. Do not use it for values; use
   * {@link java.sql.PreparedStatement} parameters instead.
   *
   * <pre>{@code
   * quoteIdent("orders");    // -> "\"orders\""
   * quoteIdent("OrderItem"); // -> "\"OrderItem\""
   * }</pre>
   *
   * @param id
   *     the identifier to validate and quote; must not be {@code null}
   * @return the quoted identifier with internal double quotes escaped
   * @throws OBException
   *     if {@code id} is {@code null} or does not match the allowed pattern
   */
  private static String quoteIdent(String id) {
    if (id == null || !IDENTIFIER.matcher(id).matches()) {
      throw new OBException("Invalid SQL identifier: " + id);
    }
    return "\"" + id.replace("\"", "\"\"") + "\"";
  }

  public void onNew(@Observes EntityNewEvent event) {
    if (!isValidEvent(event)) {
      return;
    }

    TableConfig actualTableConfig = (TableConfig) event.getTargetInstance();
    String partitionedTable = actualTableConfig.getTable().getDBTableName();

    try {
      // Get connection managed by Hibernate/Openbravo - DO NOT CLOSE IT
      Connection connection = OBDal.getInstance().getConnection(false);
      
      // Check for NULL values in the selected column
      String selectedColumn = actualTableConfig.getColumn() != null
          ? actualTableConfig.getColumn().getDBColumnName()
          : null;

      final String schema = quoteIdent(DEFAULT_SCHEMA);
      final String table = quoteIdent(partitionedTable.toLowerCase(Locale.ROOT));
      final String column = quoteIdent(selectedColumn.toLowerCase(Locale.ROOT));

      // SQL injection is prevented: quoteIdent() validates identifiers against strict regex
      // and properly escapes them before concatenation. User input is not directly used.
      final String nullCheckSql = // NOSONAR - Safe: identifiers are validated and quoted
          "SELECT CASE WHEN EXISTS (SELECT 1 FROM " + schema + "." + table +
              " WHERE " + column + " IS NULL) THEN 'Y' ELSE 'N' END AS hasnull";

      String hasNulls = "N";
      try (PreparedStatement ps = connection.prepareStatement(nullCheckSql);
           ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          hasNulls = rs.getString("hasnull");
        }
      }

      if (StringUtils.equals("Y", hasNulls)) {
        throw new OBException(OBMessageUtils.messageBD("ETARC_SelectedColumnHasNulls"));
      }

      // Check for unique constraints
      String uniqueCheckSql =
          "SELECT " +
              "  CASE WHEN EXISTS ( " +
              "    SELECT 1 " +
              "    FROM pg_constraint con " +
              "    JOIN pg_class c ON c.oid = con.conrelid " +
              "    WHERE con.contype = 'u' " +
              "      AND c.relname = ? " +
              "  ) THEN 'Y' ELSE 'N' END AS hasunique;";

      String hasUnique = "";
      try (PreparedStatement ps = connection.prepareStatement(uniqueCheckSql)) {
        ps.setString(1, partitionedTable.toLowerCase(Locale.ROOT));
        try (ResultSet rs = ps.executeQuery()) {
          if (rs.next()) {
            hasUnique = rs.getString("hasunique");
          }
        }
      }

      if (StringUtils.equals("Y", hasUnique)) {
        throw new OBException(OBMessageUtils.messageBD("ETARC_TableHasUnique"));
      }
    } catch (SQLException e) {
      logSQLError(e);
      throw new OBException(OBMessageUtils.messageBD(ETARC_COULD_NOT_RETRIEVE_TABLES), e);
    } catch (Exception e) {
      throw new OBException(e);
    }
  }

  public void onDelete(@Observes EntityDeleteEvent event) {
    if (!isValidEvent(event)) {
      return;
    }
    TableConfig actualTableConfig = (TableConfig) event.getTargetInstance();
    String partitionedTable = actualTableConfig.getTable().getDBTableName();
    String sql =
        "SELECT " +
            "    'Y' as ispartitioned " +
            "FROM " +
            "    pg_partitioned_table p " +
            "JOIN " +
            "    pg_class c ON c.oid = p.partrelid " +
            "JOIN " +
            "    pg_namespace n ON n.oid = c.relnamespace " +
            "WHERE " +
            "    c.relkind = 'p' and c.relname = ?;";

    String isPartitioned = "";
    try {
      Connection connection = OBDal.getInstance().getConnection(true);
      try (PreparedStatement ps = connection.prepareStatement(sql)) {
        ps.setString(1, partitionedTable.toLowerCase());
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            isPartitioned = rs.getString("ispartitioned");
          }
        }
      }
    } catch (SQLException e) {
      logSQLError(e);
      throw new OBException(OBMessageUtils.messageBD(ETARC_COULD_NOT_RETRIEVE_TABLES), e);
    }
    if (StringUtils.equals("Y", isPartitioned)) {
      throw new OBException(OBMessageUtils.messageBD("ETARC_DeleteAlreadyPartTable"));
    }
  }

  private static void logSQLError(SQLException e) {
    logger.error("Error executing SQL query. Message: {}, SQLState: {}, ErrorCode: {}",
        e.getMessage(), e.getSQLState(), e.getErrorCode());
  }

  @Override
  protected Entity[] getObservedEntities() {
    return entities;
  }
}
