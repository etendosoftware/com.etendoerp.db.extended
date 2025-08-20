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

  public void onNew(@Observes EntityNewEvent event) {
    if (!isValidEvent(event)) {
      return;
    }

    TableConfig actualTableConfig = (TableConfig) event.getTargetInstance();
    String partitionedTable = actualTableConfig.getTable().getDBTableName();
    // Validate that the selected partition column has no NULL values
    try {
      String selectedColumn = actualTableConfig.getColumn() != null
          ? actualTableConfig.getColumn().getDBColumnName()
          : null;

      String nullCheckSql = "SELECT CASE WHEN EXISTS (SELECT 1 FROM public."
          + partitionedTable.toLowerCase()
          + " WHERE " + selectedColumn.toLowerCase() + " IS NULL) THEN 'Y' ELSE 'N' END AS hasnull";

      String hasNulls = "N";
      Connection connection = OBDal.getInstance().getConnection(false);
      try (PreparedStatement ps = connection.prepareStatement(nullCheckSql);
           ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          hasNulls = rs.getString("hasnull");
        }
      }

      if (StringUtils.equals("Y", hasNulls)) {
        throw new OBException(OBMessageUtils.messageBD("ETARC_SelectedColumnHasNulls"));
      }
    } catch (SQLException e) {
      logSQLError(e);
      throw new OBException(OBMessageUtils.messageBD(ETARC_COULD_NOT_RETRIEVE_TABLES), e);
    } catch (Exception e) {
      throw new OBException(e);
    }
    String sql =
        "SELECT " +
            "  CASE WHEN EXISTS ( " +
            "    SELECT 1 " +
            "    FROM pg_constraint con " +
            "    JOIN pg_class c ON c.oid = con.conrelid " +
            "    WHERE con.contype = 'u' " +
            "      AND c.relname = ? " +
            "  ) THEN 'Y' ELSE 'N' END AS hasunique;";

    String hasUnique = "";

    try {
      Connection connection = OBDal.getInstance().getConnection(false);
      try (PreparedStatement ps = connection.prepareStatement(sql)) {
        ps.setString(1, partitionedTable.toLowerCase());
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            hasUnique = rs.getString("hasunique");
          }
        }
      }
    } catch (SQLException e) {
      logSQLError(e);
      throw new OBException(OBMessageUtils.messageBD(ETARC_COULD_NOT_RETRIEVE_TABLES), e);
    } catch (Exception e) {
      throw new OBException(e);
    }

    if (StringUtils.equals("Y", hasUnique)) {
      throw new OBException(OBMessageUtils.messageBD("ETARC_TableHasUnique"));
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
