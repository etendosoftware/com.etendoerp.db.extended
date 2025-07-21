package com.etendoerp.db.extended.handler;

import com.etendoerp.db.extended.data.TableConfig;
import org.apache.commons.lang3.StringUtils;
import org.openbravo.base.exception.OBException;
import org.openbravo.base.model.Entity;
import org.openbravo.base.model.ModelProvider;
import org.openbravo.client.kernel.event.EntityDeleteEvent;
import org.openbravo.client.kernel.event.EntityNewEvent;
import org.openbravo.client.kernel.event.EntityPersistenceEventObserver;
import org.openbravo.dal.service.OBDal;
import org.openbravo.erpCommon.utility.OBMessageUtils;

import javax.enterprise.event.Observes;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class PartitionTableEventHandler extends EntityPersistenceEventObserver {
  private static final Entity[] entities = { ModelProvider.getInstance().getEntity(TableConfig.ENTITY_NAME) };

  public void onNew(@Observes EntityNewEvent event) {
    if (!isValidEvent(event)) {
      return;
    }

    TableConfig actualTableConfig = (TableConfig) event.getTargetInstance();
    String partitionedTable = actualTableConfig.getTable().getDBTableName();
    String sql = """
      SELECT
        CASE WHEN EXISTS (
          SELECT 1
          FROM pg_constraint con
          JOIN pg_class c ON c.oid = con.conrelid
          WHERE con.contype = 'u'
            AND c.relname = ?
        ) THEN 'Y' ELSE 'N' END AS hasunique;
    """;
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
      throw new OBException(OBMessageUtils.messageBD("ETARC_CouldNotRetrieveTables"), e);
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
    String sql = """
        SELECT
            'Y' as ispartitioned
        FROM
            pg_partitioned_table p
        JOIN
            pg_class c ON c.oid = p.partrelid
        JOIN
            pg_namespace n ON n.oid = c.relnamespace
        WHERE
            c.relkind = 'p' and c.relname = ?;
        """;
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
      throw new OBException(OBMessageUtils.messageBD("ETARC_CouldNotRetrieveTables"), e);
    }
    if (StringUtils.equals("Y", isPartitioned)) {
      throw new OBException(OBMessageUtils.messageBD("ETARC_DeleteAlreadyPartTable"));
    }
  }

    @Override
  protected Entity[] getObservedEntities() { return entities; }
}
