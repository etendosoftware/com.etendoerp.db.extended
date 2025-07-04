package com.etendoerp.db.extended.handler;

import com.etendoerp.db.extended.data.TableConfig;
import org.apache.commons.lang3.StringUtils;
import org.openbravo.base.exception.OBException;
import org.openbravo.base.model.Entity;
import org.openbravo.base.model.ModelProvider;
import org.openbravo.client.kernel.event.EntityDeleteEvent;
import org.openbravo.client.kernel.event.EntityPersistenceEventObserver;
import org.openbravo.dal.service.OBDal;
import org.quartz.SchedulerException;

import javax.enterprise.event.Observes;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class PartitionTableEventHandler extends EntityPersistenceEventObserver {
  private static final Entity[] entities = { ModelProvider.getInstance().getEntity(TableConfig.ENTITY_NAME) };

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
      PreparedStatement ps = connection.prepareStatement(sql);
      ps.setString(1, partitionedTable.toLowerCase());
      ResultSet rs = ps.executeQuery();
      while (rs.next()) {
        isPartitioned = rs.getString("ispartitioned");
      }
    } catch (SQLException e) {
      throw new OBException("Couldn't retrieve partitiond tables from database", e);
    }
    if (StringUtils.equals("Y", isPartitioned)) {
      throw new OBException("Couldn't delete an already partitioned table ");
    }
  }

    @Override
  protected Entity[] getObservedEntities() { return entities; }
}
