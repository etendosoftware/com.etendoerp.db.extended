package com.etendoerp.archiving.modulescript;

import com.etendoerp.archiving.utils.ArchivingPreBuildUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openbravo.database.ConnectionProvider;
import org.openbravo.modulescript.ModuleScript;

import java.io.File;
import java.sql.PreparedStatement;

public class PartitionedConstraintsHandling extends ModuleScript {

  private static final Logger log4j = LogManager.getLogger();

  public void execute() {
    try {
      ConnectionProvider cp = getConnectionProvider();
      String configSql = "SELECT UPPER(TBL.TABLENAME) TABLENAME, UPPER(COL.COLUMNNAME) COLUMNNAME, UPPER(COL_PK.COLUMNNAME) PK_COLUMNNAME " + "FROM ETARC_TABLE_CONFIG CFG " + "JOIN AD_TABLE TBL ON TBL.AD_TABLE_ID = CFG.AD_TABLE_ID " + "JOIN AD_COLUMN COL ON COL.AD_COLUMN_ID = CFG.AD_COLUMN_ID " + "JOIN AD_COLUMN COL_PK ON COL_PK.AD_TABLE_ID = TBL.AD_TABLE_ID AND COL_PK.ISKEY = 'Y'";
      PreparedStatement configPs = cp.getPreparedStatement(configSql);
      java.sql.ResultSet rs = configPs.executeQuery();
      StringBuilder sql = new StringBuilder();

      while (rs.next()) {
        String tableName = rs.getString("TABLENAME");
        String columnName = rs.getString("COLUMNNAME");
        String pkColumnName = rs.getString("PK_COLUMNNAME");
        sql.append(ArchivingPreBuildUtils.buildConstraintSql(tableName, cp, pkColumnName, columnName));
      }

      File outFile = new File("/tmp/PartitionedConstraintsHandling.sql");
      outFile.createNewFile();
      java.nio.file.Files.writeString(outFile.toPath(), sql.toString());
      PreparedStatement ps = cp.getPreparedStatement(sql.toString());
      ps.executeUpdate();
    } catch (Exception e) {
      handleError(e);
    }
  }
}
