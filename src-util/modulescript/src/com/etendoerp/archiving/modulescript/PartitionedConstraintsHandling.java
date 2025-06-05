package com.etendoerp.archiving.modulescript;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openbravo.base.model.Entity;
import org.openbravo.base.model.ModelProvider;
import org.openbravo.database.ConnectionProvider;
import org.openbravo.ddlutils.util.ModulesUtil;
import org.openbravo.modulescript.ModuleScript;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PartitionedConstraintsHandling extends ModuleScript {

  private static final Logger log4j = LogManager.getLogger();

  public static final String SRC_DB_DATABASE_MODEL_TABLES = "src-db/database/model/tables";

  public static List<File> findTableXmlFiles(String tableName) {
    List<File> foundFiles = new ArrayList<>();
    File projectDir = new File(ModulesUtil.getProjectRootDir());
    List<String> modules = new ArrayList<>(Arrays.stream(ModulesUtil.moduleDirs).toList());
    List<String> moduleDirsToCheck = new ArrayList<>();

    for (String modulePath : modules) {
      File moduleDir = new File(projectDir, modulePath);
      if (moduleDir.exists() && moduleDir.isDirectory()) {
        File[] subDirs = moduleDir.listFiles(File::isDirectory);
        if (subDirs != null) {
          for (File subDir : subDirs) {
            moduleDirsToCheck.add(subDir.getAbsolutePath());
          }
        }
      }
    }
    moduleDirsToCheck.add(ModulesUtil.getProjectRootDir() + "/");

    for (String dir : moduleDirsToCheck) {
      File xmlDir = new File(dir + SRC_DB_DATABASE_MODEL_TABLES);
      if (xmlDir.exists() && xmlDir.isDirectory()) {
        File[] files = xmlDir.listFiles();
        if (files != null) {
          for (File file : files) {
            if (file.isFile() && file.getName().equalsIgnoreCase(tableName + ".xml")) {
              foundFiles.add(file);
            }
          }
        }
      }
    }
    return foundFiles;
  }

  public static String findForeignKeyName(List<File> xmlFiles, String targetTable,
      String targetColumn) {
    try {
      for (File xml : xmlFiles) {
        if (!xml.exists()) {
          log4j.error("Error: XML file does not exist: {}", xml.getAbsolutePath());
          return null;
        }
        Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(xml);
        doc.getDocumentElement().normalize();
        NodeList fkList = doc.getElementsByTagName("foreign-key");

        for (int i = 0; i < fkList.getLength(); i++) {
          Element fkEl = (Element) fkList.item(i);
          if (targetTable.equalsIgnoreCase(fkEl.getAttribute("foreignTable"))) {
            NodeList refList = fkEl.getElementsByTagName("reference");
            for (int j = 0; j < refList.getLength(); j++) {
              Element refEl = (Element) refList.item(j);
              if (targetColumn != null && targetColumn.equalsIgnoreCase(
                  refEl.getAttribute("local"))) {
                return fkEl.getAttribute("name");
              }
            }
          }
        }
      }
    } catch (Exception e) {
      log4j.error("Error processing XML: {}", e.getMessage(), e);
    }
    return null;
  }

  public static String findPrimaryKey(List<File> xmlFiles) {
    try {
      for (File xml : xmlFiles) {
        if (!xml.exists()) {
          log4j.error("Error: XML file does not exist: {}", xml.getAbsolutePath());
          return null;
        }
        Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(xml);
        doc.getDocumentElement().normalize();
        NodeList tableList = doc.getElementsByTagName("table");

        if (tableList.getLength() == 1) {
          Element tableEl = (Element) tableList.item(0);
          if (tableEl.hasAttribute("primaryKey")) {
            return tableEl.getAttribute("primaryKey");
          } else {
            log4j.warn("Warning: Missing 'primaryKey' attribute in: {}", xml.getAbsolutePath());
            return null;
          }
        } else if (tableList.getLength() == 0) {
          log4j.error("Error: No <table> tag found in: {}", xml.getAbsolutePath());
          return null;
        } else {
          log4j.error("Error: Found {} <table> tags in: {}", tableList.getLength(),
              xml.getAbsolutePath());
          return null;
        }
      }
    } catch (Exception e) {
      log4j.error("Error processing XML: {}", e.getMessage(), e);
    }
    return null;
  }

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
        sql.append(buildConstraintSql(tableName, cp, pkColumnName, columnName));
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

  private static String buildConstraintSql(String tableName, ConnectionProvider cp, String pkField,
      String partitionField) throws Exception {
    // Check if table is partitioned
    String checkPartition = "SELECT 1 FROM pg_partitioned_table WHERE partrelid = ? ::regclass";
    PreparedStatement psCheck = cp.getPreparedStatement(checkPartition);
    psCheck.setString(1, tableName);
    boolean isPartitioned = psCheck.executeQuery().next();
    psCheck.close();

    // Get required information
    List<Entity> entities = ModelProvider.getInstance().getModel();
    List<File> tableXmlFiles = findTableXmlFiles(tableName);
    String pkName = findPrimaryKey(tableXmlFiles);

    // SQL templates for primary table
    String dropPrimaryKeySQL = "ALTER TABLE IF EXISTS PUBLIC.%s\n" + "DROP CONSTRAINT IF EXISTS %s CASCADE;\n";

    String addPartitionedPrimaryKeySQL = "ALTER TABLE IF EXISTS PUBLIC.%s\n" + "ADD CONSTRAINT %s PRIMARY KEY (%s, %s);\n";

    String addSimplePrimaryKeySQL = "ALTER TABLE IF EXISTS PUBLIC.%s\n" + "ADD CONSTRAINT %s PRIMARY KEY (%s);\n";

    // Build SQL script for primary table
    StringBuilder sql = new StringBuilder();
    sql.append(String.format(dropPrimaryKeySQL, tableName, pkName));

    if (isPartitioned) {
      sql.append(
          String.format(addPartitionedPrimaryKeySQL, tableName, pkName, pkField, partitionField));
    } else {
      sql.append(String.format(addSimplePrimaryKeySQL, tableName, pkName, pkField));
    }

    // SQL templates for foreign key constraints
    String dropForeignKeySQL = "ALTER TABLE IF EXISTS PUBLIC.%s\n" + "DROP CONSTRAINT IF EXISTS %s;\n";

    String addColumnSQL = "ALTER TABLE %s\n" + "ADD COLUMN IF NOT EXISTS %s TIMESTAMP WITHOUT TIME ZONE;\n";

    String updateColumnSQL = "UPDATE %s SET %s = F.%s FROM %s F " + "WHERE F.%s = %s.%s AND %s.%s IS NULL;\n";

    String addPartitionedForeignKeySQL = "ALTER TABLE IF EXISTS PUBLIC.%s\n" + "ADD CONSTRAINT %s FOREIGN KEY (%s, %s) " + "REFERENCES PUBLIC.%s (%s, %s) MATCH SIMPLE " + "ON UPDATE CASCADE ON DELETE NO ACTION;\n";

    String addSimpleForeignKeySQL = "ALTER TABLE IF EXISTS PUBLIC.%s\n" + "ADD CONSTRAINT %s FOREIGN KEY (%s) " + "REFERENCES PUBLIC.%s (%s) MATCH SIMPLE " + "ON UPDATE NO ACTION ON DELETE NO ACTION;\n";

    Entity targetEntity = null;
    for (Entity entity : entities) {
      if (entity.isView() || entity.isVirtualEntity())
        continue;

      if (entity.getTableName().equalsIgnoreCase(tableName)) {
        targetEntity = entity;
      }

      entity.getProperties().forEach(property -> {
        if (property.getTargetEntity() != null && property.getTargetEntity()
            .getTableName()
            .equalsIgnoreCase(tableName)) {

          Entity sourceEntity = property.getEntity();
          if (sourceEntity.isView() || sourceEntity.isVirtualEntity())
            return;

          String relatedTableName = sourceEntity.getTableName().toUpperCase();
          List<File> xmlFolders = findTableXmlFiles(relatedTableName);
          if (xmlFolders.isEmpty()) {
            return;
          }

          String relationColumn = property.getColumnName();
          String foreignKey = findForeignKeyName(xmlFolders, tableName, relationColumn);
          if (foreignKey == null) {
            return;
          }

          sql.append(String.format(dropForeignKeySQL, relatedTableName, foreignKey));

          if (isPartitioned) {
            String partitionColumn = "etarc_" + partitionField + "__" + foreignKey;
            sql.append(String.format(addColumnSQL, relatedTableName, partitionColumn));
            sql.append(
                String.format(updateColumnSQL, relatedTableName, partitionColumn, partitionField,
                    tableName, pkField, relatedTableName, relationColumn, relatedTableName,
                    partitionColumn));
            sql.append(String.format(addPartitionedForeignKeySQL, relatedTableName, foreignKey,
                relationColumn, partitionColumn, tableName, pkField, partitionField));
          } else {
            sql.append(
                String.format(addSimpleForeignKeySQL, relatedTableName, foreignKey, relationColumn,
                    tableName, pkField));
          }
        }
      });
    }

    if (targetEntity == null)
      throw new RuntimeException("Entity " + tableName + " not found in model.");

    return sql.toString();
  }
}
