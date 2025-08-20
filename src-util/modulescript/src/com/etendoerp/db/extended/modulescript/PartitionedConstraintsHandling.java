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

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openbravo.database.ConnectionProvider;
import org.openbravo.modulescript.ModuleScript;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.etendoerp.db.extended.utils.TableDefinitionComparator;

public class PartitionedConstraintsHandling extends ModuleScript {

  private static final String SRC_DB_DATABASE_MODEL_TABLES = "src-db/database/model/tables";
  private static final String SRC_DB_DATABASE_MODEL_MODIFIED_TABLES = "src-db/database/model/modifiedTables";
  public static final String ALTER_TABLE = "ALTER TABLE IF EXISTS PUBLIC.%s\n";
  private static final Logger log4j = LogManager.getLogger();
  public static final String MODULES_JAR = "build/etendo/modules";
  public static final String MODULES_BASE = "modules";
  public static final String MODULES_CORE = "modules_core";
  private static final String[] moduleDirs = new String[]{ MODULES_BASE, MODULES_CORE, MODULES_JAR };
  public static final String SEPARATOR = "=======================================================";

  public static boolean isBlank(String str) {
    return StringUtils.isBlank(str);
  }

  public static boolean isEqualsIgnoreCase(String str1, String str2) {
    return str1 != null && str1.equalsIgnoreCase(str2);
  }

  public void execute() {
    try {
      ConnectionProvider cp = getConnectionProvider();
      List<Map<String, String>> tableConfigs = loadTableConfigs(cp);

      if (!tableConfigs.isEmpty()) {
        logSeparator();
        log4j.info("============== Partitioning process info ==============");
        logSeparator();
      }
      StringBuilder sql = new StringBuilder();
      for (Map<String, String> cfg : tableConfigs) {
        processTableConfig(cp, cfg, sql);
      }
      if (!tableConfigs.isEmpty()) {
        logSeparator();
      }
      executeConstraintSqlIfNeeded(cp, sql.toString());

    } catch (Exception e) {
      handleError(e);
    }
  }

  /**
   * Logs a separator line to the application log using the info level.
   * <p>
   * This method is typically used to visually separate sections in the log output,
   * improving readability during debugging or tracing execution flow.
   */
  private static void logSeparator() {
    log4j.info(SEPARATOR);
  }

  /**
   * Loads the table configuration from the `ETARC_TABLE_CONFIG` table.
   * This includes the table name, the partition column, and the primary key column.
   *
   * @param cp
   *     the connection provider for accessing the database.
   * @return a list of maps, where each map contains the keys:
   *     "tableName", "columnName", and "pkColumnName".
   * @throws Exception
   *     if a database access error occurs.
   */
  private List<Map<String, String>> loadTableConfigs(ConnectionProvider cp) throws Exception {
    String configSql = "SELECT UPPER(TBL.TABLENAME) TABLENAME, "
        + "UPPER(COL.COLUMNNAME) COLUMNNAME, "
        + "UPPER(COL_PK.COLUMNNAME) PK_COLUMNNAME "
        + "FROM ETARC_TABLE_CONFIG CFG "
        + "JOIN AD_TABLE TBL ON TBL.AD_TABLE_ID = CFG.AD_TABLE_ID "
        + "JOIN AD_COLUMN COL ON COL.AD_COLUMN_ID = CFG.AD_COLUMN_ID "
        + "JOIN AD_COLUMN COL_PK ON COL_PK.AD_TABLE_ID = TBL.AD_TABLE_ID AND COL_PK.ISKEY = 'Y'";

    List<Map<String, String>> tableConfigs = new ArrayList<>();
    try (PreparedStatement ps = cp.getPreparedStatement(configSql);
         ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        Map<String, String> cfg = new HashMap<>();
        cfg.put("tableName", rs.getString("TABLENAME"));
        cfg.put("columnName", rs.getString("COLUMNNAME"));
        cfg.put("pkColumnName", rs.getString("PK_COLUMNNAME"));
        tableConfigs.add(cfg);
      }
    }
    return tableConfigs;
  }

  /**
   * Processes a single table configuration, determining whether constraints
   * need to be recreated. If the table is not yet partitioned correctly
   * or its structure has changed, it generates the corresponding SQL.
   *
   * @param cp
   *     the connection provider for accessing the database.
   * @param cfg
   *     a map containing the table configuration (table name, partition column, primary key).
   * @param sql
   *     the SQL builder to which constraint SQL will be appended if necessary.
   * @throws Exception
   *     if an error occurs during processing or querying the database.
   */
  private void processTableConfig(ConnectionProvider cp, Map<String, String> cfg, StringBuilder sql) throws Exception {
    String tableName = cfg.get("tableName");
    String partitionCol = cfg.get("columnName");
    String pkCol = cfg.get("pkColumnName");

    log4j.info("DATA FROM ETARC_TABLE_CONFIG: tableName: {} - partitionCol: {} - pkCol: {}", tableName, partitionCol,
        pkCol);

    boolean isIncomplete = isBlank(tableName) || isBlank(partitionCol) || isBlank(pkCol);
    List<File> xmlFiles = isIncomplete ? Collections.emptyList() : findTableXmlFiles(tableName);

    boolean isUnchanged = !isIncomplete &&
        !(new TableDefinitionComparator()).isTableDefinitionChanged(tableName, cp, xmlFiles);

    boolean isPartitioned = isTablePartitioned(cp, tableName);
    List<String> pkCols = getPrimaryKeyColumns(cp, tableName);
    boolean firstPartitionRun = isPartitioned && pkCols.isEmpty();

    log4j.info("Table {} partitioned = {} existing PK cols = {}", tableName, isPartitioned, pkCols);

    if (shouldSkipTable(isIncomplete, firstPartitionRun, isUnchanged)) {
      logSkipReason(isIncomplete, tableName, pkCol, partitionCol);
      return;
    }

    log4j.info("Recreating constraints for {} (firstRun = {}, xmlChanged = {})", tableName, firstPartitionRun,
        !isUnchanged);
    sql.append(buildConstraintSql(tableName, cp, pkCol, partitionCol));
  }

  /**
   * Determines whether a table should be skipped based on its configuration
   * and partitioning state.
   *
   * @param isIncomplete
   *     true if the table configuration is incomplete.
   * @param firstPartitionRun
   *     true if this is the first partitioning run for the table.
   * @param isUnchanged
   *     true if the table definition has not changed.
   * @return true if the table should be skipped, false otherwise.
   */
  private boolean shouldSkipTable(boolean isIncomplete, boolean firstPartitionRun, boolean isUnchanged) {
    return isIncomplete || (!firstPartitionRun && isUnchanged);
  }

  /**
   * Logs the reason why a table is being skipped during processing.
   *
   * @param isIncomplete
   *     true if the configuration is incomplete.
   * @param tableName
   *     the name of the table being skipped.
   * @param pkCol
   *     the primary key column name.
   * @param partitionCol
   *     the partition column name.
   */
  private void logSkipReason(boolean isIncomplete, String tableName, String pkCol, String partitionCol) {
    if (isIncomplete) {
      log4j.warn("Skipping incomplete configuration for table {} (pk = {}, partition = {})", tableName, pkCol,
          partitionCol);
    } else {
      log4j.info("Skipping {}: already processed and no XML changes", tableName);
    }
  }

  /**
   * Checks whether the given table is currently partitioned in the PostgreSQL database.
   *
   * @param cp
   *     the connection provider for accessing the database.
   * @param tableName
   *     the name of the table to check.
   * @return true if the table is partitioned, false otherwise.
   * @throws Exception
   *     if a database access error occurs.
   */
  private boolean isTablePartitioned(ConnectionProvider cp, String tableName) throws Exception {
    try (PreparedStatement ps = cp.getPreparedStatement(
        "SELECT 1 FROM pg_partitioned_table WHERE partrelid = to_regclass(?)")) {
      ps.setString(1, tableName);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    }
  }

  /**
   * Retrieves the list of columns that make up the primary key of the given table.
   *
   * @param cp
   *     the connection provider for accessing the database.
   * @param tableName
   *     the name of the table.
   * @return a list of column names that are part of the primary key.
   * @throws Exception
   *     if a database access error occurs.
   */
  private List<String> getPrimaryKeyColumns(ConnectionProvider cp, String tableName) throws Exception {
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
   * Executes the constraint SQL if it is not blank.
   * This typically includes adding or modifying table constraints after analyzing configurations.
   *
   * @param cp
   *     the connection provider for accessing the database.
   * @param sql
   *     the SQL string to execute.
   * @throws Exception
   *     if a database access error occurs.
   */
  private void executeConstraintSqlIfNeeded(ConnectionProvider cp, String sql) throws Exception {
    if (isBlank(sql)) {
      log4j.info("No constraints to handle for the provided configurations.");
      return;
    }

    try (PreparedStatement ps = cp.getPreparedStatement(sql)) {
      ps.executeUpdate();
    }
  }

  /**
   * Parses the specified XML file into a normalized DOM Document with
   * XXE protections enabled.
   * <p>
   * Configures the parser to:
   * <ul>
   *   <li>Disallow DOCTYPE declarations (no DTDs).</li>
   *   <li>Disable resolution of external general and parameter entities.</li>
   *   <li>Enable secure processing to limit resource usage.</li>
   *   <li>Disable XInclude processing and entity expansion.</li>
   * </ul>
   *
   * @param xml
   *     the XML file to parse
   * @return a normalized {@link org.w3c.dom.Document} representing the XML content
   * @throws ParserConfigurationException
   *     if a parser cannot be configured
   * @throws SAXException
   *     if a parsing error occurs
   * @throws IOException
   *     if an I/O error occurs reading the file
   */
  private static Document getDocument(File xml) throws ParserConfigurationException, SAXException, IOException {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

    // 1. Disallow DTDs entirely (no DOCTYPE)
    //    This prevents any <!DOCTYPE…> declarations
    factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);

    // 2. Disable external entities
    factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
    factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);

    // 3. (Optional) Enable the secure-processing feature
    factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);

    // 4. Further hardening
    factory.setXIncludeAware(false);
    factory.setExpandEntityReferences(false);

    DocumentBuilder builder = factory.newDocumentBuilder();
    Document doc = builder.parse(xml);
    doc.getDocumentElement().normalize();
    return doc;
  }

  /**
   * Searches the provided list of XML table definition files for the "primaryKey"
   * attribute on the single <table> element in each file.
   * <p>
   * For each XML file:
   * <ul>
   *   <li>If the file does not exist, logs an error and returns null.</li>
   *   <li>If exactly one <table> element is found, and it has a "primaryKey" attribute,
   *       returns that attribute’s value.</li>
   *   <li>If no <table> elements or multiple <table> elements are found, or if the
   *       attribute is missing, logs the appropriate warning/error and returns null.</li>
   * </ul>
   *
   * @param xmlFiles
   *     the list of XML files to inspect
   * @return the name of the primary key if found, or null if missing or on error
   */
  public static String findPrimaryKey(List<File> xmlFiles) {
    try {
      for (File xml : xmlFiles) {
        if (StringUtils.contains(xml.getAbsolutePath(), "modifiedTables")) {
          continue;
        }
        if (!xml.exists()) {
          log4j.error("Error: XML file does not exist: {}", xml.getAbsolutePath());
          return null;
        }
        Document doc = getDocument(xml);
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

  /**
   * Gathers all directories that potentially contain table XML files.
   * <p>
   * Scans each module directory under the project root (as defined by ModulesUtil),
   * adding:
   * <ul>
   *   <li>The module’s own “src-db/database/model/tables” directory, if present.</li>
   *   <li>That same tables directory under each immediate subdirectory of the module.</li>
   * </ul>
   * Finally, adds the project‐root “src-db/database/model/tables” directory.
   * Only existing directories are returned.
   *
   * @return a List of File objects representing each valid tables directory
   */
  private List<File> collectTableDirs() throws NoSuchFileException {
    List<File> dirs = new ArrayList<>();
    File root = new File(getSourcePath());
    for (String mod : this.moduleDirs) {
      File modBase = new File(root, mod);
      if (!modBase.isDirectory()) continue;
      dirs.add(new File(modBase, SRC_DB_DATABASE_MODEL_TABLES));
      for (File sd : Objects.requireNonNull(modBase.listFiles(File::isDirectory))) {
        dirs.add(new File(sd, SRC_DB_DATABASE_MODEL_TABLES));
      }
      dirs.add(new File(modBase, SRC_DB_DATABASE_MODEL_MODIFIED_TABLES));
      for (File sd : Objects.requireNonNull(modBase.listFiles(File::isDirectory))) {
        dirs.add(new File(sd, SRC_DB_DATABASE_MODEL_MODIFIED_TABLES));
      }
    }
    dirs.add(new File(root, SRC_DB_DATABASE_MODEL_TABLES));
    return dirs.stream().filter(File::isDirectory).collect(Collectors.toList());
  }

  /**
   * Finds the .xml file(s) matching the given table name (case-insensitive)
   * under each module’s “tables” directory and under the project's root.
   * <p>
   * Constructs a target filename of the form {@code tableName + ".xml"}, then
   * filters all XMLs in the discovered directories to only those whose name
   * equals the target.
   *
   * @param tableName
   *     the base name of the table (without the .xml extension)
   * @return a List of matching XML files (maybe empty if none found)
   */
  public List<File> findTableXmlFiles(String tableName) throws NoSuchFileException {
    String target = tableName.toLowerCase() + ".xml";
    return collectTableDirs().stream()
        .flatMap(dir -> {
          File[] files = dir.listFiles(f -> f.isFile() && f.getName().endsWith(".xml"));
          return files == null ? Stream.empty() : Arrays.stream(files);
        })
        .filter(f -> f.isFile() && f.getName().equalsIgnoreCase(target))
        .collect(Collectors.toList());
  }

  /**
   * Builds a SQL script to drop and re-create primary key and foreign key
   * constraints for the specified table, taking partitioning into account.
   * <p>
   * Steps performed:
   * <ol>
   * <li>Checks whether {@code tableName} is partitioned.</li>
   * <li>Loads the table’s XML to determine the primary key name.</li>
   * <li>Drops existing PK and re-adds it (with or without partition column).</li>
   * <li>Iterates all table XML definition files to find any foreign keys referencing
   * {@code tableName}, then drops and re-adds those FKs (partitioned if needed).</li>
   * </ol>
   *
   * @param tableName
   *     the name of the table to modify
   * @param cp
   *     the ConnectionProvider used to query catalog tables
   * @param pkField
   *     the column name of the primary key
   * @param partitionField
   *     the partition key column (if table is partitioned)
   * @return the complete DDL script as a single String
   * @throws Exception
   *     if any database or XML processing error occurs
   */
  public String buildConstraintSql(String tableName, ConnectionProvider cp, String pkField,
      String partitionField) throws Exception {
    // Check if table is partitioned
    String checkPartition = "SELECT 1 FROM pg_partitioned_table WHERE partrelid = to_regclass(?)";
    PreparedStatement psCheck = cp.getPreparedStatement(checkPartition);
    psCheck.setString(1, tableName);
    boolean isPartitioned = psCheck.executeQuery().next();
    psCheck.close();

    // Get required information from the primary table's XML
    List<File> tableXmlFiles = findTableXmlFiles(tableName);
    if (tableXmlFiles.isEmpty()) {
      throw new Exception("Entity XML file for " + tableName + " not found.");
    }
    String pkName = findPrimaryKey(tableXmlFiles);
    if (pkName == null) {
      throw new Exception("Primary Key for entity " + tableName + " not found in XML.");
    }

    // SQL templates for primary table
    String dropPrimaryKeySQL = ALTER_TABLE + "DROP CONSTRAINT IF EXISTS %s CASCADE;\n";
    String addPartitionedPrimaryKeySQL = ALTER_TABLE + "ADD CONSTRAINT %s PRIMARY KEY (%s, %s);\n";
    String addSimplePrimaryKeySQL = ALTER_TABLE + "ADD CONSTRAINT %s PRIMARY KEY (%s);\n";

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
    String dropForeignKeySQL = ALTER_TABLE + "DROP CONSTRAINT IF EXISTS %s;\n";
    String addColumnSQL = "ALTER TABLE %s\n" + "ADD COLUMN IF NOT EXISTS %s TIMESTAMP WITHOUT TIME ZONE;\n";
    String updateColumnSQL = "UPDATE %s SET %s = F.%s FROM %s F "
        + "WHERE F.%s = %s.%s AND %s.%s IS NULL;\n";
    String addPartitionedForeignKeySQL = ALTER_TABLE
        + "ADD CONSTRAINT %s FOREIGN KEY (%s, %s) "
        + "REFERENCES PUBLIC.%s (%s, %s) MATCH SIMPLE "
        + "ON UPDATE CASCADE ON DELETE NO ACTION;\n";
    String addSimpleForeignKeySQL = ALTER_TABLE + "ADD CONSTRAINT %s FOREIGN KEY (%s) "
        + "REFERENCES PUBLIC.%s (%s) MATCH SIMPLE "
        + "ON UPDATE NO ACTION ON DELETE NO ACTION;\n";

    // Iterate over all table XMLs to find references to our target table
    for (File dir : collectTableDirs()) {
      File[] xmlsInDir = dir.listFiles(f -> f.isFile() && f.getName().endsWith(".xml"));
      if (xmlsInDir == null) {
        continue;
      }

      for (File sourceXmlFile : xmlsInDir) {
        try {
          Document doc = getDocument(sourceXmlFile);
          NodeList tableNodes = doc.getElementsByTagName("table");
          if (tableNodes.getLength() != 1) {
            continue;
          }

          Element tableEl = (Element) tableNodes.item(0);
          // Skip views or the table we are already processing
          if (Boolean.parseBoolean(tableEl.getAttribute("isView")) || tableName.equalsIgnoreCase(
              tableEl.getAttribute("name"))) {
            continue;
          }

          String relatedTableName = tableEl.getAttribute("name").toUpperCase();
          NodeList fkList = tableEl.getElementsByTagName("foreign-key");

          for (int i = 0; i < fkList.getLength(); i++) {
            Element fkEl = (Element) fkList.item(i);
            if (tableName.equalsIgnoreCase(fkEl.getAttribute("foreignTable"))) {
              // This table has a foreign key to our target table
              String foreignKey = fkEl.getAttribute("name");
              NodeList refList = fkEl.getElementsByTagName("reference");
              if (refList.getLength() == 0) {
                continue;
              }

              // Assuming a single-column FK for this logic, like the original method
              Element refEl = (Element) refList.item(0);
              String relationColumn = refEl.getAttribute("local");

              if (isBlank(foreignKey) || isBlank(relationColumn)) {
                continue;
              }

              sql.append(String.format(dropForeignKeySQL, relatedTableName, foreignKey));

              if (isPartitioned) {
                String partitionColumn = "etarc_" + partitionField + "__" + foreignKey;
                sql.append(String.format(addColumnSQL, relatedTableName, partitionColumn));
                sql.append(String.format(updateColumnSQL, relatedTableName, partitionColumn,
                    partitionField, tableName, pkField, relatedTableName, relationColumn,
                    relatedTableName, partitionColumn));
                sql.append(
                    String.format(addPartitionedForeignKeySQL, relatedTableName, foreignKey,
                        relationColumn, partitionColumn, tableName, pkField, partitionField));
              } else {
                sql.append(String.format(addSimpleForeignKeySQL, relatedTableName, foreignKey,
                    relationColumn, tableName, pkField));
              }
            }
          }
        } catch (Exception e) {
          log4j.error("Error processing XML file: {}", sourceXmlFile.getAbsolutePath(), e);
        }
      }
    }
    return sql.toString();
  }
}
