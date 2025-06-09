package com.etendoerp.archiving.utils;

import java.io.File;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.util.*;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openbravo.base.exception.OBException;
import org.openbravo.base.model.Entity;
import org.openbravo.base.model.ModelProvider;
import org.openbravo.database.ConnectionProvider;
import org.openbravo.ddlutils.util.ModulesUtil;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

public class ArchivingPreBuildUtils {

  private static final String SRC_DB_DATABASE_MODEL_TABLES = "src-db/database/model/tables";
  private static final Logger log4j = LogManager.getLogger();

  private ArchivingPreBuildUtils() {
    throw new IllegalStateException("Utility class. It shouldn't be instantiated");
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
   * @param xmlFiles the list of XML files to inspect
   * @return the name of the primary key if found, or null if missing or on error
   */
  public static String findPrimaryKey(List<File> xmlFiles) {
    try {
      for (File xml : xmlFiles) {
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
   * Retrieves all .xml files located under the configured “tables” directory of
   * each module and under the project's root “tables” directory.
   * <p>
   * Relies on {@code collectTableDirs()} to discover each valid directory, then
   * lists and collects any files ending with “.xml”.
   *
   * @return a List of all XML files found in module and root table directories
   */
  public static List<File> findAllTableXmlFiles() {
    return collectTableDirs().stream()
        .flatMap(dir -> {
          File[] files = dir.listFiles(f -> f.isFile() && f.getName().endsWith(".xml"));
          return files == null ? Stream.empty() : Arrays.stream(files);
        })
        .toList();
  }

  /**
   * Finds the .xml file(s) matching the given table name (case-insensitive)
   * under each module’s “tables” directory and under the project's root.
   * <p>
   * Constructs a target filename of the form {@code tableName + ".xml"}, then
   * filters all XMLs in the discovered directories to only those whose name
   * equals the target.
   *
   * @param tableName the base name of the table (without the .xml extension)
   * @return a List of matching XML files (maybe empty if none found)
   */
  public static List<File> findTableXmlFiles(String tableName) {
    String target = tableName.toLowerCase() + ".xml";
    return collectTableDirs().stream()
        .flatMap(dir -> {
          File[] files = dir.listFiles(f -> f.isFile() && f.getName().endsWith(".xml"));
          return files == null ? Stream.empty() : Arrays.stream(files);
        })
        .filter(f -> f.isFile() && f.getName().equalsIgnoreCase(target))
        .toList();
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
  private static List<File> collectTableDirs() {
    List<File> dirs = new ArrayList<>();
    File root = new File(ModulesUtil.getProjectRootDir());
    for (String mod : ModulesUtil.moduleDirs) {
      File modBase = new File(root, mod);
      if (!modBase.isDirectory()) continue;
      dirs.add(new File(modBase, SRC_DB_DATABASE_MODEL_TABLES));
      for (File sd : Objects.requireNonNull(modBase.listFiles(File::isDirectory))) {
        dirs.add(new File(sd, SRC_DB_DATABASE_MODEL_TABLES));
      }
    }
    dirs.add(new File(root, SRC_DB_DATABASE_MODEL_TABLES));
    return dirs.stream().filter(File::isDirectory).toList();
  }

//  public static List<File> findAllTableXmlFiles() {
//    List<File> allTableFiles = new ArrayList<>();
//    File projectRootDir = new File(ModulesUtil.getProjectRootDir());
//    List<String> modules = Arrays.stream(ModulesUtil.moduleDirs).toList();
//
//    for (String modulePath : modules) {
//      File moduleBase = new File(projectRootDir, modulePath);
//      if (!moduleBase.exists() || !moduleBase.isDirectory()) {
//        continue;
//      }
//
//      File directXmlDir = new File(moduleBase, ArchivingPreBuildUtils.SRC_DB_DATABASE_MODEL_TABLES);
//      if (directXmlDir.exists() && directXmlDir.isDirectory()) {
//        File[] files = directXmlDir.listFiles(f -> f.isFile() && f.getName().toLowerCase().endsWith(".xml"));
//        if (files != null) {
//          allTableFiles.addAll(Arrays.asList(files));
//        }
//      }
//
//      File[] subDirs = moduleBase.listFiles(File::isDirectory);
//      if (subDirs != null) {
//        for (File subDir : subDirs) {
//          File xmlInSubDir = new File(subDir, ArchivingPreBuildUtils.SRC_DB_DATABASE_MODEL_TABLES);
//          if (xmlInSubDir.exists() && xmlInSubDir.isDirectory()) {
//            File[] files = xmlInSubDir.listFiles(f -> f.isFile() && f.getName().toLowerCase().endsWith(".xml"));
//            if (files != null) {
//              allTableFiles.addAll(Arrays.asList(files));
//            }
//          }
//        }
//      }
//    }
//
//    // 3) Check the project’s root folder for table XMLs
//    File rootXmlDir = new File(projectRootDir, ArchivingPreBuildUtils.SRC_DB_DATABASE_MODEL_TABLES);
//    if (rootXmlDir.exists() && rootXmlDir.isDirectory()) {
//      File[] files = rootXmlDir.listFiles(f -> f.isFile() && f.getName().toLowerCase().endsWith(".xml"));
//      if (files != null) {
//        allTableFiles.addAll(Arrays.asList(files));
//      }
//    }
//
//    return allTableFiles;
//  }
//
//  public static List<File> findTableXmlFiles(String tableName) {
//    List<File> foundFiles = new ArrayList<>();
//    File projectDir = new File(ModulesUtil.getProjectRootDir());
//    List<String> modules = new ArrayList<>(Arrays.stream(ModulesUtil.moduleDirs).toList());
//    List<String> moduleDirsToCheck = new ArrayList<>();
//
//    for (String modulePath : modules) {
//      File moduleDir = new File(projectDir, modulePath);
//      if (moduleDir.exists() && moduleDir.isDirectory()) {
//        File[] subDirs = moduleDir.listFiles(File::isDirectory);
//        if (subDirs != null) {
//          for (File subDir : subDirs) {
//            moduleDirsToCheck.add(subDir.getAbsolutePath());
//          }
//        }
//      }
//    }
//    moduleDirsToCheck.add(ModulesUtil.getProjectRootDir() + "/");
//
//    for (String dir : moduleDirsToCheck) {
//      File xmlDir = new File(dir + SRC_DB_DATABASE_MODEL_TABLES);
//      if (xmlDir.exists() && xmlDir.isDirectory()) {
//        File[] files = xmlDir.listFiles();
//        if (files != null) {
//          for (File file : files) {
//            if (file.isFile() && file.getName().equalsIgnoreCase(tableName + ".xml")) {
//              foundFiles.add(file);
//            }
//          }
//        }
//      }
//    }
//    return foundFiles;
//

  /**
   * Searches the supplied XML files for a <foreign-key> element that references
   * the specified target table and column, and returns its "name" attribute.
   * <p>
   * Iterates each file:
   * <ul>
   *   <li>Skips any file that does not exist (logs an error).</li>
   *   <li>For each <foreign-key> whose foreignTable matches {@code targetTable},
   *       inspects its <reference> children to match {@code targetColumn}.</li>
   *   <li>Returns the foreign-key’s name as soon as both table and column match.</li>
   * </ul>
   *
   * @param xmlFiles     the list of XML files to search through
   * @param targetTable  the name of the table being referenced
   * @param targetColumn the local column name in the foreign key reference
   * @return the foreign-key name if found, or null otherwise
   */
  public static String findForeignKeyName(List<File> xmlFiles, String targetTable,
                                          String targetColumn) {
    try {
      for (File xml : xmlFiles) {
        if (!xml.exists()) {
          log4j.error("Error: XML file does not exist: {}", xml.getAbsolutePath());
          return null;
        }
        Document doc = getDocument(xml);
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
   * @param xml the XML file to parse
   * @return a normalized {@link org.w3c.dom.Document} representing the XML content
   * @throws ParserConfigurationException if a parser cannot be configured
   * @throws SAXException                 if a parsing error occurs
   * @throws IOException                  if an I/O error occurs reading the file
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
   * Builds a SQL script to drop and re-create primary key and foreign key
   * constraints for the specified table, taking partitioning into account.
   * <p>
   * Steps performed:
   * <ol>
   *   <li>Checks whether {@code tableName} is partitioned.</li>
   *   <li>Loads the table’s XML to determine the primary key name.</li>
   *   <li>Drops existing PK and re-adds it (with or without partition column).</li>
   *   <li>Iterates the Openbravo model to find any properties referencing
   *       {@code tableName}, drops and re-adds those FKs (partitioned if needed).</li>
   * </ol>
   *
   * @param tableName      the name of the table to modify
   * @param cp             the ConnectionProvider used to query catalog tables
   * @param pkField        the column name of the primary key
   * @param partitionField the partition key column (if table is partitioned)
   * @return the complete DDL script as a single String
   * @throws Exception if any database or XML processing error occurs
   */
  public static String buildConstraintSql(String tableName, ConnectionProvider cp, String pkField,
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
      throw new OBException("Entity " + tableName + " not found in model.");

    return sql.toString();
  }

  /**
   * Scans all table XML definition files for <foreign-key> elements that reference
   * the specified target table and returns their constraint names.
   * <p>
   * Uses {@link #findAllTableXmlFiles()} to retrieve every XML file, parses each
   * safely via {@link #getDocument(File)}, and inspects all <foreign-key> elements.
   * If a foreign-key’s <code>foreignTable</code> attribute matches
   * {@code targetTable} (case-insensitive) and its <code>name</code> attribute
   * is non-empty, the name (uppercased) is added to the result set.
   * <p>
   * Any parse or I/O errors are caught and logged; in such cases the method
   * continues processing remaining files and ultimately returns whatever has
   * been collected (possibly an empty set).
   *
   * @param targetTable the name of the table whose foreign-key references are sought
   * @return a Set of unique, uppercased foreign-key names referencing {@code targetTable},
   *         or an empty set if none are found or errors occur
   */
  public static Set<String> findAllForeignKeysReferencing(String targetTable) {
    Set<String> fkNames = new HashSet<>();
    try {
      List<File> allXmlFiles = ArchivingPreBuildUtils.findAllTableXmlFiles();

      for (File xmlFile : allXmlFiles) {
        if (!xmlFile.exists()) {
          continue;
        }
        Document document = getDocument(xmlFile);

        NodeList foreignKeyList = document.getElementsByTagName("foreign-key");
        for (int i = 0; i < foreignKeyList.getLength(); i++) {
          Element foreignKeyElement = (Element) foreignKeyList.item(i);
          String fkTarget = foreignKeyElement.getAttribute("foreignTable");
          if (fkTarget != null && fkTarget.equalsIgnoreCase(targetTable)) {
            String fkName = foreignKeyElement.getAttribute("name");
            if (fkName != null && !fkName.trim().isEmpty()) {
              fkNames.add(fkName.toUpperCase());
            }
          }
        }
      }
    } catch (Exception e) {
      log4j.error("Error searching for FKs referencing '{}': {}", targetTable, e.getMessage());
    }
    return fkNames;
  }
}
