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

package com.etendoerp.db.extended.buildvalidation;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openbravo.buildvalidation.BuildValidation;
import org.openbravo.database.ConnectionProvider;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * Generates the excludeFilter.xml by collecting constraint names
 * (PKs and FKs) found in the model XML files instead of using information_schema.
 */
public class CreateExcludeFilter extends BuildValidation {
  public static final String MODULES_JAR = "build/etendo/modules";
  public static final String MODULES_BASE = "modules";
  public static final String MODULES_CORE = "modules_core";
  public static final String END_XML_FILE = "\"/>\n";
  private static final Logger logger = LogManager.getLogger();
  private static final String SRC_DB_DATABASE_MODEL_TABLES = "src-db/database/model/tables";
  private static final String SRC_DB_DATABASE_MODEL_MODIFIED_TABLES = "src-db/database/model/modifiedTables";
  private static String[] moduleDirs = new String[]{ MODULES_BASE, MODULES_CORE, MODULES_JAR };

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
          logger.error("Error: XML file does not exist: {}", xml.getAbsolutePath());
          return null;
        }
        Document doc = getDocument(xml);
        NodeList tableList = doc.getElementsByTagName("table");

        if (tableList.getLength() == 1) {
          Element tableEl = (Element) tableList.item(0);
          if (tableEl.hasAttribute("primaryKey")) {
            return tableEl.getAttribute("primaryKey");
          } else {
            logger.warn("Warning: Missing 'primaryKey' attribute in: {}", xml.getAbsolutePath());
            return null;
          }
        } else if (tableList.getLength() == 0) {
          logger.error("Error: No <table> tag found in: {}", xml.getAbsolutePath());
          return null;
        } else {
          logger.error("Error: Found {} <table> tags in: {}", tableList.getLength(),
              xml.getAbsolutePath());
          return null;
        }
      }
    } catch (Exception e) {
      logger.error("Error processing XML: {}", e.getMessage(), e);
    }
    return null;
  }

  @Override
  public List<String> execute() {
    Set<String> constraintsToExclude = new HashSet<>();
    Set<String> columnsToExclude = new HashSet<>();
    Set<String> triggersToExclude = new HashSet<>();
    Set<String> functionsToExclude = new HashSet<>();

    try {
      ConnectionProvider connectionProvider = getConnectionProvider();
      String baseTablesQuery = getBaseTablesQuery();

      processPartitionedTables(connectionProvider, baseTablesQuery, constraintsToExclude, columnsToExclude);
      generateTriggersAndFunctionsForChildTables(connectionProvider, baseTablesQuery,
          triggersToExclude, functionsToExclude);
      writeExcludeFilterXml(constraintsToExclude, columnsToExclude, triggersToExclude, functionsToExclude);

    } catch (Exception e) {
      logger.error("Error generating excludeFilter.xml: {}", e.getMessage(), e);
    }

    return List.of();
  }

  private String getBaseTablesQuery() {
    return "SELECT LOWER(T.TABLENAME) TABLENAME, LOWER(C.COLUMNNAME) COLUMNNAME " +
        "FROM ETARC_TABLE_CONFIG TC " +
        "JOIN AD_TABLE T ON TC.AD_TABLE_ID = T.AD_TABLE_ID " +
        "JOIN AD_COLUMN C ON C.AD_COLUMN_ID = TC.AD_COLUMN_ID";
  }

  private void processPartitionedTables(ConnectionProvider connectionProvider, String baseTablesQuery,
      Set<String> constraintsToExclude, Set<String> columnsToExclude) throws Exception {

    logger.info("Executing query to retrieve base tables");
    PreparedStatement baseTablesStmt = connectionProvider.getPreparedStatement(baseTablesQuery);
    try (baseTablesStmt; ResultSet baseTablesResult = baseTablesStmt.executeQuery()) {
      while (baseTablesResult.next()) {
        String baseTableName = baseTablesResult.getString("tablename");
        String partitionColumnName = baseTablesResult.getString("columnname");

        if (StringUtils.isBlank(baseTableName)) {
          logger.warn("Received an empty or null base table name; skipping.");
          continue;
        }

        processSingleTable(baseTableName, partitionColumnName, constraintsToExclude, columnsToExclude);
      }
    } catch (Exception e) {
      logger.error("Error processing data from ETARC_Table_Config.", e);
      throw new BuildValidationException("Failed to process partitioned tables configuration", e);
    }
  }

  private void processSingleTable(String baseTableName, String partitionColumnName,
      Set<String> constraintsToExclude, Set<String> columnsToExclude) throws BuildValidationException {

    logger.info("Processing base table: {}", baseTableName);

    processPrimaryKey(baseTableName, constraintsToExclude);
    processForeignKeys(baseTableName, partitionColumnName, constraintsToExclude, columnsToExclude);
  }

  private void processPrimaryKey(String baseTableName,
      Set<String> constraintsToExclude) throws BuildValidationException {
    try {
      List<File> baseTableXmlFiles = findTableXmlFiles(baseTableName);
      String primaryKeyName = findPrimaryKey(baseTableXmlFiles);

      if (!StringUtils.isBlank(primaryKeyName)) {
        String primaryKeyUpper = primaryKeyName.toUpperCase();
        constraintsToExclude.add(primaryKeyUpper);
        logger.info("Found PK for '{}': {}", baseTableName, primaryKeyUpper);
      } else {
        logger.warn("No PRIMARY KEY found in the XMLs of '{}'", baseTableName);
      }
    } catch (NoSuchFileException e) {
      throw new BuildValidationException("Failed to find XML files for table: " + baseTableName, e);
    }
  }

  private void processForeignKeys(String baseTableName, String partitionColumnName,
      Set<String> constraintsToExclude, Set<String> columnsToExclude) {

    Set<String> referencingFks = findAllForeignKeysReferencing(baseTableName);
    if (referencingFks.isEmpty()) {
      logger.info("No FKs found referencing '{}'", baseTableName);
      return;
    }

    logger.info("Found {} FKs referencing '{}'", referencingFks.size(), baseTableName);
    constraintsToExclude.addAll(referencingFks);
    generatePartitionColumns(partitionColumnName, referencingFks, columnsToExclude);
  }

  private void generatePartitionColumns(String partitionColumnName, Set<String> referencingFks,
      Set<String> columnsToExclude) {

    logger.info("Partition column for table: {}", partitionColumnName);
    if (StringUtils.isBlank(partitionColumnName)) {
      return;
    }

    referencingFks.forEach(fk -> {
      String columnToExclude = "ETARC_" + partitionColumnName.toUpperCase() + "__" + fk.toUpperCase();
      columnsToExclude.add(columnToExclude);
      logger.info("Adding column to exclude: {}", columnToExclude);
    });
  }

  private void writeExcludeFilterXml(Set<String> constraintsToExclude, Set<String> columnsToExclude,
      Set<String> triggersToExclude, Set<String> functionsToExclude) throws IOException {

    logger.info("Generating excludeFilter.xml for {} excluded constraints, {} columns, {} triggers, {} functions",
        constraintsToExclude.size(), columnsToExclude.size(), triggersToExclude.size(), functionsToExclude.size());

    String sourcePath = getSourcePath();
    Path outputFile = Paths.get(sourcePath, MODULES_BASE, "com.etendoerp.db.extended.exclude.filter",
        "src-db", "database", "model", "excludeFilter.xml");
    Files.createDirectories(outputFile.getParent());

    StringBuilder xmlBuilder = new StringBuilder();
    xmlBuilder.append("<vector>\n");

    appendXmlEntries(xmlBuilder, constraintsToExclude, "excludedConstraint");
    appendXmlEntries(xmlBuilder, columnsToExclude, "excludedColumn");
    appendXmlEntries(xmlBuilder, triggersToExclude, "excludedTrigger");
    appendXmlEntries(xmlBuilder, functionsToExclude, "excludedFunction");

    xmlBuilder.append("</vector>\n");

    Files.writeString(outputFile, xmlBuilder.toString(), StandardCharsets.UTF_8);
    logger.info("Generated excludeFilter.xml successfully.");
  }

  private void appendXmlEntries(StringBuilder xmlBuilder, Set<String> entries, String elementName) {
    for (String entry : entries) {
      xmlBuilder.append("  <").append(elementName).append(" name=\"")
          .append(entry).append(END_XML_FILE);
    }
  }

  /**
   * Generates trigger and function names for child tables that reference partitioned parent tables.
   * Uses the same naming convention as PartitionedConstraintsHandling.
   */
  private void generateTriggersAndFunctionsForChildTables(ConnectionProvider connectionProvider,
      String baseTablesQuery, Set<String> triggersToExclude, Set<String> functionsToExclude) {

    try {
      // Get all partitioned base tables
      PreparedStatement baseTablesStmt = connectionProvider.getPreparedStatement(baseTablesQuery);
      ResultSet baseTablesResult = baseTablesStmt.executeQuery();

      Set<String> partitionedTables = new HashSet<>();
      while (baseTablesResult.next()) {
        String baseTableName = baseTablesResult.getString("tablename");
        if (!StringUtils.isBlank(baseTableName)) {
          partitionedTables.add(baseTableName.toUpperCase());
        }
      }
      baseTablesResult.close();
      baseTablesStmt.close();

      // Find all child tables that reference these partitioned tables
      Set<String> processedChildTables = new HashSet<>();

      for (String partitionedTable : partitionedTables) {
        logger.info("Generating trigger exclusions for partitioned table: {}", partitionedTable);

        // Find all FKs that reference this partitioned table
        Set<String> referencingFks = findAllForeignKeysReferencing(partitionedTable);

        for (String fkName : referencingFks) {
          // Find the child table that contains this FK
          String childTable = findChildTableForForeignKey(fkName);

          if (!StringUtils.isBlank(childTable)) {
            String childTableUpper = childTable.toUpperCase();

            // Skip if we already processed this child table
            if (processedChildTables.contains(childTableUpper)) {
              continue;
            }

            // Generate trigger and function names using the same pattern as PartitionedConstraintsHandling
            String triggerName = "ETARC_PARTITION_TRIGGER_" + childTableUpper;
            String functionName = "ETARC_AUTO_PARTITION_" + childTableUpper;

            triggersToExclude.add(triggerName);
            functionsToExclude.add(functionName);
            processedChildTables.add(childTableUpper);

            logger.info("Added trigger exclusion: {} for child table: {}", triggerName, childTableUpper);
            logger.info("Added function exclusion: {} for child table: {}", functionName, childTableUpper);
          }
        }
      }

    } catch (Exception e) {
      logger.error("Error generating trigger and function exclusions: {}", e.getMessage(), e);
    }
  }

  /**
   * Finds the child table that contains the specified foreign key constraint.
   */
  private String findChildTableForForeignKey(String fkName) {
    try {
      List<File> allXmlFiles = findAllTableXmlFiles();

      for (File xmlFile : allXmlFiles) {
        if (!xmlFile.exists()) {
          continue;
        }

        Document document = getDocument(xmlFile);
        NodeList tableList = document.getElementsByTagName("table");

        if (tableList.getLength() == 1) {
          Element tableElement = (Element) tableList.item(0);
          String tableName = tableElement.getAttribute("name");

          // Check if this table contains the FK
          NodeList foreignKeyList = document.getElementsByTagName("foreign-key");
          for (int i = 0; i < foreignKeyList.getLength(); i++) {
            Element foreignKeyElement = (Element) foreignKeyList.item(i);
            String currentFkName = foreignKeyElement.getAttribute("name");

            if (StringUtils.equalsIgnoreCase(fkName, currentFkName)) {
              return tableName;
            }
          }
        }
      }
    } catch (Exception e) {
      logger.error("Error finding child table for FK '{}': {}", fkName, e.getMessage());
    }
    return null;
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
   * @param targetTable
   *     the name of the table whose foreign-key references are sought
   * @return a Set of unique, uppercased foreign-key names referencing {@code targetTable},
   *     or an empty set if none are found or errors occur
   */
  public Set<String> findAllForeignKeysReferencing(String targetTable) {
    Set<String> fkNames = new HashSet<>();
    try {
      List<File> allXmlFiles = findAllTableXmlFiles();

      for (File xmlFile : allXmlFiles) {
        if (!xmlFile.exists()) {
          continue;
        }
        Document document = getDocument(xmlFile);

        NodeList foreignKeyList = document.getElementsByTagName("foreign-key");
        for (int i = 0; i < foreignKeyList.getLength(); i++) {
          Element foreignKeyElement = (Element) foreignKeyList.item(i);
          String fkTarget = foreignKeyElement.getAttribute("foreignTable");
          if (StringUtils.equalsIgnoreCase(fkTarget, targetTable)) {
            String fkName = foreignKeyElement.getAttribute("name");
            if (!StringUtils.isBlank(fkName.trim())) {
              fkNames.add(fkName.toUpperCase());
            }
          }
        }
      }
    } catch (Exception e) {
      logger.error("Error searching for FKs referencing '{}': {}", targetTable, e.getMessage());
    }
    return fkNames;
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
    for (String mod : moduleDirs) {
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
   * Retrieves all .xml files located under the configured “tables” directory of
   * each module and under the project's root “tables” directory.
   * <p>
   * Relies on {@code collectTableDirs()} to discover each valid directory, then
   * lists and collects any files ending with “.xml”.
   *
   * @return a List of all XML files found in module and root table directories
   */
  public List<File> findAllTableXmlFiles() throws NoSuchFileException {
    return collectTableDirs().stream()
        .flatMap(dir -> {
          File[] files = dir.listFiles(f -> f.isFile() && f.getName().endsWith(".xml"));
          return files == null ? Stream.empty() : Arrays.stream(files);
        })
        .collect(Collectors.toList());
  }
}
