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

package com.etendoerp.db.extended.utils;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openbravo.database.ConnectionProvider;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * Processes XML table definition files used by the Etendo framework.
 * 
 * <p>This class handles the parsing, validation, and analysis of XML files that define
 * database table structures in the Etendo system. It provides functionality to locate
 * table definitions, extract foreign key relationships, and detect changes that might
 * affect partitioned table constraints.
 * 
 * <h3>Key Responsibilities:</h3>
 * <ul>
 *   <li><strong>XML File Discovery:</strong> Locates table definition files across module directories</li>
 *   <li><strong>Secure XML Parsing:</strong> Parses XML with XXE (XML External Entity) attack protections</li>
 *   <li><strong>Foreign Key Detection:</strong> Identifies external foreign key references to tables</li>
 *   <li><strong>Primary Key Extraction:</strong> Retrieves primary key definitions from XML</li>
 *   <li><strong>Change Detection:</strong> Works with {@link TableDefinitionComparator} for change tracking</li>
 * </ul>
 * 
 * <h3>XML File Structure:</h3>
 * <p>The processor searches for XML files in the following locations:
 * <ul>
 *   <li>{@code src-db/database/model/tables/} - Standard table definitions</li>
 *   <li>{@code src-db/database/model/modifiedTables/} - Modified table definitions</li>
 *   <li>{@code build/etendo/modules/} - Built module definitions</li>
 *   <li>{@code modules/} - Module source definitions</li>
 *   <li>{@code modules_core/} - Core module definitions</li>
 * </ul>
 * 
 * <h3>Security Features:</h3>
 * <p>XML parsing includes protection against common XML-based attacks:
 * <ul>
 *   <li>Disallows DOCTYPE declarations</li>
 *   <li>Disables external entity processing</li>
 *   <li>Enables secure processing features</li>
 *   <li>Prevents XML inclusion and entity expansion</li>
 * </ul>
 * 
 * <h3>Foreign Key Analysis:</h3>
 * <p>The processor can detect when external tables reference a given table through
 * foreign keys, which is crucial for determining when constraint recreation is needed
 * even if the target table itself hasn't changed.
 * 
 * <h3>Usage Example:</h3>
 * <pre>{@code
 * XmlTableProcessor processor = new XmlTableProcessor(backupManager, sourcePath);
 * List<File> xmlFiles = processor.findTableXmlFiles("C_Order");
 * boolean hasExternalRefs = processor.hasForeignReferencesInXml("C_Order", connectionProvider);
 * String primaryKey = XmlTableProcessor.findPrimaryKey(xmlFiles);
 * }</pre>
 * 
 * @author Futit Services S.L.
 * @since ETP-2450
 * @see TableDefinitionComparator
 * @see BackupManager
 */
public class XmlTableProcessor {
  public static final String MODULES_JAR = "build/etendo/modules";
  public static final String MODULES_BASE = "modules";
  public static final String MODULES_CORE = "modules_core";
  private static final Logger log4j = LogManager.getLogger();
  private static final String SRC_DB_DATABASE_MODEL_TABLES = "src-db/database/model/tables";
  private static final String SRC_DB_DATABASE_MODEL_MODIFIED_TABLES = "src-db/database/model/modifiedTables";
  private static final String[] moduleDirs = new String[]{ MODULES_BASE, MODULES_CORE, MODULES_JAR };
  private static final String TABLE = "table";
  private static final String FOREIGN_KEY = "foreign-key";

  private final BackupManager backupManager;
  private final String sourcePath;

  /**
   * Constructs a new XmlTableProcessor with the specified backup manager and source path.
   * 
   * @param backupManager the BackupManager instance for handling database backups
   * @param sourcePath the base path for locating XML table definition files
   */
  public XmlTableProcessor(BackupManager backupManager, String sourcePath) {
    this.backupManager = backupManager;
    this.sourcePath = sourcePath;
  }

  /**
   * Parses an XML file into a normalized DOM {@link Document} with XXE protections enabled.
   * 
   * @param xml the XML file to parse
   * @return a normalized DOM Document representation of the XML file
   * @throws ParserConfigurationException if a DocumentBuilder cannot be created
   * @throws SAXException if any parse errors occur
   * @throws IOException if any I/O errors occur during parsing
   */
  public static Document getDocument(File xml)
      throws ParserConfigurationException, SAXException, IOException {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
    factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
    factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
    factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
    factory.setXIncludeAware(false);
    factory.setExpandEntityReferences(false);

    DocumentBuilder builder = factory.newDocumentBuilder();
    Document doc = builder.parse(xml);
    doc.getDocumentElement().normalize();
    return doc;
  }

  /**
   * Scans a list of XML files to locate and return the primary key attribute
   * defined in a <table> element.
   * 
   * @param xmlFiles the list of XML files to scan for primary key definitions
   * @return the primary key attribute name, or null if not found
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
        NodeList tableList = doc.getElementsByTagName(TABLE);

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
          log4j.error("Error: Found {} <table> tags in: {}", tableList.getLength(), xml.getAbsolutePath());
          return null;
        }
      }
    } catch (Exception e) {
      log4j.error("Error processing XML: {}", e.getMessage(), e);
    }
    return null;
  }

  private static void logUnparseableXML(File sourceXmlFile, Exception e) {
    log4j.info("Skipping unparsable XML while scanning for FK refs: {} -> {}",
        sourceXmlFile.getAbsolutePath(), e.getMessage());
  }

  /**
   * Scans table XMLs for any {@code <foreign-key foreignTable="...">} referencing {@code tableName}.
   * 
   * @param tableName the table name to search for foreign key references
   * @param cp the ConnectionProvider for database operations
   * @return true if any XML files contain foreign key references to the specified table
   */
  public boolean hasForeignReferencesInXml(String tableName, ConnectionProvider cp) {
    Timestamp lastProcessed = backupManager.getLastProcessed(cp, tableName);
    for (File xml : collectAllXmlFiles()) {
      if (!shouldScan(xml, lastProcessed)) continue;
      if (fileReferencesTable(xml, tableName)) {
        backupManager.setLastProcessed(cp, tableName); // avoid repeated forced runs
        return true;
      }
    }
    return false;
  }

  /**
   * Finds XML files that define {@code tableName} either by filename (fast path)
   * or by containing a {@code <table name="...">} with the same name.
   * 
   * @param tableName the table name to search for in XML definitions
   * @return a list of XML files that define the specified table
   */
  public List<File> findTableXmlFiles(String tableName) {
    String targetLower = normalizeLower(tableName);
    Set<File> matches = new LinkedHashSet<>();
    for (File xml : collectAllXmlFiles()) {
      if (fileNameMatches(xml, targetLower) || containsTableDefinition(xml, tableName)) {
        matches.add(xml);
      }
    }
    return new ArrayList<>(matches);
  }

  /**
   * Returns a flattened list of all {@code .xml} files under the collected directories.
   * 
   * @return a list of all XML files found in the configured source directories
   */
  public List<File> collectAllXmlFiles() {
    List<File> xmls = new ArrayList<>();
    for (File dir : collectTableDirs()) {
      Collections.addAll(xmls, listXmlFiles(dir));
    }
    return xmls;
  }

  /**
   * Collects directories that may contain table XMLs.
   * 
   * @return a list of directories that potentially contain table XML definitions
   */
  public List<File> collectTableDirs() {
    List<File> dirs = new ArrayList<>();
    File root = new File(sourcePath);
    for (String mod : moduleDirs) {
      File modBase = new File(root, mod);
      if (!modBase.isDirectory()) continue;
      dirs.add(new File(modBase, SRC_DB_DATABASE_MODEL_TABLES));
      File[] subDirs = modBase.listFiles(File::isDirectory);
      if (subDirs != null) {
        for (File sd : subDirs) {
          dirs.add(new File(sd, SRC_DB_DATABASE_MODEL_TABLES));
        }
      }
      dirs.add(new File(modBase, SRC_DB_DATABASE_MODEL_MODIFIED_TABLES));
      if (subDirs != null) {
        for (File sd : subDirs) {
          dirs.add(new File(sd, SRC_DB_DATABASE_MODEL_MODIFIED_TABLES));
        }
      }
    }
    dirs.add(new File(root, SRC_DB_DATABASE_MODEL_TABLES));
    return dirs.stream().filter(File::isDirectory).collect(Collectors.toList());
  }

  /**
   * Collects table directories; returns empty list (not null) on failure and logs a warning.
   * 
   * @return a list of table directories, or an empty list if collection fails
   */
  public List<File> collectTableDirsSafe() {
    try {
      return collectTableDirs();
    } catch (Exception e) {
      log4j.warn("Could not collect table dirs: {}", e.getMessage());
      return List.of();
    }
  }

  /**
   * Lists {@code .xml} files inside {@code dir}. Returns an empty array if none/inaccessible.
   * 
   * @param dir the directory to scan for XML files
   * @return an array of XML files, or an empty array if none found or directory inaccessible
   */
  public File[] listXmlFiles(File dir) {
    File[] files = dir.listFiles(f -> f.isFile() && f.getName().endsWith(".xml"));
    return files != null ? files : new File[0];
  }

  /**
   * Returns the single {@code <table>} element if exactly one exists; otherwise {@code null}.
   * 
   * @param doc the DOM document to search for table elements
   * @return the single table element if found, or null if none or multiple found
   */
  public Element singleTableElementOrNull(Document doc) {
    NodeList tables = doc.getElementsByTagName(TABLE);
    if (tables.getLength() != 1) return null;
    return (Element) tables.item(0);
  }

  /**
   * True if the element represents a view or is the same as the target.
   * 
   * @param tableEl the table element to check
   * @param targetTable the target table name for comparison
   * @return true if the table element should be skipped during processing
   */
  public boolean shouldSkipTableElement(Element tableEl, String targetTable) {
    if (Boolean.parseBoolean(tableEl.getAttribute("isView"))) return true;
    return targetTable.equalsIgnoreCase(tableEl.getAttribute("name"));
  }

  /**
   * True if the {@code <foreign-key>} points to {@code targetTable}.
   * 
   * @param fkEl the foreign key element to check
   * @param targetTable the target table name to compare against
   * @return true if the foreign key references the target table
   */
  public boolean referencesTarget(Element fkEl, String targetTable) {
    return targetTable.equalsIgnoreCase(fkEl.getAttribute("foreignTable"));
  }

  /**
   * Returns the first {@code local} attribute of a {@code <reference>} child (single-column FK).
   * 
   * @param fkEl the foreign key element to extract the local column from
   * @return the first local column name, or null if no reference elements found
   */
  public String firstLocalColumn(Element fkEl) {
    NodeList refList = fkEl.getElementsByTagName("reference");
    if (refList.getLength() == 0) return null;
    Element refEl = (Element) refList.item(0);
    return refEl.getAttribute("local");
  }

  private boolean shouldScan(File xml, Timestamp lastProcessed) {
    boolean inModified = StringUtils.contains(xml.getAbsolutePath(), "modifiedTables");
    if (inModified) return true;
    if (lastProcessed == null) return true;
    return xml.lastModified() > lastProcessed.getTime();
  }

  private boolean fileReferencesTable(File xml, String tableName) {
    try {
      Document doc = getDocument(xml);
      return hasForeignKeyRef(doc, tableName, xml);
    } catch (Exception e) {
      logUnparseableXML(xml, e);
      return false;
    }
  }

  private boolean hasForeignKeyRef(Document doc, String tableName, File sourceXml) {
    NodeList fkList = doc.getElementsByTagName(FOREIGN_KEY);
    for (int i = 0; i < fkList.getLength(); i++) {
      Element fkEl = (Element) fkList.item(i);
      if (foreignKeyElementMatches(fkEl, tableName, sourceXml)) {
        return true;
      }
    }
    log4j.debug("Scanned {} and found {} foreign-key elements",
        sourceXml.getAbsolutePath(), fkList.getLength());
    return false;
  }

  private boolean foreignKeyElementMatches(Element fkEl, String tableName, File sourceXml) {
    String foreignTable = fkEl.getAttribute("foreignTable");
    if (tableName.equalsIgnoreCase(foreignTable)) {
      log4j.debug("Found external FK reference to {} in file {} (attribute foreignTable)",
          tableName, sourceXml.getAbsolutePath());
      return true;
    }
    NamedNodeMap attrs = fkEl.getAttributes();
    for (int a = 0; a < attrs.getLength(); a++) {
      String attrName = attrs.item(a).getNodeName();
      String attrVal = attrs.item(a).getNodeValue();
      if (tableName.equalsIgnoreCase(attrVal)) {
        log4j.debug("Found external FK reference to {} in file {} (attribute {})",
            tableName, sourceXml.getAbsolutePath(), attrName);
        return true;
      }
    }
    return false;
  }

  private boolean fileNameMatches(File xml, String targetLower) {
    return (xml != null) && xml.getName().equalsIgnoreCase(targetLower + ".xml");
  }

  private boolean containsTableDefinition(File xml, String tableName) {
    try {
      Document doc = getDocument(xml);
      NodeList tableNodes = doc.getElementsByTagName(TABLE);
      return nodeListHasTableName(tableNodes, tableName);
    } catch (Exception e) {
      logUnparseableXML(xml, e);
      return false;
    }
  }

  private boolean nodeListHasTableName(NodeList tableNodes, String tableName) {
    for (int i = 0; i < tableNodes.getLength(); i++) {
      Element el = (Element) tableNodes.item(i);
      if (tableName.equalsIgnoreCase(el.getAttribute("name"))) {
        return true;
      }
    }
    return false;
  }

  private String normalizeLower(String s) {
    return s == null ? null : s.toLowerCase(Locale.ROOT);
  }
}