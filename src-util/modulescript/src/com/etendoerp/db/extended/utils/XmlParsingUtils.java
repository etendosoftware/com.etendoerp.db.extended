package com.etendoerp.db.extended.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility class for XML parsing operations related to table definitions.
 * Handles secure XML processing with XXE protection and provides methods
 * for extracting table structure information from XML files.
 */
public class XmlParsingUtils {

  private static final Logger log4j = LogManager.getLogger();

  /**
   * Represents a column definition from XML.
   */
  public record ColumnDefinition(String name, String dataType, Integer length, Boolean isNullable,
                                 Boolean isPrimaryKey) {
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
   * @return a normalized {@link Document} representing the XML content
   * @throws ParserConfigurationException if a parser cannot be configured
   * @throws SAXException                 if a parsing error occurs
   * @throws IOException                  if an I/O error occurs reading the file
   */
  public static Document getDocument(File xml) throws ParserConfigurationException, SAXException, IOException {
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
   *       returns that attribute's value.</li>
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
   *   <li>The module's own "src-db/database/model/tables" directory, if present.</li>
   *   <li>That same tables directory under each immediate subdirectory of the module.</li>
   * </ul>
   * Finally, adds the project‐root "src-db/database/model/tables" directory.
   * Only existing directories are returned.
   *
   * @param sourcePath the source path of the project
   * @return a List of File objects representing each valid tables directory
   */
  public static List<File> collectTableDirs(String sourcePath) throws NoSuchFileException {
    List<File> dirs = new ArrayList<>();
    File root = new File(sourcePath);
    for (String mod : Constants.MODULE_DIRS) {
      File modBase = new File(root, mod);
      if (!modBase.isDirectory()) continue;
      dirs.add(new File(modBase, Constants.SRC_DB_DATABASE_MODEL_TABLES));
      for (File sd : Objects.requireNonNull(modBase.listFiles(File::isDirectory))) {
        dirs.add(new File(sd, Constants.SRC_DB_DATABASE_MODEL_TABLES));
      }
      dirs.add(new File(modBase, Constants.SRC_DB_DATABASE_MODEL_MODIFIED_TABLES));
      for (File sd : Objects.requireNonNull(modBase.listFiles(File::isDirectory))) {
        dirs.add(new File(sd, Constants.SRC_DB_DATABASE_MODEL_MODIFIED_TABLES));
      }
    }
    dirs.add(new File(root, Constants.SRC_DB_DATABASE_MODEL_TABLES));
    return dirs.stream().filter(File::isDirectory).collect(Collectors.toList());
  }

  /**
   * Finds the .xml file(s) matching the given table name (case-insensitive)
   * under each module's "tables" directory and under the project's root.
   * <p>
   * Constructs a target filename of the form {@code tableName + ".xml"}, then
   * filters all XMLs in the discovered directories to only those whose name
   * equals the target.
   *
   * @param tableName  the base name of the table (without the .xml extension)
   * @param sourcePath the source path of the project
   * @return a List of matching XML files (maybe empty if none found)
   */
  public static List<File> findTableXmlFiles(String tableName, String sourcePath) throws NoSuchFileException {
    String target = tableName.toLowerCase() + ".xml";
    return collectTableDirs(sourcePath).stream()
        .flatMap(dir -> {
          File[] files = dir.listFiles(f -> f.isFile() && f.getName().endsWith(".xml"));
          return files == null ? Stream.empty() : Arrays.stream(files);
        })
        .filter(f -> f.isFile() && f.getName().equalsIgnoreCase(target))
        .collect(Collectors.toList());
  }

  /**
   * Parses XML definition to extract column information.
   *
   * @param xmlFile the XML file to parse
   * @return a map of column name to ColumnDefinition
   * @throws Exception if parsing fails
   */
  public static Map<String, ColumnDefinition> parseXmlDefinition(File xmlFile) throws Exception {
    Map<String, ColumnDefinition> columns = new LinkedHashMap<>();

    if (!xmlFile.exists()) {
      log4j.warn("XML file does not exist: {}", xmlFile.getAbsolutePath());
      return columns;
    }

    try {
      Document doc = getDocument(xmlFile);
      NodeList columnNodes = doc.getElementsByTagName("column");

      for (int i = 0; i < columnNodes.getLength(); i++) {
        Element column = (Element) columnNodes.item(i);

        String name = column.getAttribute("name");
        String type = column.getAttribute("type");
        String lengthStr = column.getAttribute("size");
        String nullableStr = column.getAttribute("mandatory");
        String primaryKeyStr = column.getAttribute("primaryKey");

        if (name.isEmpty()) {
          continue; // Skip columns without names
        }

        Integer length = null;
        if (!lengthStr.isEmpty()) {
          try {
            length = parseIntLenient(lengthStr);
          } catch (NumberFormatException e) {
            log4j.warn("Invalid length '{}' for column '{}' in {}", lengthStr, name, xmlFile.getName());
          }
        }

        Boolean isNullable = nullableStr.isEmpty() ? null : !"Y".equalsIgnoreCase(nullableStr);
        Boolean isPrimaryKey = "Y".equalsIgnoreCase(primaryKeyStr);

        ColumnDefinition columnDef = new ColumnDefinition(name, type, length, isNullable, isPrimaryKey);
        columns.put(name.toUpperCase(), columnDef);
      }

    } catch (Exception e) {
      log4j.error("Error parsing XML file {}: {}", xmlFile.getAbsolutePath(), e.getMessage());
      throw e;
    }

    return columns;
  }

  /**
   * Helper method for lenient integer parsing.
   *
   * @param raw the string to parse
   * @return the parsed integer
   */
  private static int parseIntLenient(String raw) {
    if (raw == null || raw.trim().isEmpty()) {
      return 0;
    }

    try {
      // Remove any non-digit characters except minus sign
      String cleaned = raw.replaceAll("[^\\d-]", "");
      if (cleaned.isEmpty()) {
        return 0;
      }
      return Integer.parseInt(cleaned);
    } catch (NumberFormatException e) {
      log4j.warn("Could not parse '{}' as integer, defaulting to 0", raw);
      return 0;
    }
  }

  /**
   * Maps XML column types to PostgreSQL types.
   *
   * @param xmlType the XML column type
   * @param length  the column length (can be null)
   * @return the PostgreSQL equivalent type
   */
  public static String mapXmlTypeToPostgreSQL(String xmlType, Integer length) {
    if (xmlType == null || xmlType.trim().isEmpty()) {
      return "TEXT";
    }

    String type = xmlType.toUpperCase().trim();

    switch (type) {
      case "ID":
      case "TABLEDIR":
      case "TABLE":
        return "VARCHAR(32)";
      case "SEARCH":
        return "VARCHAR(2000)";
      case "STRING":
        if (length != null && length > 0) {
          return "VARCHAR(" + length + ")";
        }
        return "VARCHAR(255)";
      case "TEXT":
        return "TEXT";
      case "MEMO":
        return "TEXT";
      case "INTEGER":
        return "NUMERIC(10)";
      case "NUMERIC":
        if (length != null && length > 0) {
          return "NUMERIC(" + length + ")";
        }
        return "NUMERIC";
      case "AMOUNT":
        return "NUMERIC";
      case "QUANTITY":
        return "NUMERIC";
      case "PRICE":
        return "NUMERIC";
      case "DATE":
        return "DATE";
      case "DATETIME":
      case "TIMESTAMP":
        return "TIMESTAMP WITHOUT TIME ZONE";
      case "TIME":
        return "TIME WITHOUT TIME ZONE";
      case "BOOLEAN":
      case "YESNO":
        return "VARCHAR(1)";
      case "LIST":
        if (length != null && length > 0) {
          return "VARCHAR(" + length + ")";
        }
        return "VARCHAR(60)";
      case "BINARY":
      case "IMAGE":
        return "BYTEA";
      default:
        log4j.warn("Unknown XML type '{}', defaulting to TEXT", xmlType);
        return "TEXT";
    }
  }

  /**
   * Generates a CREATE TABLE statement based on XML definitions.
   *
   * @param tableName    the name of the table to create
   * @param xmlFiles     list of XML files defining the table structure
   * @param partitionCol the column to partition by
   * @return the complete CREATE TABLE SQL statement
   * @throws Exception if parsing XML fails
   */
  public static String generateCreateTableFromXml(String tableName, List<File> xmlFiles, String partitionCol) throws Exception {
    // Parse all XML files to get the complete column definition
    Map<String, ColumnDefinition> xmlColumns = new LinkedHashMap<>();

    for (File xmlFile : xmlFiles) {
      Map<String, ColumnDefinition> fileColumns = parseXmlDefinition(xmlFile);
      xmlColumns.putAll(fileColumns);
    }

    if (xmlColumns.isEmpty()) {
      throw new Exception("No column definitions found in XML files for table " + tableName);
    }

    StringBuilder sql = new StringBuilder();
    sql.append("CREATE TABLE public.").append(tableName).append(" (\n");

    boolean first = true;
    for (ColumnDefinition column : xmlColumns.values()) {
      if (!first) {
        sql.append(",\n");
      }
      first = false;

      sql.append("  ").append(column.name());
      sql.append(" ").append(mapXmlTypeToPostgreSQL(column.dataType(), column.length()));

      if (column.isNullable() != null && !column.isNullable()) {
        sql.append(" NOT NULL");
      }
    }

    sql.append("\n) PARTITION BY RANGE (").append(partitionCol).append(")");

    return sql.toString();
  }
}
