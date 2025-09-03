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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openbravo.database.ConnectionProvider;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * Utility class to compare the column definitions of a database table
 * against one or more XML definitions.
 * <p>
 * It is used to detect schema changes in a given table, comparing live database metadata
 * with XML-based structure definitions.
 */
public class TableDefinitionComparator {

  private static final Logger logger = LogManager.getLogger();

  /**
   * Compares the database definition of a table with the merged XML definitions.
   *
   * @param tableName
   *     the name of the table to compare
   * @param cp
   *     a valid Openbravo ConnectionProvider
   * @param xmlFiles
   *     list of XML files defining the table structure
   * @return {@code true} if the definitions differ, {@code false} if they match
   * @throws Exception
   *     if any error occurs while parsing XML or querying the database
   */
  public boolean isTableDefinitionChanged(String tableName, ConnectionProvider cp,
      List<File> xmlFiles) throws Exception {
    // If no XML files are provided, consider the definition changed so that
    // installations that introduce new XML files will be processed by the ModuleScript.
    if (xmlFiles == null || xmlFiles.isEmpty()) {
      logger.debug("No XML files for table {}: treating as changed to allow creation/apply during install.", tableName);
      return true;
    }

    Map<String, ColumnDefinition> xmlColumns = new LinkedHashMap<>();
    for (File xmlFile : xmlFiles) {
      Map<String, ColumnDefinition> partial = parseXmlDefinition(xmlFile);
      for (Map.Entry<String, ColumnDefinition> entry : partial.entrySet()) {
        xmlColumns.putIfAbsent(entry.getKey(), entry.getValue());
      }
    }
    logger.debug("XML - Columns from {}: {}", tableName, xmlColumns);
    Map<String, ColumnDefinition> dbColumns = fetchDbDefinition(tableName, cp);
    logger.debug("DB - Columns from {}: {}", tableName, dbColumns);
    return !xmlColumns.equals(dbColumns);
  }

  /**
   * Compares the column definitions of a database table against the definitions
   * provided in a list of XML files and identifies differences.
   * <p>
   * The method aggregates all column definitions found in the given XML files
   * (ignoring duplicates by keeping the first occurrence), then fetches the
   * current column definitions from the database. It determines which columns
   * need to be added or removed to align the database table with the XML definitions.
   *
   * @param tableName
   *     the name of the database table to compare
   * @param cp
   *     the connection provider used to access the database
   * @param xmlFiles
   *     a list of XML files containing column definitions
   * @return a {@link ColumnDiff} object containing:
   *     <ul>
   *       <li>columns to add (present in XML but not in the database)</li>
   *       <li>columns to remove (present in the database but not in XML)</li>
   *     </ul>
   * @throws Exception
   *     if an error occurs while parsing XML files or fetching database definitions
   */
  public ColumnDiff diffTableDefinition(String tableName, ConnectionProvider cp, List<File> xmlFiles) throws Exception {
    Map<String, ColumnDefinition> xmlColumns = new LinkedHashMap<>();
    for (File xmlFile : xmlFiles) {
      Map<String, ColumnDefinition> partial = parseXmlDefinition(xmlFile);
      for (Map.Entry<String, ColumnDefinition> entry : partial.entrySet()) {
        xmlColumns.putIfAbsent(entry.getKey(), entry.getValue());
      }
    }
    Map<String, ColumnDefinition> dbColumns = fetchDbDefinition(tableName, cp);
    Map<String, ColumnDefinition> toAdd = new LinkedHashMap<>();
    Map<String, ColumnDefinition> toRemove = new LinkedHashMap<>();
    for (Map.Entry<String, ColumnDefinition> e : xmlColumns.entrySet()) {
      if (!dbColumns.containsKey(e.getKey())) toAdd.put(e.getKey(), e.getValue());
    }
    for (Map.Entry<String, ColumnDefinition> e : dbColumns.entrySet()) {
      if (!xmlColumns.containsKey(e.getKey())) toRemove.put(e.getKey(), e.getValue());
    }
    return new ColumnDiff(toAdd, toRemove);
  }

  /**
   * Parses the given XML file to extract column definitions.
   * <p>
   * The method expects the XML to contain a set of {@code <column>} elements,
   * each with attributes such as {@code name}, {@code type}, {@code nullable},
   * {@code length} or {@code size}, and optionally {@code primarykey}.
   *
   * @param xmlFile
   *     the XML file that defines the table structure
   * @return a map of column names to their corresponding {@link ColumnDefinition}
   * @throws ParserConfigurationException
   *     if a DocumentBuilder cannot be created
   * @throws IOException
   *     if an I/O error occurs while reading the file
   * @throws SAXException
   *     if the XML content is malformed or cannot be parsed
   */
  private Map<String, ColumnDefinition> parseXmlDefinition(File xmlFile) throws ParserConfigurationException,
      IOException, SAXException {
    Map<String, ColumnDefinition> columns = new LinkedHashMap<>();
    DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
    dbFactory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
    dbFactory.setFeature("http://xml.org/sax/features/external-general-entities", false);
    dbFactory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
    dbFactory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
    dbFactory.setXIncludeAware(false);
    dbFactory.setExpandEntityReferences(false);
    DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
    Document doc = dBuilder.parse(xmlFile);
    doc.getDocumentElement().normalize();
    NodeList columnList = doc.getElementsByTagName("column");
    for (int i = 0; i < columnList.getLength(); i++) {
      Element colElem = (Element) columnList.item(i);
      String name = colElem.getAttribute("name").toLowerCase();
      String dataType = colElem.getAttribute("type").toLowerCase();
      Boolean isNullable = !"false".equalsIgnoreCase(colElem.getAttribute("nullable"));
      Integer length = null;
      if (colElem.hasAttribute("length")) {
        length = parseIntLenient(colElem.getAttribute("length"));
      } else if (colElem.hasAttribute("size")) {
        length = parseIntLenient(colElem.getAttribute("size"));
      }
      Boolean isPrimaryKey = StringUtils.equalsIgnoreCase("true", colElem.getAttribute("primarykey"));
      columns.put(name, new ColumnDefinition(name, dataType, length, isNullable, isPrimaryKey));
    }
    return columns;
  }

  /**
   * Parses a string into an integer, using a lenient approach.
   * <p>
   * If the input string is blank (null, empty, or only whitespace), returns 0.
   * If the string contains a decimal separator (e.g., "123,45" or "123.45"), it replaces commas with dots,
   * trims whitespace, and attempts to parse the value. If parsing as an integer fails, it tries parsing as a double
   * and then casts the result to an integer (truncating the decimal part).
   * </p>
   *
   * @param raw
   *     the raw input string to parse
   * @return the parsed integer value, or 0 if the input is blank
   */
  private int parseIntLenient(String raw) {
    if (StringUtils.isBlank(raw)) {
      return 0;
    }
    String cleaned = StringUtils.replace(StringUtils.trim(raw), ",", ".");
    try {
      return Integer.parseInt(cleaned);
    } catch (NumberFormatException e) {
      return (int) Math.round(Double.parseDouble(cleaned));
    }
  }

  /**
   * Retrieves column definitions for a given table from the database.
   *
   * @param tableName
   *     the table to inspect
   * @param cp
   *     a valid Openbravo ConnectionProvider
   * @return a map of column names to their definitions
   * @throws Exception
   *     if a database error occurs
   */
  private Map<String, ColumnDefinition> fetchDbDefinition(String tableName, ConnectionProvider cp) throws Exception {
    Map<String, ColumnDefinition> columns = new LinkedHashMap<>();
    String columnSql = "SELECT column_name, data_type, character_maximum_length, is_nullable " +
        "FROM information_schema.columns " +
        "WHERE table_schema = 'public' AND table_name = ? AND column_name NOT LIKE 'etarc_%'";
    String pkSql = "SELECT kcu.column_name " +
        "FROM information_schema.table_constraints tc " +
        "JOIN information_schema.key_column_usage kcu " +
        "  ON tc.constraint_name = kcu.constraint_name " +
        "WHERE tc.table_schema = 'public' " +
        "  AND tc.table_name = ? " +
        "  AND tc.constraint_type = 'PRIMARY KEY'";
    Set<String> primaryKeys = new HashSet<>();
    try (PreparedStatement ps = cp.getPreparedStatement(pkSql)) {
      ps.setString(1, tableName.toLowerCase());
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          primaryKeys.add(rs.getString("column_name").toLowerCase());
        }
      }
    }
    try (PreparedStatement ps = cp.getPreparedStatement(columnSql)) {
      ps.setString(1, tableName.toLowerCase());
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          String name = rs.getString("column_name").toLowerCase();
          String dataType = rs.getString("data_type").toLowerCase();
          Integer length = rs.getObject("character_maximum_length") != null
              ? rs.getInt("character_maximum_length") : null;
          Boolean isNullable = "YES".equalsIgnoreCase(rs.getString("is_nullable"));
          Boolean isPrimaryKey = primaryKeys.contains(name);
          columns.put(name, new ColumnDefinition(name, dataType, length, isNullable, isPrimaryKey));
        }
      }
    }
    return columns;
  }

  /**
   * Represents the differences between two sets of column definitions
   * when comparing a database table with its XML specification.
   * <p>
   * The class contains two maps:
   * <ul>
   *   <li>{@code added}: columns that are present in XML but missing in the database</li>
   *   <li>{@code removed}: columns that are present in the database but missing in XML</li>
   * </ul>
   */
  public static class ColumnDiff {
    /**
     * Columns that exist in the XML specification but are missing in the database.
     * <p>
     * Each entry maps the column name to its {@link ColumnDefinition}.
     * </p>
     */
    public final Map<String, ColumnDefinition> added;

    /**
     * Columns that exist in the database but are missing in the XML specification.
     * <p>
     * Each entry maps the column name to its {@link ColumnDefinition}.
     * </p>
     */
    public final Map<String, ColumnDefinition> removed;

    /**
     * Creates a new {@code ColumnDiff} instance with the given added and removed columns.
     *
     * @param added
     *     the set of columns to be added to the database
     * @param removed
     *     the set of columns to be removed from the database
     */
    public ColumnDiff(Map<String, ColumnDefinition> added, Map<String, ColumnDefinition> removed) {
      this.added = added;
      this.removed = removed;
    }
  }

  /**
   * Internal class representing the structure of a column in a table.
   */
  public static class ColumnDefinition {
    String name;
    String dataType;
    Integer length;
    Boolean isNullable;
    Boolean isPrimaryKey;

    /**
     * Constructor for ColumnDefinition.
     *
     * @param name
     *     the column name
     * @param dataType
     *     the SQL data type
     * @param length
     *     the length if applicable (nullable)
     * @param isNullable
     *     whether the column allows NULL
     * @param isPrimaryKey
     *     whether the column is part of the primary key
     */
    public ColumnDefinition(String name, String dataType, Integer length, Boolean isNullable, Boolean isPrimaryKey) {
      this.name = name;
      this.dataType = dataType;
      this.length = length;
      this.isNullable = isNullable;
      this.isPrimaryKey = isPrimaryKey;
    }

    public String getName() {
      return name;
    }

    // New getters for other properties to allow external callers to build SQL
    public String getDataType() {
      return dataType;
    }

    public Integer getLength() {
      return length;
    }

    public Boolean isNullable() {
      return isNullable;
    }

    public Boolean isPrimaryKey() {
      return isPrimaryKey;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof ColumnDefinition that)) return false;
      return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name);
    }
  }
}
