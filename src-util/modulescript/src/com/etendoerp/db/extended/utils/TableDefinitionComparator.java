package com.etendoerp.db.extended.utils;

import org.openbravo.database.ConnectionProvider;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.*;
import java.io.*;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xml.sax.*;

public class TableDefinitionComparator {

  private static final Logger logger = LogManager.getLogger();

  public boolean isTableDefinitionChanged(String tableName, ConnectionProvider cp, List<File> xmlFiles) throws Exception {
    Map<String, ColumnDefinition> xmlColumns = new LinkedHashMap<>();
    for (File xmlFile : xmlFiles) {
      Map<String, ColumnDefinition> partial = parseXmlDefinition(xmlFile);
      for (Map.Entry<String, ColumnDefinition> entry : partial.entrySet()) {
        xmlColumns.putIfAbsent(entry.getKey(), entry.getValue());
      }
    }
    logger.debug("XML - Columns from " + tableName + ": " + xmlColumns);

    Map<String, ColumnDefinition> dbColumns = fetchDbDefinition(tableName, cp);
    logger.debug("DB - Columns from " + tableName + ": " + dbColumns);

    return !xmlColumns.equals(dbColumns);
  }

  /**
   * Parsea atributos numéricos de forma “leniente”: acepta tanto "10" como "10,0" o "10.0".
   */
  private int parseIntLenient(String raw) {
    if (raw == null || raw.trim().isEmpty()) {
      return 0;
    }
    String cleaned = raw.trim().replace(',', '.');
    try {
      return Integer.parseInt(cleaned);
    } catch (NumberFormatException e) {
      // Si trae parte decimal, parseamos como double y devolvemos la parte entera
      return (int) Double.parseDouble(cleaned);
    }
  }

  /**
   * Parses the given XML file to extract column definitions.
   * <p>
   * The method expects the XML to contain a set of {@code <column>} elements,
   * each with attributes such as {@code name}, {@code type}, {@code nullable},
   * {@code length} or {@code size}, and optionally {@code primarykey}.
   *
   * @param xmlFile the XML file that defines the table structure
   * @return a map of column names to their corresponding {@link ColumnDefinition}
   *
   * @throws ParserConfigurationException if a DocumentBuilder cannot be created
   * @throws IOException if an I/O error occurs while reading the file
   * @throws SAXException if the XML content is malformed or cannot be parsed
   */
  private Map<String, ColumnDefinition> parseXmlDefinition(File xmlFile)
      throws ParserConfigurationException, IOException, SAXException {
    Map<String, ColumnDefinition> columns = new LinkedHashMap<>();

    DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
    DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
    Document doc = dBuilder.parse(xmlFile);
    doc.getDocumentElement().normalize();

    NodeList columnList = doc.getElementsByTagName("column");

    for (int i = 0; i < columnList.getLength(); i++) {
      Element colElem = (Element) columnList.item(i);
      String name        = colElem.getAttribute("name").toLowerCase();
      String dataType    = colElem.getAttribute("type").toLowerCase();
      Boolean isNullable = !"false".equalsIgnoreCase(colElem.getAttribute("nullable"));
      Integer length = null;
      if (colElem.hasAttribute("length")) {
        length = parseIntLenient(colElem.getAttribute("length"));
      } else if (colElem.hasAttribute("size")) {
        length = parseIntLenient(colElem.getAttribute("size"));
      }

      Boolean isPrimaryKey = "true".equalsIgnoreCase(colElem.getAttribute("primarykey"));

      columns.put(name, new ColumnDefinition(name, dataType, length, isNullable, isPrimaryKey));
    }
    return columns;
  }

  private Map<String, ColumnDefinition> fetchDbDefinition(String tableName, ConnectionProvider cp) throws Exception {
    Map<String, ColumnDefinition> columns = new LinkedHashMap<>();

    // Consulta columnas excluyendo las que empiezan con etarc_
    String columnSql = "SELECT column_name, data_type, character_maximum_length, is_nullable " +
        "FROM information_schema.columns " +
        "WHERE table_schema = 'public' AND table_name = ? AND column_name NOT LIKE 'etarc_%'";

    // Consulta claves primarias
    String pkSql = "SELECT kcu.column_name " +
        "FROM information_schema.table_constraints tc " +
        "JOIN information_schema.key_column_usage kcu " +
        "  ON tc.constraint_name = kcu.constraint_name " +
        "WHERE tc.table_schema = 'public' " +
        "  AND tc.table_name = ? " +
        "  AND tc.constraint_type = 'PRIMARY KEY'";

    // Obtener columnas PK
    Set<String> primaryKeys = new HashSet<>();
    try (PreparedStatement ps = cp.getPreparedStatement(pkSql)) {
      ps.setString(1, tableName.toLowerCase());
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          primaryKeys.add(rs.getString("column_name").toLowerCase());
        }
      }
    }

    // Obtener columnas con sus definiciones
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


  public static class ColumnDefinition {
    String name;
    String dataType;
    Integer length;
    Boolean isNullable;
    Boolean isPrimaryKey;

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

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof ColumnDefinition)) return false;
      ColumnDefinition that = (ColumnDefinition) o;
      return Objects.equals(name, that.name);
//          && Objects.equals(dataType, that.dataType)
//          && Objects.equals(length, that.length)
//          && Objects.equals(isNullable, that.isNullable)
//          && Objects.equals(isPrimaryKey, that.isPrimaryKey);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, dataType, length, isNullable, isPrimaryKey);
    }
  }
}
