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
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * Manages trigger creation and cleanup for partitioned tables to automatically
 * populate partition columns in child tables.
 */
public class TriggerManager {
  private static final Logger log4j = LogManager.getLogger();
  private static final String FOREIGN_KEY = "foreign-key";

  // Trigger templates for automatic partition column population
  private static final String DROP_TRIGGER =
      "DROP TRIGGER IF EXISTS %s ON %s;\n";
  private static final String DROP_FUNCTION =
      "DROP FUNCTION IF EXISTS %s();\n";
  private static final String CREATE_TRIGGER_FUNCTION =
      "CREATE OR REPLACE FUNCTION %s()\n" +
          "RETURNS TRIGGER AS $$\n" +
          "BEGIN\n" +
          "    -- Auto-populate partition column from parent table\n" +
          "    IF NEW.%s IS NULL THEN\n" +
          "        SELECT %s INTO NEW.%s\n" +
          "        FROM %s\n" +
          "        WHERE %s = NEW.%s;\n" +
          "    END IF;\n" +
          "    \n" +
          "    RETURN NEW;\n" +
          "END;\n" +
          "$$ LANGUAGE plpgsql;\n";
  private static final String CREATE_TRIGGER =
      "CREATE TRIGGER %s\n" +
          "    BEFORE INSERT ON %s\n" +
          "    FOR EACH ROW\n" +
          "    EXECUTE FUNCTION %s();\n";

  private final XmlTableProcessor xmlProcessor;

  public TriggerManager(XmlTableProcessor xmlProcessor) {
    this.xmlProcessor = xmlProcessor;
  }

  /**
   * Creates trigger SQL for child tables to automatically populate partition columns.
   */
  public void appendTriggerSql(StringBuilder sql, SqlBuilder.FkContext ctx) {
    if (!ctx.isParentPartitioned()) {
      return; // Only create triggers for partitioned parent tables
    }

    Set<String> processedChildTables = new HashSet<>();
    processTablesForTriggers(sql, ctx, processedChildTables);
  }

  private void processTablesForTriggers(StringBuilder sql, SqlBuilder.FkContext ctx, Set<String> processedChildTables) {
    for (File dir : xmlProcessor.collectTableDirsSafe()) {
      for (File xml : xmlProcessor.listXmlFiles(dir)) {
        processXmlForTriggers(sql, ctx, xml, processedChildTables);
      }
    }
  }

  private void processXmlForTriggers(StringBuilder sql, SqlBuilder.FkContext ctx, File xml,
      Set<String> processedChildTables) {
    try {
      Document doc = XmlTableProcessor.getDocument(xml);
      Element tableEl = xmlProcessor.singleTableElementOrNull(doc);
      if (shouldSkipTableForTrigger(tableEl, ctx.getParentTable())) {
        return;
      }

      String childTable = tableEl.getAttribute("name").toUpperCase();
      if (processedChildTables.contains(childTable)) {
        return;
      }

      ForeignKeyInfo fkInfo = findFirstValidForeignKey(tableEl, ctx.getParentTable());
      if (fkInfo.isValid()) {
        appendTriggerForChild(sql, ctx, childTable, fkInfo.getName(), fkInfo.getLocalColumn());
        processedChildTables.add(childTable);
      }
    } catch (Exception e) {
      log4j.error("Error processing XML file for triggers: {}", xml.getAbsolutePath(), e);
    }
  }

  private boolean shouldSkipTableForTrigger(Element tableEl, String parentTable) {
    return tableEl == null || xmlProcessor.shouldSkipTableElement(tableEl, parentTable);
  }

  private ForeignKeyInfo findFirstValidForeignKey(Element tableEl, String parentTable) {
    NodeList fkList = tableEl.getElementsByTagName(FOREIGN_KEY);

    for (int i = 0; i < fkList.getLength(); i++) {
      Element fkEl = (Element) fkList.item(i);
      if (!xmlProcessor.referencesTarget(fkEl, parentTable)) {
        continue;
      }

      String fkName = fkEl.getAttribute("name");
      String localCol = xmlProcessor.firstLocalColumn(fkEl);
      if (!StringUtils.isBlank(fkName) && !StringUtils.isBlank(localCol)) {
        return new ForeignKeyInfo(fkName, localCol);
      }
    }

    return new ForeignKeyInfo(null, null);
  }

  /**
   * Appends trigger creation SQL for a specific child table.
   */
  private void appendTriggerForChild(StringBuilder sql, SqlBuilder.FkContext ctx, String childTable,
      String fkName, String localCol) {
    String helperCol = "etarc_" + ctx.getPartitionField() + "__" + fkName;
    String triggerName = "etarc_partition_trigger_" + childTable.toLowerCase();
    String functionName = "etarc_auto_partition_" + childTable.toLowerCase();

    // Always drop and recreate to ensure trigger is current
    sql.append(String.format(DROP_TRIGGER, triggerName, childTable));
    sql.append(String.format(DROP_FUNCTION, functionName));

    // Create function that populates the partition column
    sql.append(String.format(CREATE_TRIGGER_FUNCTION,
        functionName,           // function name
        helperCol,             // NEW.helper_column (check if null)
        ctx.getPartitionField(),    // parent.partition_field (select this)
        helperCol,             // NEW.helper_column (assign to this)
        ctx.getParentTable(),       // parent table name
        ctx.getPkField(),          // parent.pk_field (where condition)
        localCol              // NEW.local_column (where value)
    ));

    // Create trigger
    sql.append(String.format(CREATE_TRIGGER,
        triggerName,          // trigger name
        childTable,           // child table name
        functionName          // function name
    ));

    log4j.info("Added trigger SQL for child table {} -> {} (partition column: {})",
        childTable, ctx.getParentTable(), helperCol);
  }
  
  /**
   * Immutable holder for foreign key information.
   */
  private static class ForeignKeyInfo {
    private final String name;
    private final String localColumn;

    public ForeignKeyInfo(String name, String localColumn) {
      this.name = name;
      this.localColumn = localColumn;
    }

    public boolean isValid() {
      return name != null && localColumn != null;
    }

    public String getName() {
      return name;
    }

    public String getLocalColumn() {
      return localColumn;
    }
  }


}