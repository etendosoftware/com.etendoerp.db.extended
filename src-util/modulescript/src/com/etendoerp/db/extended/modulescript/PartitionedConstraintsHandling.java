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

import java.nio.file.NoSuchFileException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openbravo.database.ConnectionProvider;
import org.openbravo.modulescript.ModuleScript;

import com.etendoerp.db.extended.utils.BackupManager;
import com.etendoerp.db.extended.utils.ConstraintProcessor;
import com.etendoerp.db.extended.utils.SqlBuilder;
import com.etendoerp.db.extended.utils.TableDefinitionComparator;
import com.etendoerp.db.extended.utils.TriggerManager;
import com.etendoerp.db.extended.utils.XmlTableProcessor;

/**
 * Main module script for handling partitioned table constraints.
 * This class has been refactored to use specialized component classes
 * to comply with SonarQube's method count recommendations.
 */
public class PartitionedConstraintsHandling extends ModuleScript {

  public static final String SEPARATOR = "=======================================================";
  public static final String TABLE_NAME = "tableName";
  private static final Logger log4j = LogManager.getLogger();

  // Component instances
  private BackupManager backupManager;
  private XmlTableProcessor xmlProcessor;

  private SqlBuilder sqlBuilder;
  private ConstraintProcessor constraintProcessor;

  /**
   * Null/blank convenience, kept for readability in callers.
   */
  public static boolean isBlank(String str) {
    return StringUtils.isBlank(str);
  }

  /**
   * Logs a separator line at INFO, useful to visually split log sections.
   */
  private static void logSeparator() {
    log4j.info(SEPARATOR);
  }

  /**
   * Initializes the component instances.
   */
  private void initializeComponents() throws NoSuchFileException {
    TriggerManager triggerManager;
    this.backupManager = new BackupManager();
    this.xmlProcessor = new XmlTableProcessor(backupManager, getSourcePath());
    this.sqlBuilder = new SqlBuilder();
    triggerManager = new TriggerManager(xmlProcessor);
    this.constraintProcessor = new ConstraintProcessor(xmlProcessor, sqlBuilder, triggerManager);
  }

  @Override
  public void execute() {
    try {
      initializeComponents();
      ConnectionProvider cp = getConnectionProvider();
      List<Map<String, String>> tableConfigs = loadTableConfigs(cp);

      if (!tableConfigs.isEmpty()) {
        logSeparator();
        log4j.info("============== Partitioning process info ==============");
        logSeparator();
      }

      // Ensure backup infra and clear old backups before making changes
      backupManager.ensureBackupInfrastructure(cp);
      backupManager.cleanupExcessBackups(cp);

      Map<String, Long> timings = new LinkedHashMap<>();
      boolean hasPartitioned = quickCheckPartitioned(cp, tableConfigs);

      for (Map<String, String> cfg : tableConfigs) {
        String tName = cfg.get(TABLE_NAME);
        long elapsed = processTableSafely(cp, cfg);
        timings.put(tName, elapsed);
      }

      boolean anyProcessed = timings.values().stream().anyMatch(v -> v != null && v >= 0L);
      if (anyProcessed || hasPartitioned) {
        logSeparator();
        log4j.info("Partitioning processing summary (ms):");
        for (Map.Entry<String, Long> e : timings.entrySet()) {
          log4j.info(" - {} => {}", e.getKey(), e.getValue());
        }
      }
      if (!tableConfigs.isEmpty()) {
        logSeparator();
      }

    } catch (Exception e) {
      handleError(e);
    }
  }

  /**
   * Quickly checks whether any configured table is already partitioned.
   */
  private boolean quickCheckPartitioned(ConnectionProvider cp, List<Map<String, String>> tableConfigs) {
    try {
      for (Map<String, String> cfg : tableConfigs) {
        String tName = cfg.get(TABLE_NAME);
        if (safeIsTablePartitioned(cp, tName)) {
          return true;
        }
      }
    } catch (Exception ignore) {
      // ignore global failures
    }
    return false;
  }

  /**
   * Safe wrapper for checking if table is partitioned.
   */
  private boolean safeIsTablePartitioned(ConnectionProvider cp, String tableName) {
    try {
      return constraintProcessor.isTablePartitioned(cp, tableName);
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Processes one table configuration and returns elapsed time in milliseconds.
   */
  private long processTableSafely(ConnectionProvider cp, Map<String, String> cfg) {
    long start = System.currentTimeMillis();
    try {
      boolean processed = processTableConfig(cp, cfg);
      return processed ? (System.currentTimeMillis() - start) : -1L;
    } catch (Exception e) {
      return -1L;
    }
  }

  /**
   * Loads table configuration from {@code ETARC_TABLE_CONFIG}.
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
        cfg.put(TABLE_NAME, rs.getString("TABLENAME"));
        cfg.put("columnName", rs.getString("COLUMNNAME"));
        cfg.put("pkColumnName", rs.getString("PK_COLUMNNAME"));
        tableConfigs.add(cfg);
      }
    }
    return tableConfigs;
  }

  /**
   * Processes table configuration and decides whether constraints need to be recreated.
   */
  private boolean processTableConfig(ConnectionProvider cp, Map<String, String> cfg) throws Exception {
    String tableName = cfg.get(TABLE_NAME);
    String partitionCol = cfg.get("columnName");
    String pkCol = cfg.get("pkColumnName");

    log4j.info("DATA FROM ETARC_TABLE_CONFIG: tableName: {} - partitionCol: {} - pkCol: {}",
        tableName, partitionCol, pkCol);

    boolean isIncomplete = isBlank(tableName) || isBlank(partitionCol) || isBlank(pkCol);
    List<java.io.File> xmlFiles = isIncomplete ? java.util.Collections.emptyList() : xmlProcessor.findTableXmlFiles(
        tableName);

    boolean isUnchanged = !isIncomplete &&
        !(new TableDefinitionComparator()).isTableDefinitionChanged(tableName, cp, xmlFiles);

    // Check for external foreign key references
    if (isUnchanged) {
      try {
        if (xmlProcessor.hasForeignReferencesInXml(tableName, cp)) {
          log4j.info("Detected external XML foreign-key references to {} — forcing processing.", tableName);
          isUnchanged = false;
        }
      } catch (Exception e) {
        log4j.info("Failed to scan for external foreign-key references for {}: {}", tableName, e.getMessage());
      }
    }

    boolean isPartitioned = constraintProcessor.isTablePartitioned(cp, tableName);
    List<String> pkCols = constraintProcessor.getPrimaryKeyColumns(cp, tableName);
    boolean firstPartitionRun = isPartitioned && pkCols.isEmpty();

    log4j.info("Table {} partitioned = {} existing PK cols = {}", tableName, isPartitioned, pkCols);

    if (shouldSkipTable(isIncomplete, firstPartitionRun, isUnchanged)) {
      logSkipReason(isIncomplete, tableName, pkCol, partitionCol);
      return false;
    }

    log4j.info("Recreating constraints for {} (firstRun = {}, xmlChanged = {})",
        tableName, firstPartitionRun, !isUnchanged);

    String tableSql = constraintProcessor.buildConstraintSql(tableName, cp, pkCol, partitionCol);

    try {
      TableDefinitionComparator.ColumnDiff diff =
          (new TableDefinitionComparator()).diffTableDefinition(tableName, cp, xmlFiles);

      StringBuilder alterSql = sqlBuilder.getAlterSql(diff, tableName);

      if (alterSql.length() > 0) {
        backupManager.executeSqlWithBackup(cp, tableName, isPartitioned, alterSql.toString());
      }
      if (!isBlank(tableSql)) {
        backupManager.executeSqlWithBackup(cp, tableName, isPartitioned, tableSql);
      }
      return true;
    } catch (Exception e) {
      log4j.error("Failed to persist XML schema changes for {}: {}", tableName, e.getMessage(), e);
      // Fallback: still try constraint SQL
      try {
        backupManager.executeSqlWithBackup(cp, tableName, isPartitioned, tableSql);
      } catch (Exception e2) {
        log4j.error("Failed executing fallback constraint SQL for {}: {}", tableName, e2.getMessage(), e2);
      }
      return false;
    }
  }

  /**
   * Skip logic for table processing.
   */
  private boolean shouldSkipTable(boolean isIncomplete, boolean firstPartitionRun, boolean isUnchanged) {
    return isIncomplete || (!firstPartitionRun && isUnchanged);
  }

  /**
   * Logs the reason a table was skipped.
   */
  private void logSkipReason(boolean isIncomplete, String tableName, String pkCol, String partitionCol) {
    if (isIncomplete) {
      log4j.warn("Skipping incomplete configuration for table {} (pk = {}, partition = {})",
          tableName, pkCol, partitionCol);
    } else {
      log4j.info("Skipping {}: already processed and no XML changes", tableName);
    }
  }
}