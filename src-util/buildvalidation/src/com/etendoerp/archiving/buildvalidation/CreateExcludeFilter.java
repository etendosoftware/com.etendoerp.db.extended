package com.etendoerp.archiving.buildvalidation;

import com.etendoerp.archiving.utils.ArchivingPreBuildUtils;
import org.openbravo.base.session.OBPropertiesProvider;
import org.openbravo.buildvalidation.BuildValidation;
import org.openbravo.database.ConnectionProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.io.File;

/**
 * Generates the excludeFilter.xml by collecting constraint names
 * (PKs and FKs) found in the model XML files instead of using information_schema.
 */
public class CreateExcludeFilter extends BuildValidation {
  private static final Logger logger = LogManager.getLogger();

  @Override
  public List<String> execute() {
    Set<String> constraintsToExclude = new HashSet<>();

    try {
      ConnectionProvider connectionProvider = getConnectionProvider();

      // 1) Retrieve all base tables configured in ETARC_Table_Config
      String baseTablesQuery =
          "SELECT tablename " +
              "FROM ad_table " +
              "WHERE ad_table_id IN ( " +
              "  SELECT ad_table_id FROM ETARC_Table_Config" +
              ")";
      logger.info("Executing query to retrieve base tables");
      PreparedStatement baseTablesStmt = connectionProvider.getPreparedStatement(baseTablesQuery);
      ResultSet baseTablesResult = baseTablesStmt.executeQuery();

      while (baseTablesResult.next()) {
        String baseTableName = baseTablesResult.getString("tablename");
        if (baseTableName == null || baseTableName.trim().isEmpty()) {
          logger.warn("Received an empty or null base table name; skipping.");
          continue;
        }
        baseTableName = baseTableName.toLowerCase();
        logger.info("Processing base table: {}", baseTableName);

        // 2) Extract the PRIMARY KEY (PK) from the XML files of that base table
        List<File> baseTableXmlFiles = ArchivingPreBuildUtils.findTableXmlFiles(baseTableName);
        String primaryKeyName = ArchivingPreBuildUtils.findPrimaryKey(baseTableXmlFiles);
        if (primaryKeyName != null && !primaryKeyName.trim().isEmpty()) {
          String primaryKeyUpper = primaryKeyName.toUpperCase();
          constraintsToExclude.add(primaryKeyUpper);
          logger.info("Found PK for '{}': {}", baseTableName, primaryKeyUpper);
        } else {
          logger.warn("No PRIMARY KEY found in the XMLs of '{}'", baseTableName);
        }

        // 3) Extract all FOREIGN KEYS (FK) that reference this base table
        Set<String> referencingFks = ArchivingPreBuildUtils.findAllForeignKeysReferencing(baseTableName);
        if (!referencingFks.isEmpty()) {
          logger.info("Found {} FKs referencing '{}'", referencingFks.size(), baseTableName);
          constraintsToExclude.addAll(referencingFks);
        } else {
          logger.info("No FKs found referencing '{}'", baseTableName);
        }
      }

      baseTablesResult.close();
      baseTablesStmt.close();

      // 4) Write the excludeFilter.xml with all collected constraint names
      logger.info("Generating excludeFilter.xml for {} excluded constraints", constraintsToExclude.size());
      String sourcePath =
          OBPropertiesProvider
              .getInstance()
              .getOpenbravoProperties()
              .getProperty("source.path");
      Path outputFile = Paths.get(
          sourcePath,
          "modules",
          "com.etendoerp.archiving",
          "src-db",
          "database",
          "model",
          "excludeFilter.xml"
      );
      Files.createDirectories(outputFile.getParent());

      StringBuilder xmlBuilder = new StringBuilder();
      xmlBuilder.append("<vector>\n");
      for (String constraintName : constraintsToExclude) {
        xmlBuilder
            .append("  <excludedConstraint name=\"")
            .append(constraintName)
            .append("\"/>\n");
      }
      xmlBuilder.append("</vector>\n");

      Files.writeString(outputFile, xmlBuilder.toString(), StandardCharsets.UTF_8);
      logger.info("Generated excludeFilter.xml successfully.");

    } catch (Exception e) {
      logger.error("Error generating excludeFilter.xml: {}", e.getMessage(), e);
    }

    return List.of();
  }
}
