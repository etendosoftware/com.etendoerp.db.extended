package com.etendoerp.db.extended.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Utility class for common logging operations in the partitioning process.
 * Centralizes logging patterns and reduces duplication across the codebase.
 */
public class LoggingUtils {
    
    private static final Logger log4j = LogManager.getLogger();

    /**
     * Logs a separator line to the application log using the info level.
     * <p>
     * This method is typically used to visually separate sections in the log output,
     * improving readability during debugging or tracing execution flow.
     */
    public static void logSeparator() {
        log4j.info(Constants.SEPARATOR);
    }

    /**
     * Logs a successful migration message with row count and source/target tables.
     */
    public static void logMigrationSuccessMessage(String tableName, long migratedRows, String dataSourceTable) {
        log4j.info(Constants.MIGRATION_SUCCESS_MSG, migratedRows, dataSourceTable, tableName);
    }

    /**
     * Logs successful constraint recreation.
     */
    public static void logSuccessRecreatedConstraints(String tableName) {
        log4j.info("Successfully recreated constraints for {}", tableName);
    }

    /**
     * Logs critical restoration errors.
     */
    public static void logCriticalRestorationError(String tableName, Exception restoreError) {
        log4j.error("CRITICAL: Failed to restore original table {}: {}", tableName, restoreError.getMessage());
    }

    /**
     * Logs successful table restoration.
     */
    public static void logSuccessRestoredTable(String tableName) {
        log4j.info("Successfully restored original table {}", tableName);
    }

    /**
     * Logs attempt to restore original table.
     */
    public static void logRestoreOriginalTable(String tableName, String tempTableName) {
        log4j.info("Attempting to restore original table {} from {}", tableName, tempTableName);
    }

    /**
     * Logs partitioned table creation.
     */
    public static void logCreatingPartitionedTable(String createTableSql) {
        log4j.info("Creating partitioned table with SQL: {}", createTableSql);
    }

    /**
     * Logs successful creation of new partitioned table.
     */
    public static void logNewPartitionedTableCreated(String newTableName) {
        log4j.info("Created new partitioned table: public.{}", newTableName);
    }

    /**
     * Logs the reason why a table is being skipped during processing.
     *
     * @param isIncomplete  true if the configuration is incomplete.
     * @param tableName     the name of the table being skipped.
     * @param pkCol         the primary key column name.
     * @param partitionCol  the partition column name.
     */
    public static void logSkipReason(boolean isIncomplete, String tableName, String pkCol, String partitionCol) {
        if (isIncomplete) {
            log4j.warn("Skipping incomplete configuration for table {} (pk = {}, partition = {})", tableName, pkCol, partitionCol);
        } else {
            log4j.info("Skipping {}: already processed and no XML changes", tableName);
        }
    }
}
