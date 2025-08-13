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

/**
 * Central repository for shared constants used across the partitioning module.
 * This class helps eliminate code duplication and provides a single point
 * for maintaining common values.
 */
public final class Constants {
    
    private Constants() {
        // Utility class - prevent instantiation
    }
    
    // Database schema constants
    public static final String BACKUP_SCHEMA = "etarc_backup";
    public static final String PUBLIC_SCHEMA_PREFIX = "public.";
    
    // SQL templates
    public static final String CREATE_SCHEMA_SQL = "CREATE SCHEMA IF NOT EXISTS %s";
    public static final String ALTER_TABLE = "ALTER TABLE IF EXISTS PUBLIC.%s\n";
    public static final String DROP_TABLE_CASCADE_SQL = "DROP TABLE IF EXISTS public.%s CASCADE";
    public static final String ALTER_TABLE_RENAME_SQL = "ALTER TABLE IF EXISTS public.%s RENAME TO %s";
    
    // File path constants
    public static final String SRC_DB_DATABASE_MODEL_TABLES = "src-db/database/model/tables";
    public static final String SRC_DB_DATABASE_MODEL_MODIFIED_TABLES = "src-db/database/model/modifiedTables";
    public static final String MODULES_JAR = "build/etendo/modules";
    public static final String MODULES_BASE = "modules";
    public static final String MODULES_CORE = "modules_core";
    public static final String[] MODULE_DIRS = new String[]{MODULES_BASE, MODULES_CORE, MODULES_JAR};
    
    // Performance constants
    public static final int LARGE_TABLE_THRESHOLD = 1000000;
    public static final int DEFAULT_BATCH_SIZE = 50000;
    
    // Common field names
    public static final String TABLE_NAME_KEY = "tableName";
    public static final String COLUMN_NAME_KEY = "columnName";
    public static final String PK_COLUMN_NAME_KEY = "pkColumnName";
    
    // Data types
    public static final String VARCHAR_255 = "VARCHAR(255)";
    
    // Logging messages
    public static final String MIGRATION_SUCCESS_MSG = "Successfully migrated {} rows from {} to {}";
    public static final String SEPARATOR = "=======================================================";
}
