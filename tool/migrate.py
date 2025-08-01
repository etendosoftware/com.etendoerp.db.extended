import os
import yaml
from collections import defaultdict
from pathlib import Path
from configparser import ConfigParser
import psycopg2
from psycopg2 import sql
import re
import uuid
import xml.etree.ElementTree as ET
from xml.dom import minidom
import argparse
from datetime import datetime
import traceback
from db_utils import handle_db_errors, fetchall, fetchone

BBDD_SID = 'bbdd.sid'
BBDD_RDBMS = 'bbdd.rdbms'
BBDD_URL = 'bbdd.url'
BBDD_USER = 'bbdd.user'
BBDD_PASSWORD = 'bbdd.password'

COL_NAME = 0
DATA_TYPE = 1
IS_NULLABLE = 2
DEFAULT = 3
MAX_LENGTH = 4
NUM_PREC = 5
NUM_SCALE = 6
DATETIME_PREC = 7
UDT_NAME = 8

# Variable global para controlar el uso de salida ANSI (colores/emojis)
_use_ansi_output = True

class Style:
    """Clase para definir estilos de consola (colores ANSI y emojis)."""
    # Colores ANSI
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m' # Corrected Blue
    MAGENTA = '\033[95m' # Corrected Magenta
    CYAN = '\033[96m' # Corrected Cyan
    WHITE = '\033[97m' # Corrected White
    RESET = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m' # Corrected Underline

    # Emojis (asegúrate de que tu terminal los soporte y esté configurada para UTF-8)
    SUCCESS = '✅'
    FAILURE = '❌'
    WARNING = '⚠️'
    INFO = 'ℹ️' # Changed to a more common info icon
    STEP = '➡️' # Changed to a right arrow
    PROCESSING = '⏳' # Changed to an hourglass
    HEADER = '✨' # Changed to sparkles
    FINISH = '🏁'
    TABLE = '📄' # Changed to a document icon
    SCRIPT_VERSION = '🏷️' # Changed to a label
    DRY_RUN = '🧪'
    LIVE_RUN = '🚀' # Changed to a rocket
    DATABASE = '💾' # Changed to a floppy disk
    CONFIGURATION = '⚙️' # Changed to a gear
    SUMMARY = '📊' # Changed to a bar chart
    ERROR_ICON = '❗' # Changed to an exclamation mark

MESSAGE_STYLES = {
    "HEADER":      (Style.MAGENTA + Style.BOLD, Style.HEADER + " "),
    "SUBHEADER":   (Style.CYAN + Style.BOLD, Style.PROCESSING + " "),
    "STEP_INFO":   (Style.BLUE, "  "),
    "STEP":        (Style.BLUE + Style.BOLD, Style.STEP + " "),
    "SUCCESS":     (Style.GREEN + Style.BOLD, Style.SUCCESS + " "),
    "FAILURE":     (Style.RED + Style.BOLD, Style.FAILURE + " "),
    "ERROR":       (Style.RED + Style.BOLD, Style.ERROR_ICON + " "),
    "WARNING":     (Style.YELLOW, Style.WARNING + " "),
    "INFO":        (Style.WHITE, Style.INFO + " "),
    "DEBUG":       (Style.CYAN, "  "),
    "IMPORTANT":   (Style.YELLOW + Style.BOLD, Style.WARNING + " "),
    "SCRIPT_INFO": (Style.MAGENTA, Style.SCRIPT_VERSION + " "),
    "RUN_MODE":    (Style.YELLOW + Style.BOLD, ""),
    "DB_CONNECT":  (Style.GREEN, Style.DATABASE + " "),
    "CONFIG_LOAD": (Style.CYAN, Style.CONFIGURATION + " "),
    "TABLE_PROCESS": (Style.MAGENTA + Style.BOLD, Style.TABLE + " "),
}

def print_message(message, level="INFO", end="\n"):
    global _use_ansi_output
    if not _use_ansi_output:
        print(message, end=end)
        return

    color, emoji = MESSAGE_STYLES.get(level, (Style.RESET, ""))
    print(f"{color}{emoji}{message}{Style.RESET}", end=end)

# --- Funciones auxiliares ---
def column_exists(conn, schema, table, column):
    return object_exists(conn, schema, table, 'columns', column)

def read_properties_file(file_path):
    try:
        config = ConfigParser()
        # Ensure the directory exists before trying to open the file
        p = Path(file_path)
        if not p.parent.exists():
            print_message(f"Configuration directory {p.parent} does not exist. Please create it.", "ERROR")
            return {}
        if not p.exists():
            print_message(f"Properties file {file_path} not found.", "ERROR")
            # Create a dummy file for robust parsing if it doesn't exist, or handle more gracefully
            # For now, returning empty to avoid crash, but this should be handled by user.
            return {}

        with open(file_path, 'r', encoding='utf-8') as f:
            content = f"[default]\n{f.read()}"
        config.read_string(content)
        properties = {
            'source.path': config.get('default', 'source.path', fallback=None),
            BBDD_RDBMS: config.get('default', BBDD_RDBMS, fallback=None),
            BBDD_URL: config.get('default', BBDD_URL, fallback=None).replace('\\', ''),
            BBDD_SID: config.get('default', BBDD_SID, fallback=None),
            BBDD_USER: config.get('default', BBDD_USER, fallback=None),
            BBDD_PASSWORD: config.get('default', BBDD_PASSWORD, fallback=None),
            'bbdd.sessionConfig': config.get('default', 'bbdd.sessionConfig', fallback=None)
        }
        return properties
    except Exception as e:
        print_message(f"Error reading {file_path}: {e}", "ERROR")
        return {}

def parse_db_params(properties):
    if not all([properties.get(BBDD_URL), properties.get(BBDD_SID), properties.get(BBDD_USER), properties.get(BBDD_PASSWORD)]):
        raise ValueError("Missing required database connection properties (bbdd.url, bbdd.sid, bbdd.user, bbdd.password)")
    url_match = re.match(r'jdbc:postgresql://([^:]+):(\d+)', properties[BBDD_URL])
    if not url_match:
        raise ValueError(f"Invalid bbdd.url format: {properties[BBDD_URL]}. Expected format: jdbc:postgresql://host:port")
    host, port = url_match.groups()
    return {'host': host.replace('\\', ''), 'port': port, 'database': properties[BBDD_SID],
            'user': properties.get(BBDD_USER), 'password': properties.get(BBDD_PASSWORD)}

def get_tables_and_fields_from_database(conn):
    """
    Obtiene las tablas y campos de partición desde la base de datos usando la query SQL proporcionada.
    Retorna un diccionario con formato: {tabla: {campo}}
    """
    table_fields_config = defaultdict(set)

    try:
        print_message("Executing query to get table and partition field configuration from database...", "CONFIG_LOAD")

        query = """
        SELECT
            LOWER(TBL.TABLENAME) TABLENAME,
            LOWER(COL.COLUMNNAME) COLUMNNAME
        FROM
            ETARC_TABLE_CONFIG CFG
        JOIN AD_TABLE TBL ON TBL.AD_TABLE_ID = CFG.AD_TABLE_ID
        JOIN AD_COLUMN COL ON COL.AD_COLUMN_ID = CFG.AD_COLUMN_ID
        """
        # Ensure connection is not in a failed transaction state before executing
        if conn.status == psycopg2.extensions.STATUS_IN_TRANSACTION and conn.get_transaction_status() == psycopg2.extensions.TRANSACTION_STATUS_INERROR:
            conn.rollback()

        with conn.cursor() as cur:
            cur.execute(query)
            results = cur.fetchall()

            for table_name, column_name in results:
                table_fields_config[table_name].add(column_name)
                print_message(f"Found configuration: Table={table_name}, Partition Field={column_name}", "DEBUG") # Changed to DEBUG for less verbosity

        if not table_fields_config:
            print_message("No table configuration found in database tables ETARC_TABLE_CONFIG, AD_TABLE, AD_COLUMN.", "WARNING")
        else:
            print_message(f"Successfully loaded configuration for {len(table_fields_config)} table(s) from database.", "SUCCESS")

        return dict(table_fields_config)

    except psycopg2.Error as db_err:
        print_message(f"Database error while fetching table configuration: {db_err}", "ERROR")
        print_message("Please ensure tables ETARC_TABLE_CONFIG, AD_TABLE, and AD_COLUMN exist and are accessible.", "ERROR")
        return {}
    except Exception as e:
        print_message(f"Error fetching table configuration from database: {e}", "ERROR")
        return {}

def get_table_schema(conn, table_name, schema='public'):
    try:
        column_data = get_column_metadata(conn, schema, table_name)
        if not column_data:
            raise ValueError(f"Couldn't retrieve metadata in {schema}.{table_name}")

        print_message(f"Schema for {schema}.{table_name}:", "DEBUG")
        for col in column_data:
            print_message(f"  - Column: {col[COL_NAME]}, Type: {col[DATA_TYPE]}, Nullable: {col[IS_NULLABLE]}, Default: {col[DEFAULT]}", "DEBUG")

        return column_data
    except Exception as e:
        print_message(f"Error retrieving schema for {schema}.{table_name}: {e}", "ERROR")
        return []

def table_exists(conn, table_name, schema=None):
    if schema:
        exists = object_exists(conn, schema, table_name, 'tables', table_name)
        return schema if exists else None

    row = fetchone(
        conn,
        """
        SELECT table_schema
        FROM information_schema.tables
        WHERE table_name = %s
          AND table_schema NOT IN ('pg_catalog','information_schema')
        LIMIT 1
        """,
        (table_name,),
        log_msg=f"Searching schema from table {table_name}", level="DEBUG"
    )
    return row[0] if row else None

def is_table_partitioned(conn, table_name, schema):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = %s AND n.nspname = %s", (table_name, schema))
            result = cur.fetchone()
            return result[0] == 'p' if result else False # 'p' for partitioned table
    except Exception as e:
        print_message(f"Error checking if table {schema}.{table_name} is partitioned: {e}", "ERROR")
        return False

def object_exists(conn, schema, table, object_type, object_name):
    """
    object_type: 'columns' | 'tables' | 'views' ...
    """
    query = f"""
      SELECT 1
      FROM information_schema.{object_type}
      WHERE table_schema = %s
        AND table_name = %s
        AND {"column_name" if object_type=='columns' else "table_name"} = %s
      LIMIT 1
    """
    return fetchone(conn, query, (schema, table, object_name)) is not None

@handle_db_errors(default=[])
def get_dependent_views(conn, table_name, schema):
    sql= """
        SELECT DISTINCT dependent_ns.nspname AS view_schema, dependent_view.relname AS view_name, pg_get_viewdef(dependent_view.oid) AS view_definition
        FROM pg_depend AS d
        JOIN pg_rewrite AS r ON r.oid = d.objid
        JOIN pg_class AS dependent_view ON dependent_view.oid = r.ev_class
        JOIN pg_namespace AS dependent_ns ON dependent_ns.oid = dependent_view.relnamespace
        JOIN pg_class AS source_table ON source_table.oid = d.refobjid
        JOIN pg_namespace AS source_ns ON source_ns.oid = source_table.relnamespace
        WHERE d.refclassid = 'pg_class'::REGCLASS
          AND d.classid = 'pg_rewrite'::REGCLASS
          AND dependent_view.relkind = 'v'
          AND source_table.relname = %s
          AND source_ns.nspname = %s;
    """
    return fetchall(conn, sql, (table_name, schema),
                        log_msg=f"Searching related view with {schema}.{table_name}", level="DEBUG")

@handle_db_errors(default=[])
def get_triggers(conn, table_name, schema):
    sql ="""
        SELECT n.nspname AS schema_name, tg.tgname AS trigger_name, pg_get_triggerdef(tg.oid) as trigger_definition
        FROM pg_trigger tg
        JOIN pg_class t ON tg.tgrelid = t.oid
        JOIN pg_namespace n ON t.relnamespace = n.oid
        WHERE t.relname = %s AND n.nspname = %s AND tg.tgisinternal = FALSE
    """ # Added tgisinternal = FALSE to exclude system triggers
    return fetchall(conn, sql, (table_name, schema),
                            log_msg=f"Searching triggers in {schema}.{table_name}", level="DEBUG")

@handle_db_errors(default=[])
def get_foreign_keys(conn, table, schema, direction="on"):
    """
    direction='on' → FKs definidas EN esta tabla (tabla → otras tablas)
    direction='to' → FKs que apuntan A esta tabla (otras tablas → tabla)
    """
    if direction == "on":
        sql = """
          SELECT
            pc.conname AS constraint_name,
            pg_get_constraintdef(pc.oid) AS constraint_definition,
            ARRAY(
              SELECT att.attname
              FROM unnest(pc.conkey) WITH ORDINALITY k(attnum, ord)
              JOIN pg_attribute att
                ON att.attrelid = pc.conrelid
               AND att.attnum = k.attnum
              ORDER BY k.ord
            ) AS fk_columns
          FROM pg_constraint pc
          JOIN pg_namespace n ON n.oid = pc.connamespace
          JOIN pg_class cl      ON cl.oid = pc.conrelid
          WHERE pc.contype = 'f'
            AND cl.relname = %s
            AND n.nspname = %s;
        """
        rows = fetchall(
            conn, sql,
            params=(table, schema),
            log_msg=f"Buscando FKs definidas ON {schema}.{table}",
            level="DEBUG"
        )
        return [
            {'name': name, 'definition': definition, 'columns': list(columns)}
            for name, definition, columns in rows
        ]

    else:  # direction == "to"
        sql = """
          SELECT
            con.conname AS constraint_name,
            cl_ref.relname AS referencing_table,
            ns_ref.nspname AS referencing_schema,
            pg_get_constraintdef(con.oid) AS constraint_definition,
            ARRAY(
              SELECT att.attname
              FROM unnest(con.conkey) WITH ORDINALITY k(attnum, ord)
              JOIN pg_attribute att ON att.attrelid = con.conrelid AND att.attnum = k.attnum
              ORDER BY k.ord
            ) AS fk_columns
          FROM pg_constraint con
          JOIN pg_class cl_ref      ON cl_ref.oid = con.conrelid
          JOIN pg_namespace ns_ref  ON ns_ref.oid = cl_ref.relnamespace
          JOIN pg_class cl_referenced ON cl_referenced.oid = con.confrelid
          JOIN pg_namespace ns_referenced ON ns_referenced.oid = cl_referenced.relnamespace
          WHERE cl_referenced.relname = %s
            AND ns_referenced.nspname = %s
            AND con.contype = 'f';
        """
        rows = fetchall(
            conn, sql,
            params=(table, schema),
            log_msg=f"Buscando FKs que apuntan A {schema}.{table}",
            level="DEBUG"
        )
        return [
            {
              'name': name,
              'referencing_table': ref_table,
              'referencing_schema': ref_schema,
              'definition': definition,
              'columns': list(columns)
            }
            for name, ref_table, ref_schema, definition, columns in rows
        ]

def process_related_tables_for_partitioning(conn, source_table_name, source_schema, partition_field, dry_run=False):
    """
    Main orchestration function to process all related tables for partitioning.
    """
    result = {
        'success': True,
        'total_related_tables': 0,
        'processed_tables': 0,
        'failed_tables': 0,
        'total_records_updated': 0,
        'details': []
    }

    try:
        print_message(f"\n--- Processing Related Tables for {source_schema}.{source_table_name} (Partition Field: {partition_field}) ---", "SUBHEADER")

        # Step 1: Get partition field type information from the source table
        try:
            field_type_info = get_partition_field_type(conn, source_table_name, source_schema, partition_field)
        except Exception as e:
            print_message(f"Failed to get partition field type for {source_schema}.{source_table_name}.{partition_field}: {e}", "ERROR")
            result['success'] = False
            result['details'].append({'error_message': f"Critical error: Could not get source partition field type: {e}"})
            return result # Cannot proceed without field type

        # Step 2: Discover related tables (tables that have FKs referencing the source table)
        related_tables = get_related_tables(conn, source_table_name, source_schema)
        result['total_related_tables'] = len(related_tables)

        if not related_tables:
            print_message("No related tables found. Skipping related table processing.", "INFO")
            return result # No related tables to process

        print_message(f"Found {len(related_tables)} related table(s) to process.", "INFO")

        # Step 3: Process each related table
        for i, related_table_info in enumerate(related_tables, 1):
            table_schema = related_table_info['table_schema']
            table_name = related_table_info['table_name']
            table_detail = {
                'table_schema': table_schema,
                'table_name': table_name,
                'constraint_name': related_table_info['constraint_name'],
                'success': False,
                'records_updated': 0,
                'validation_result': None,
                'error_message': None
            }

            try:
                print_message(f"\n[{i}/{len(related_tables)}] Processing related table: {Style.BOLD}{table_schema}.{table_name}{Style.RESET}...", "STEP")

                # Step 3a: Add partition field to related table
                if add_partition_field_to_related_table(conn, related_table_info, partition_field, field_type_info, dry_run):
                    print_message(f"  ✓ Field 'etarc_{partition_field}' addition/check successful for {table_schema}.{table_name}", "STEP_INFO")

                    # Step 3b: Populate the field with data from the source table
                    records_updated = populate_partition_field_in_related_table(
                        conn, source_table_name, source_schema, related_table_info, partition_field, dry_run
                    )

                    if records_updated >= 0: # 0 is a valid number of updated records
                        table_detail['records_updated'] = records_updated
                        result['total_records_updated'] += records_updated
                        print_message(f"  ✓ Data population for 'etarc_{partition_field}' successful for {table_schema}.{table_name} (Records: {records_updated})", "STEP_INFO")

                        # Step 3c: Validate population (only in live mode)
                        if not dry_run:
                            validation_result = validate_partition_field_population(conn, related_table_info, partition_field)
                            table_detail['validation_result'] = validation_result

                            if validation_result['success']:
                                print_message(f"  ✓ Validation successful for {table_schema}.{table_name}", "STEP_INFO")
                                table_detail['success'] = True
                                result['processed_tables'] += 1
                                conn.commit() # Commit changes for this specific related table
                            else:
                                print_message(f"  {Style.WARNING} Validation failed for {table_schema}.{table_name}: {validation_result['message']}", "WARNING")
                                table_detail['error_message'] = f"Validation failed: {validation_result['message']}"
                                result['failed_tables'] += 1
                                conn.rollback() # Rollback changes for this table due to validation failure
                        else:
                            # In dry run mode, consider it successful if we got this far
                            table_detail['success'] = True
                            result['processed_tables'] += 1
                            print_message(f"  ✓ [Dry Run] Processing simulation successful for {table_schema}.{table_name}", "STEP_INFO")
                            # No commit/rollback in dry_run for the main conn
                    else: # records_updated is -1, indicating an error during population
                        table_detail['error_message'] = "Data population failed (see logs)."
                        result['failed_tables'] += 1
                        print_message(f"  {Style.FAILURE} Data population failed for {table_schema}.{table_name}", "FAILURE")
                        if not dry_run: conn.rollback()
                else: # Field addition failed
                    table_detail['error_message'] = "Field addition failed (see logs)."
                    result['failed_tables'] += 1
                    print_message(f"  {Style.FAILURE} Field addition failed for {table_schema}.{table_name}", "FAILURE")
                    if not dry_run: conn.rollback() # Rollback if alter table failed

            except Exception as e_inner:
                table_detail['error_message'] = str(e_inner)
                result['failed_tables'] += 1
                print_message(f"  {Style.ERROR_ICON} Error processing {table_schema}.{table_name}: {e_inner}", "ERROR")
                traceback.print_exc()
                if not dry_run:
                    try:
                        conn.rollback()
                    except psycopg2.Error as rb_err:
                        print_message(f"  {Style.ERROR_ICON} Rollback failed for {table_schema}.{table_name}: {rb_err}", "ERROR")


            result['details'].append(table_detail)

        # Final status update for the overall related table processing
        if result['failed_tables'] > 0:
            result['success'] = False # Overall success is false if any related table failed
            print_message(f"\n{Style.WARNING} Related table processing completed with {result['failed_tables']} failure(s) out of {result['total_related_tables']}.", "WARNING")
        else:
            if result['total_related_tables'] > 0:
                print_message(f"\n{Style.SUCCESS} All {result['total_related_tables']} related table(s) processed successfully.", "SUCCESS")
            # If no related tables, success remains true by default.

        print_message(f"Summary for related tables: {result['processed_tables']} successful, {result['failed_tables']} failed. Total records updated in related tables: {result['total_records_updated']}.", "INFO")
        return result

    except Exception as e_outer:
        print_message(f"{Style.ERROR_ICON} Fatal error in related table processing orchestration: {e_outer}", "ERROR")
        traceback.print_exc()
        result['success'] = False
        result['details'].append({
            'error_message': f"Fatal orchestration error: {e_outer}",
            'success': False
        })
        if not dry_run:
            try:
                conn.rollback() # Rollback any overarching transaction if something went very wrong
            except psycopg2.Error as rb_err:
                print_message(f"  {Style.ERROR_ICON} Outer rollback failed: {rb_err}", "ERROR")
        return result

def get_partition_field_name(partition_field, constraint_name):
    return f"etarc_{partition_field}__{constraint_name}"

def validate_partition_field_population(conn, related_table_info, partition_field):
    """
    Validate that partition field population was successful.
    """
    try:
        table_schema = related_table_info['table_schema']
        table_name = related_table_info['table_name']
        new_field_name = get_partition_field_name(partition_field, related_table_info['constraint_name'])
        print_message(f"Validating '{new_field_name}' population in {table_schema}.{table_name}...", "DEBUG")

        with conn.cursor() as cur:
            # Count total records
            cur.execute(
                sql.SQL("SELECT COUNT(*) FROM {}.{} WHERE NOT {} IS NULL").format(
                    sql.Identifier(table_schema),
                    sql.Identifier(table_name),
                    sql.Identifier(related_table_info['fk_columns'][0])  # Assuming the first FK column is representative
                )
            )
            total_records = cur.fetchone()[0]

            # Count NULL records in the new partition field
            cur.execute(
                sql.SQL("SELECT COUNT(*) FROM {}.{} WHERE {} IS NULL AND NOT {} IS NULL").format(
                    sql.Identifier(table_schema),
                    sql.Identifier(table_name),
                    sql.Identifier(new_field_name),
                    sql.Identifier(related_table_info['fk_columns'][0])  # Assuming the first FK column is representative
                )
            )
            null_records = cur.fetchone()[0]

            populated_records = total_records - null_records

            if null_records == 0:
                success = True
                message = f"All {total_records} record(s) have '{new_field_name}' populated."
                print_message(f"  {Style.SUCCESS} {message}", "STEP_INFO")
            else:
                # This is a critical issue if FKs are involved and data integrity is expected.
                success = False
                message = f"{null_records} record(s) have NULL '{new_field_name}' out of {total_records} total. This may indicate issues with FK relationships or data."
                print_message(f"  {Style.WARNING} {message}", "WARNING")


            return {
                'success': success,
                'total_records': total_records,
                'null_records': null_records,
                'populated_records': populated_records,
                'message': message
            }

    except psycopg2.Error as db_err:
        error_msg = f"Database error validating {table_schema}.{table_name}: {db_err}"
        print_message(error_msg, "ERROR")
        return { 'success': False, 'total_records': 0, 'null_records': 0, 'populated_records': 0, 'message': error_msg }
    except Exception as e:
        error_msg = f"Error validating {table_schema}.{table_name}: {e}"
        print_message(error_msg, "ERROR")
        return { 'success': False, 'total_records': 0, 'null_records': 0, 'populated_records': 0, 'message': error_msg }

def populate_partition_field_in_related_table(conn, source_table, source_schema, related_table_info, partition_field, dry_run=False):
    """
    Populate the new 'etarc_PARTITIONFIELD' in related tables with data from source table's PARTITIONFIELD.
    """
    try:
        table_schema = related_table_info['table_schema']
        table_name = related_table_info['table_name']
        new_field_name = get_partition_field_name(partition_field, related_table_info['constraint_name'])
        fk_columns = related_table_info['fk_columns'] # Columns in related_table forming the FK
        referenced_columns = related_table_info['referenced_columns'] # Columns in source_table referenced by FK

        print_message(f"Populating '{new_field_name}' in {table_schema}.{table_name} from {source_schema}.{source_table}.{partition_field}...", "DEBUG")

        # Disable trigger temporarily calling database funciont ad_disable_trigger
        if not dry_run:
            disable_trigger_stmt = sql.SQL("SELECT ad_disable_triggers()").format(
                sql.Identifier(table_schema),
                sql.Identifier(table_name)
            )
            with conn.cursor() as cur:
                cur.execute(disable_trigger_stmt)
                print_message(f"  {Style.INFO} Temporarily disabled triggers for {table_schema}.{table_name}.", "STEP_INFO")
        # Build the JOIN conditions for the UPDATE statement
        # r (related_table) JOIN s (source_table)
        join_conditions = []
        for fk_col, ref_col in zip(fk_columns, referenced_columns):
            join_conditions.append(
                sql.SQL("r.{} = s.{}").format(
                    sql.Identifier(fk_col), # from related_table
                    sql.Identifier(ref_col)  # from source_table
                )
            )
        join_condition_sql = sql.SQL(" AND ").join(join_conditions)

        # Build UPDATE statement
        # UPDATE related_table r SET etarc_partition_field = s.partition_field FROM source_table s WHERE r.fk_col = s.pk_col;
        update_stmt = sql.SQL("""
            UPDATE {related_schema}.{related_table} AS r
            SET {new_partition_field_ident} = s.{source_partition_field_ident}
            FROM {source_schema_ident}.{source_table_ident} AS s
            WHERE {join_conditions} 
            AND r.{new_partition_field_ident} IS DISTINCT FROM s.{source_partition_field_ident}
        """).format(
            related_schema=sql.Identifier(table_schema),
            related_table=sql.Identifier(table_name),
            new_partition_field_ident=sql.Identifier(new_field_name),
            source_partition_field_ident=sql.Identifier(partition_field),
            source_schema_ident=sql.Identifier(source_schema),
            source_table_ident=sql.Identifier(source_table),
            join_conditions=join_condition_sql
        )
        # Added "IS DISTINCT FROM" to avoid updating rows that already have the correct value, reducing WAL.

        if dry_run:
            print_message(f"[Dry Run] Would execute population: {update_stmt.as_string(conn)}", "STEP_INFO")
            # Simulate count for dry run (approximate, as it doesn't consider IS DISTINCT FROM easily here)
            count_stmt = sql.SQL("""
                SELECT COUNT(1)
                FROM {related_schema}.{related_table} AS r
                JOIN {source_schema_ident}.{source_table_ident} AS s ON {join_conditions}
                WHERE r.{new_partition_field_ident} IS NULL OR r.{new_partition_field_ident} != s.{source_partition_field_ident} 
            """).format(
                related_schema=sql.Identifier(table_schema),
                related_table=sql.Identifier(table_name),
                source_schema_ident=sql.Identifier(source_schema),
                source_table_ident=sql.Identifier(source_table),
                join_conditions=join_condition_sql,
                new_partition_field_ident=sql.Identifier(new_field_name),
                source_partition_field_ident=sql.Identifier(partition_field)
            )
            with conn.cursor() as cur:
                cur.execute(count_stmt)
                count = cur.fetchone()[0]
            print_message(f"[Dry Run] Estimated {count} record(s) would be populated/updated in {table_schema}.{table_name}.", "STEP_INFO")
            return count
        else:
            with conn.cursor() as cur:
                cur.execute(update_stmt)
                updated_count = cur.rowcount
            print_message(f"  {Style.SUCCESS} Populated/Updated {updated_count} record(s) in {table_schema}.{table_name}.{new_field_name}.", "STEP_INFO")
            enable_trigger_stmt = sql.SQL("SELECT ad_enable_triggers()")
            with conn.cursor() as cur:
                cur.execute(enable_trigger_stmt)
                print_message(f"  {Style.INFO} Re-enabled triggers for {table_schema}.{table_name}.", "STEP_INFO")
            return updated_count
        

    except psycopg2.Error as db_err:
        print_message(f"Database error populating field in {table_schema}.{table_name}: {db_err}", "ERROR")
        return -1 # Indicate error
    except Exception as e:
        print_message(f"Error populating field in {table_schema}.{table_name}: {e}", "ERROR")
        return -1 # Indicate error

def add_partition_field_to_related_table(conn, related_table_info, partition_field, field_type_info, dry_run=False):
    """
    Add the 'etarc_PARTITIONFIELD' to a related table, matching the type of PARTITIONFIELD in the source table.
    """
    try:
        table_schema = related_table_info['table_schema']
        table_name = related_table_info['table_name']
        new_field_name = get_partition_field_name(partition_field, related_table_info['constraint_name'])
        print_message(f"Ensuring field '{new_field_name}' in {table_schema}.{table_name}...", "DEBUG")

        # Check if field already exists
        with conn.cursor() as cur:
            if column_exists(conn, table_schema, table_name, new_field_name):
                print_message(f"Field '{new_field_name}' already exists in {table_schema}.{table_name}. Verifying type...", "INFO")
                new_field_name += "_1"
                # Optional: Add type verification here if needed, though get_partition_field_type should be robust.
                # For now, assume if it exists, it's likely from a previous run and hopefully correct.
                return True # Field exists, proceed

        # Build the data type string using info from source table's partition field
        data_type_str = field_type_info['data_type']
        if field_type_info.get('character_maximum_length'):
            data_type_str += f"({field_type_info['character_maximum_length']})"
        elif field_type_info.get('numeric_precision') and field_type_info.get('numeric_scale') is not None:
            data_type_str += f"({field_type_info['numeric_precision']},{field_type_info['numeric_scale']})"
        elif field_type_info.get('datetime_precision') is not None: # Check for None explicitly
            # For types like 'timestamp without time zone', precision might be part of the base type string
            # or specified separately. information_schema.columns.datetime_precision gives it.
            # Example: TIMESTAMP(P)
            if 'timestamp' in data_type_str.lower() and '(' not in data_type_str: # Avoid double parenthesis
                precision = field_type_info['datetime_precision']
                # data_type_str += f"({precision})" # This might be redundant if type is 'timestamp(p) without time zone'
                # Let's rely on the base data_type string from source, it's usually complete.
                pass


        # Create ALTER TABLE statement
        # The new column should be NULLABLE.
        alter_stmt = sql.SQL("ALTER TABLE {}.{} ADD COLUMN {} {} NULL").format(
            sql.Identifier(table_schema),
            sql.Identifier(table_name),
            sql.Identifier(new_field_name),
            sql.SQL(data_type_str) # Use the precise data type string
        )

        if dry_run:
            print_message(f"[Dry Run] Would execute field addition: {alter_stmt.as_string(conn)}", "STEP_INFO")
        else:
            with conn.cursor() as cur:
                cur.execute(alter_stmt)
            print_message(f"  {Style.SUCCESS} Successfully added field '{new_field_name}' ({data_type_str}) to {table_schema}.{table_name}.", "STEP_INFO")
        return True

    except psycopg2.Error as db_err:
        print_message(f"Database error adding field '{new_field_name}' to {table_schema}.{table_name}: {db_err}", "ERROR")
        return False
    except Exception as e:
        print_message(f"Error adding field '{new_field_name}' to {table_schema}.{table_name}: {e}", "ERROR")
        return False

def get_column_metadata(conn, schema, table, column=None):
    base_query = """
        SELECT column_name, data_type, is_nullable, column_default,
               character_maximum_length, numeric_precision, numeric_scale, datetime_precision, udt_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
    """
    params = [schema, table]
    if column:
        base_query += " AND column_name = %s"
        params.append(column)

    with conn.cursor() as cur:
        cur.execute(base_query, params)
        return cur.fetchall()


def get_partition_field_type(conn, table_name, schema, field_name):
    """
    Get the exact data type and relevant attributes of the partition field from the source table.
    """
    try:
        print_message(f"Getting data type for field '{field_name}' in {schema}.{table_name}...", "DEBUG")

        column_data = get_column_metadata(conn, schema, table_name, field_name)
        if not column_data:
            raise ValueError(f"Partition field '{field_name}' not found in {schema}.{table_name}")
        col = column_data[0]

        field_type_info = {
            'data_type': col[DATA_TYPE],
            'is_nullable': col[IS_NULLABLE],
            'column_default': col[DEFAULT],
            'character_maximum_length': col[MAX_LENGTH],
            'numeric_precision': col[NUM_PREC],
            'numeric_scale': col[NUM_SCALE],
            'datetime_precision': col[DATETIME_PREC],
            'udt_name': col[UDT_NAME]  # puedes mantenerlo como data_type si udt_name no se incluye
        }
        # Prefer udt_name if it's more specific, but data_type is generally good for CREATE TABLE LIKE
        # For ALTER TABLE ADD COLUMN, the full data_type string is usually needed.

        print_message(
            f"Source partition field '{schema}.{table_name}.{field_name}' type: {field_type_info['data_type']} "
            f"(UDT: {field_type_info['udt_name']}, MaxLen: {field_type_info['character_maximum_length']}, "
            f"NumPrec: {field_type_info['numeric_precision']}, NumScale: {field_type_info['numeric_scale']}, "
            f"DatePrec: {field_type_info['datetime_precision']})",
            "DEBUG"
        )
        return field_type_info

    except psycopg2.Error as db_err:
        print_message(f"Database error getting field type for {schema}.{table_name}.{field_name}: {db_err}", "ERROR")
        raise
    except Exception as e:
        print_message(f"Error getting field type for {schema}.{table_name}.{field_name}: {e}", "ERROR")
        raise

def get_related_tables(conn, table_name, schema):
    """
    Discover all tables that have foreign key relationships pointing TO the source table.
    (i.e., source_table is the REFERENCED table).
    """
    related_tables = []
    try:
        print_message(f"Discovering tables with FKs referencing {schema}.{table_name}...", "DEBUG")
        with conn.cursor() as cur:
            # Query to find tables (referencing_table) that have FKs pointing to the given table_name (referenced_table)
            query = """
                SELECT 
                    tc.table_schema AS referencing_table_schema,
                    tc.table_name AS referencing_table_name,
                    tc.constraint_name,
                    ARRAY_AGG(kcu.column_name::TEXT ORDER BY kcu.ordinal_position) AS fk_columns_in_referencing_table,
                    ARRAY_AGG(ccu.column_name::TEXT) AS referenced_columns_in_source_table 
                        -- ccu.column_name should be from the primary/unique key of the source_table
                FROM information_schema.table_constraints AS tc 
                JOIN information_schema.key_column_usage AS kcu 
                    ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage AS ccu 
                    ON ccu.constraint_name = tc.constraint_name AND ccu.constraint_schema = tc.constraint_schema
                WHERE tc.constraint_type = 'FOREIGN KEY'
                  AND ccu.table_schema = %s  -- Schema of the referenced (source) table
                  AND ccu.table_name = %s    -- Name of the referenced (source) table
                GROUP BY tc.table_schema, tc.table_name, tc.constraint_name
                ORDER BY tc.table_schema, tc.table_name;
            """
            cur.execute(query, (schema, table_name))
            results = cur.fetchall()

            for row in results:
                related_table_info = {
                    'table_schema': row[0],       # Schema of the table that has the FK
                    'table_name': row[1],         # Name of the table that has the FK
                    'constraint_name': row[2],    # Name of the FK constraint
                    'fk_columns': list(row[3]),   # Columns in the related_table that form the FK
                    'referenced_columns': list(row[4]) # Columns in the source_table that are referenced
                }
                related_tables.append(related_table_info)
                print_message(
                    f"  Found related: {row[0]}.{row[1]} via FK '{row[2]}' "
                    f"({list(row[3])} -> {schema}.{table_name}{list(row[4])})",
                    "DEBUG"
                )
        if not related_tables:
            print_message(f"No tables found with FKs referencing {schema}.{table_name}.", "INFO")
        else:
            print_message(f"Found {len(related_tables)} table(s) with FKs referencing {schema}.{table_name}.", "INFO")
        return related_tables

    except psycopg2.Error as db_err:
        print_message(f"Database error finding related tables for {schema}.{table_name}: {db_err}", "ERROR")
        return []
    except Exception as e:
        print_message(f"Error finding related tables for {schema}.{table_name}: {e}", "ERROR")
        return []

def get_indexes_for_table(conn, schema, table_name_idx):
    indexes = {}
    try:
        with conn.cursor() as cur:
            # Excludes primary key index (indisprimary = FALSE)
            # Also excludes unique constraints that are not primary keys but have indexes
            cur.execute("""
                SELECT i.relname AS indexname,
                       ARRAY_AGG(a.attname ORDER BY array_position(ix.indkey::int[], a.attnum::int)) AS columnnames,
                       ix.indisunique AS is_unique_constraint_index
                FROM pg_class t
                JOIN pg_index ix ON t.oid = ix.indrelid
                JOIN pg_class i ON i.oid = ix.indexrelid
                JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE n.nspname = %s AND t.relname = %s AND ix.indisprimary = FALSE
                GROUP BY i.relname, ix.indisunique; 
            """, (schema, table_name_idx))
            for row in cur.fetchall():
                # Only consider non-unique indexes or if you want to handle unique constraint indexes separately
                # For now, taking all non-primary indexes
                indexes[row[0]] = {'columns': list(row[1]), 'is_unique_constraint_index': row[2]}
    except Exception as e: print_message(f"Error retrieving indexes for {schema}.{table_name_idx}: {e}", "ERROR")
    return indexes

def drop_triggers(conn, table_name, schema, dry_run=False):
    dropped_triggers_info = []
    try:
        triggers = get_triggers(conn, table_name, schema) # get_triggers already filters internal ones
        for trigger_schema, trigger_name, trigger_def in triggers:
            # Additional safeguard for well-known system/extension triggers if any slip through
            if trigger_name.startswith('RI_ConstraintTrigger') or trigger_name.startswith('zombodb_'): # Example
                print_message(f"Skipping system/special trigger {trigger_schema}.{trigger_name}", "INFO")
                continue

            sql_stmt_str = sql.SQL("DROP TRIGGER {} ON {}.{}").format(
                sql.Identifier(trigger_name), sql.Identifier(schema), sql.Identifier(table_name)
            ).as_string(conn)

            if dry_run: print_message(f"[Dry Run] Would execute trigger drop: {sql_stmt_str}", "STEP_INFO")
            else:
                with conn.cursor() as cur: cur.execute(sql_stmt_str)
                print_message(f"Dropped trigger {trigger_schema}.{trigger_name} on {schema}.{table_name}", "SUCCESS")
            dropped_triggers_info.append({'name': trigger_name, 'schema': trigger_schema, 'definition': trigger_def})
        return dropped_triggers_info
    except Exception as e:
        print_message(f"Error dropping triggers for {schema}.{table_name}: {e}", "ERROR")
        raise # Re-raise to be caught by the main step executor

def validate_partition_field(conn, table_to_validate, schema_of_table, field_to_validate):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT data_type FROM information_schema.columns WHERE table_schema = %s AND table_name = %s AND column_name = %s", (schema_of_table, table_to_validate, field_to_validate))
            result = cur.fetchone()
            if not result: raise ValueError(f"Partition field '{field_to_validate}' not found in {schema_of_table}.{table_to_validate}")

            data_type = result[0].lower()
            # Common date/timestamp types in PostgreSQL
            supported_types = ['timestamp without time zone', 'timestamp with time zone', 'date', 'timestamptz', 'timestamp']
            if not any(stype in data_type for stype in supported_types):
                raise ValueError(f"Partition field '{field_to_validate}' in {schema_of_table}.{table_to_validate} has type '{data_type}'. Must be a timestamp or date type for range partitioning by year.")

            print_message(f"Validation successful: Partition field '{field_to_validate}' in {schema_of_table}.{table_to_validate} is type '{data_type}'.", "SUCCESS")
    except Exception as e:
        print_message(f"Error validating partition field for {schema_of_table}.{table_to_validate}: {e}", "ERROR")
        raise

def get_primary_key_info(conn, table_name_pk, schema_pk):
    try:
        with conn.cursor() as cur:
            # Get PK constraint name and columns
            cur.execute("""
                SELECT tc.constraint_name, array_agg(kcu.column_name::text ORDER BY kcu.ordinal_position) AS columns 
                FROM information_schema.table_constraints tc 
                JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema 
                WHERE tc.table_schema = %s AND tc.table_name = %s AND tc.constraint_type = 'PRIMARY KEY' 
                GROUP BY tc.constraint_name
            """, (schema_pk, table_name_pk))
            result = cur.fetchone()
            return {'name': result[0], 'columns': list(result[1])} if result else {'name': None, 'columns': []}
    except Exception as e:
        print_message(f"Error retrieving PK info for {schema_pk}.{table_name_pk}: {e}", "ERROR")
        return {'name': None, 'columns': []} # Return a default structure on error

def get_year_range(conn, table_name_for_range, schema_for_range, partition_by_field):
    try:
        with conn.cursor() as cur:
            # Query to get min and max year from the partition field
            query = sql.SQL("SELECT EXTRACT(YEAR FROM MIN({}))::int, EXTRACT(YEAR FROM MAX({}))::int FROM {}.{} WHERE {} IS NOT NULL").format(
                sql.Identifier(partition_by_field), sql.Identifier(partition_by_field),
                sql.Identifier(schema_for_range), sql.Identifier(table_name_for_range),
                sql.Identifier(partition_by_field) # Added WHERE clause to ignore NULLs
            )
            cur.execute(query)
            min_year, max_year = cur.fetchone()

            current_year = datetime.now().year
            # Handle cases where table might be empty or all partition_by_field values are NULL
            min_year = min_year if min_year is not None else current_year
            max_year = max_year if max_year is not None else current_year

            if min_year > max_year: # Should not happen if data exists and MIN/MAX work
                print_message(f"Warning: Min year ({min_year}) is greater than max year ({max_year}) for {schema_for_range}.{table_name_for_range}. Defaulting to current year range.", "WARNING")
                max_year = min_year

            return min_year, max_year
    except Exception as e:
        print_message(f"Error determining year range for {schema_for_range}.{table_name_for_range} (field {partition_by_field}): {e}. Using default range (current year).", "WARNING")
        current_year = datetime.now().year
        return current_year, current_year # Default to a single year partition for current year

# --- Main Execution Function ---
def execute_partition_steps(conn, table_to_partition_name, partition_by_field, dry_run=False):
    new_partitioned_table_name = table_to_partition_name # The target partitioned table will have the same name
    tmp_table_name = f"{table_to_partition_name}_etarc_tmp_old" # More unique temp table name
    partitions_schema_name = "partitions" # Schema for individual partition tables

    dropped_referencing_fks_info = [] # FKs from other tables pointing to this one
    dropped_on_table_fks_info = []    # FKs defined on this table, pointing to others
    dropped_triggers_info = []        # Triggers on this table
    dependent_views_info = []         # Views depending on this table

    related_tables_result = None # To store result from process_related_tables_for_partitioning

    try:
        actual_schema = table_exists(conn, table_to_partition_name)
        if not actual_schema:
            raise ValueError(f"Table '{table_to_partition_name}' does not exist in any accessible schema.")

        print_message(f"Processing table {Style.BOLD}{actual_schema}.{table_to_partition_name}{Style.RESET} for partitioning by field '{Style.UNDERLINE}{partition_by_field}{Style.RESET}'.", "SUBHEADER")

        if is_table_partitioned(conn, new_partitioned_table_name, actual_schema):
            print_message(f"Table {actual_schema}.{new_partitioned_table_name} is already a partitioned table. Skipping.", "INFO")
            return {'success': True, 'message': 'Already partitioned', 'details': []} # Return a success-like structure

        # --- Step 0: Initial Validations and Info Gathering ---
        print_message(f"\n--- Validating and Preparing Original Table: {actual_schema}.{table_to_partition_name} ---", "SUBHEADER")
        get_table_schema(conn, table_to_partition_name, actual_schema) # For debug/info
        validate_partition_field(conn, table_to_partition_name, actual_schema, partition_by_field) # Critical validation

        pk_info = get_primary_key_info(conn, table_to_partition_name, actual_schema)
        original_pk_name = pk_info['name']
        original_pk_columns = pk_info['columns']
        if original_pk_name: print_message(f"Original Primary Key found: Name='{original_pk_name}', Columns={original_pk_columns}", "INFO")
        else: print_message(f"No Primary Key found on {actual_schema}.{table_to_partition_name}.", "WARNING")


        # --- Step 1.5: Process Related Tables (Critical - Moved Earlier) ---
        print_message(f"\n[Step 1.5] Pre-processing related tables for partition field '{partition_by_field}' propagation...", "STEP")
        # This step adds the etarc_<partition_field> to tables that FK to the current table_to_partition
        # and populates it.
        related_tables_result = process_related_tables_for_partitioning(
            conn, table_to_partition_name, actual_schema, partition_by_field, dry_run
        )

        if not related_tables_result['success']:
            print_message(f"{Style.FAILURE} Processing of related tables failed for {actual_schema}.{table_to_partition_name}.", "FAILURE")
            print_message("Aborting main partitioning process for this table due to related table errors. Review logs from 'process_related_tables_for_partitioning'.", "ERROR")
            if not dry_run:
                print_message("Rolling back any changes made during related table processing for this table...", "INFO")
                conn.rollback() # Rollback changes from the failed related_tables_result processing
            return related_tables_result # Return the failure details

        print_message(f"{Style.SUCCESS} Related tables pre-processing completed.", "SUCCESS")
        # If dry_run or successful, changes by process_related_tables_for_partitioning are committed per table or simulated.
        # The main transaction for execute_partition_steps continues.

        # --- Main Partitioning Steps ---
        print_message(f"\n[Step 1] Dropping dependent views on {actual_schema}.{table_to_partition_name}...", "STEP")
        dependent_views = get_dependent_views(conn, table_to_partition_name, actual_schema)
        if not dependent_views: print_message("No dependent views found.", "STEP_INFO")
        for vs, vn, vdef in dependent_views:
            dependent_views_info.append({'schema': vs, 'name': vn, 'definition': vdef})
            s = sql.SQL("DROP VIEW IF EXISTS {}.{} CASCADE").format(sql.Identifier(vs), sql.Identifier(vn))
            if dry_run: print_message(f"[Dry Run] Would drop view: {s.as_string(conn)}", "STEP_INFO")
            else: conn.cursor().execute(s); print_message(f"Dropped view {vs}.{vn}", "STEP_INFO")

        print_message(f"\n[Step 2] Dropping Foreign Keys from OTHER tables that reference {actual_schema}.{table_to_partition_name}...", "STEP")
        fks_referencing_source = get_foreign_keys(conn, table_to_partition_name, actual_schema, direction="to")
        if not fks_referencing_source: print_message("No external FKs found referencing this table.", "STEP_INFO")
        for fk in fks_referencing_source:
            s_fk = sql.SQL("ALTER TABLE {}.{} DROP CONSTRAINT IF EXISTS {}").format(
                sql.Identifier(fk['referencing_schema']), sql.Identifier(fk['referencing_table']), sql.Identifier(fk['name']))
            if dry_run: print_message(f"[Dry Run] External FK Drop: {s_fk.as_string(conn)}", "STEP_INFO")
            else: conn.cursor().execute(s_fk); print_message(f"Dropped external FK '{fk['name']}' from {fk['referencing_schema']}.{fk['referencing_table']}", "STEP_INFO")
            dropped_referencing_fks_info.append(fk)
            # Note: Dropping supporting indexes for these external FKs is complex as index names might not be standard.
            # This script version does not automatically drop indexes on OTHER tables that supported these FKs.
            # This might need manual intervention or a more sophisticated index discovery.

        print_message(f"\n[Step 3] Dropping FKs defined ON {actual_schema}.{table_to_partition_name} (pointing to other tables)...", "STEP")
        fks_on_this_table = get_foreign_keys(conn, table_to_partition_name, actual_schema, direction="on")
        if not fks_on_this_table: print_message("No FKs found defined on this table.", "STEP_INFO")
        for fk in fks_on_this_table:
            dropped_on_table_fks_info.append(fk)
            s_fk = sql.SQL("ALTER TABLE {}.{} DROP CONSTRAINT IF EXISTS {}").format(
                sql.Identifier(actual_schema), sql.Identifier(table_to_partition_name), sql.Identifier(fk['name']))
            if dry_run: print_message(f"[Dry Run] Own FK Drop: {s_fk.as_string(conn)}", "STEP_INFO")
            else: conn.cursor().execute(s_fk); print_message(f"Dropped own FK '{fk['name']}' on {actual_schema}.{table_to_partition_name}", "STEP_INFO")
            # Indexes supporting these FKs (if any) on the table_to_partition_name will be gone with the table rename/recreate approach.

        print_message(f"\n[Step 4] Dropping user-defined triggers on {actual_schema}.{table_to_partition_name}...", "STEP")
        # drop_triggers itself handles dry_run and returns info
        dropped_triggers_info = drop_triggers(conn, table_to_partition_name, actual_schema, dry_run)
        if not dropped_triggers_info : print_message("No user-defined triggers found to drop or would be dropped.", "STEP_INFO")

        if original_pk_name:
            print_message(f"\n[Step 5] Dropping original PK '{original_pk_name}' from {actual_schema}.{table_to_partition_name}...", "STEP")
            s = sql.SQL("ALTER TABLE {}.{} DROP CONSTRAINT IF EXISTS {}").format(
                sql.Identifier(actual_schema), sql.Identifier(table_to_partition_name), sql.Identifier(original_pk_name))
            if dry_run: print_message(f"[Dry Run] PK Drop: {s.as_string(conn)}", "STEP_INFO")
            else: conn.cursor().execute(s); print_message(f"Dropped PK '{original_pk_name}'", "STEP_INFO")
        else:
            print_message(f"\n[Step 5] Skipping PK drop as no original PK was found.", "STEP_INFO")

        print_message(f"\n[Step 6] Renaming {actual_schema}.{table_to_partition_name} to {actual_schema}.{tmp_table_name}...", "STEP")
        if table_exists(conn, tmp_table_name, actual_schema):
            # Safety: if temp table from a failed previous run exists, drop it in non-dry-run.
            if not dry_run:
                print_message(f"Temporary table {actual_schema}.{tmp_table_name} already exists. Dropping it...", "WARNING")
                conn.cursor().execute(sql.SQL("DROP TABLE IF EXISTS {}.{}").format(sql.Identifier(actual_schema), sql.Identifier(tmp_table_name)))
            else:
                print_message(f"[Dry Run] Temporary table {actual_schema}.{tmp_table_name} already exists. Would attempt to drop if not dry run.", "WARNING")
                # In dry run, we can't proceed if it exists as rename would fail.
                # raise Exception(f"[Dry Run Abort] Temp table {actual_schema}.{tmp_table_name} already exists. Cannot simulate rename.")

        stmt_rename = sql.SQL("ALTER TABLE {}.{} RENAME TO {}").format(sql.Identifier(actual_schema), sql.Identifier(table_to_partition_name), sql.Identifier(tmp_table_name))
        if dry_run: print_message(f"[Dry Run] Rename to temp: {stmt_rename.as_string(conn)}", "STEP_INFO")
        else: conn.cursor().execute(stmt_rename); print_message(f"Renamed original table to {actual_schema}.{tmp_table_name}", "STEP_INFO")

        print_message(f"\n--- Creating New Partitioned Table {actual_schema}.{new_partitioned_table_name} & Migrating Data ---", "SUBHEADER")

        print_message(f"\n[Step 7] Creating new partitioned table shell {actual_schema}.{new_partitioned_table_name}...", "STEP")
        # Create the partitioned table shell (LIKE includes defaults, comments, storage options but NOT constraints/indexes)
        stmt_create_part_shell = sql.SQL("CREATE TABLE {}.{} (LIKE {}.{} INCLUDING DEFAULTS INCLUDING STORAGE INCLUDING COMMENTS) PARTITION BY RANGE ({})").format(
            sql.Identifier(actual_schema), sql.Identifier(new_partitioned_table_name),
            sql.Identifier(actual_schema), sql.Identifier(tmp_table_name), # Likeness from temp table
            sql.Identifier(partition_by_field))
        if dry_run: print_message(f"[Dry Run] Create partitioned shell: {stmt_create_part_shell.as_string(conn)}", "STEP_INFO")
        else: conn.cursor().execute(stmt_create_part_shell); print_message(f"Created partitioned table shell {actual_schema}.{new_partitioned_table_name}", "STEP_INFO")

        print_message(f"\n[Step 9] Ensuring partitions schema '{partitions_schema_name}' exists...", "STEP")
        stmt_create_schema = sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(partitions_schema_name))
        if dry_run: print_message(f"[Dry Run] Ensure schema: {stmt_create_schema.as_string(conn)}", "STEP_INFO")
        else: conn.cursor().execute(stmt_create_schema); print_message(f"Schema '{partitions_schema_name}' ensured.", "STEP_INFO")

        print_message(f"\n[Step 10] Determining year range for partitions from {actual_schema}.{tmp_table_name}...", "STEP")
        start_year, end_year = get_year_range(conn, tmp_table_name, actual_schema, partition_by_field)
        current_year = datetime.now().year
        if current_year > end_year:
            end_year = current_year
        print_message(f"Data year range found: {start_year} to {end_year} (inclusive). Partitions will be created up to {end_year + 1}.", "STEP_INFO")

        print_message(f"\n[Step 11] Creating yearly partitions for {actual_schema}.{new_partitioned_table_name}...", "STEP")
        # Create partitions for the determined range + one future year
        # Handle case where start_year might be > end_year if table is empty or only has NULLs
        if start_year > end_year :
            print_message(f"Warning: start_year ({start_year}) is greater than end_year ({end_year}). This can happen with empty tables or all NULLs in partition field. Creating a default partition for current year.", "WARNING")
            current_year_for_default_part = datetime.now().year
            start_year = current_year_for_default_part
            end_year = current_year_for_default_part # Will create for current year and current_year + 1

        for year_val in range(start_year, end_year + 2): # +2 to create one year beyond max data year
            part_table_actual_name = f"{new_partitioned_table_name}_y{year_val}"
            from_date_str = f"{year_val}-01-01"
            to_date_str = f"{year_val + 1}-01-01" # Exclusive upper bound

            stmt_create_actual_part = sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} PARTITION OF {}.{} FOR VALUES FROM (%s) TO (%s)").format(
                sql.Identifier(partitions_schema_name), sql.Identifier(part_table_actual_name),
                sql.Identifier(actual_schema), sql.Identifier(new_partitioned_table_name))

            if dry_run: print_message(f"[Dry Run] Would ensure partition: {stmt_create_actual_part.as_string(conn)} with ('{from_date_str}', '{to_date_str}')", "STEP_INFO")
            else:
                try:
                    conn.cursor().execute(stmt_create_actual_part, (from_date_str, to_date_str))
                    print_message(f"Ensured partition {partitions_schema_name}.{part_table_actual_name} for {from_date_str} TO {to_date_str}", "STEP_INFO")
                except psycopg2.Error as e_part: # Catch specific error if partition already exists or overlaps
                    if "already exists" in str(e_part) or "would overlap" in str(e_part):
                        print_message(f"Partition for year {year_val} ({partitions_schema_name}.{part_table_actual_name}) likely already exists or overlaps. Skipping creation. ({e_part})", "WARNING")
                        conn.rollback() # Rollback the failed CREATE TABLE PARTITION OF
                    else:
                        print_message(f"Error creating partition for year {year_val}: {e_part}", "ERROR")
                        raise # Re-raise for more serious issues


        print_message(f"\n[Step 12] Copying data from {actual_schema}.{tmp_table_name} to {actual_schema}.{new_partitioned_table_name}...", "STEP")
        # This will route data to appropriate partitions.
        stmt_copy_data = sql.SQL("INSERT INTO {}.{} SELECT * FROM {}.{}").format(
            sql.Identifier(actual_schema), sql.Identifier(new_partitioned_table_name),
            sql.Identifier(actual_schema), sql.Identifier(tmp_table_name))
        if dry_run: print_message(f"[Dry Run] Data copy: {stmt_copy_data.as_string(conn)}", "STEP_INFO")
        else:
            with conn.cursor() as cur:
                cur.execute(stmt_copy_data)
                print_message(f"Copied {cur.rowcount} rows from temp table to {actual_schema}.{new_partitioned_table_name}", "STEP_INFO")

        print_message(f"\n--- Data migration to {actual_schema}.{new_partitioned_table_name} complete. ---", "SUCCESS")

        # Information about dropped FKs - these are NOT recreated by this script.
        if dropped_referencing_fks_info:
            print_message("\nINFO: The following FKs from OTHER tables (that were referencing the original table) were DROPPED and NOT automatically recreated:", "IMPORTANT")
            for fk_info in dropped_referencing_fks_info: print_message(f"  - FK '{fk_info['name']}' on {fk_info['referencing_schema']}.{fk_info['referencing_table']} (Def: {fk_info['definition']})", "INFO")

        if dropped_on_table_fks_info:
            print_message("\nINFO: The following FKs defined ON the original table (pointing to other tables) were effectively removed during recreation and NOT automatically recreated on the new partitioned table:", "IMPORTANT")
            for fk_info in dropped_on_table_fks_info: print_message(f"  - Original FK '{fk_info['name']}' (Def: {fk_info['definition']})", "INFO")

        if not dry_run:
            print_message("\nCommitting main partitioning changes...", "DB_CONNECT")
            conn.commit()
        else:
            print_message("\n[Dry Run] Main partitioning changes would be committed here. Rolling back simulation.", "DB_CONNECT")
            conn.rollback()

        print_message(f"\n{'[Dry Run] Simulated' if dry_run else 'Successfully completed'} main partitioning process for {actual_schema}.{new_partitioned_table_name}.", "SUCCESS" if not dry_run else "INFO")

        # Dropping the temporary table is now a separate, committed step if not dry_run
        if not dry_run:
            print_message(f"\n[Step 15] Dropping temporary table {actual_schema}.{tmp_table_name}...", "STEP")
            stmt_drop_tmp_table = sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                sql.Identifier(actual_schema), sql.Identifier(tmp_table_name)
            )
            try:
                # Dropping table often needs to be outside transaction or in its own.
                with conn.cursor() as cur:
                    cur.execute(stmt_drop_tmp_table)
                print_message(f"Successfully dropped temporary table {actual_schema}.{tmp_table_name}.", "SUCCESS")
            except Exception as e_drop_tmp:
                print_message(f"Error dropping temporary table {actual_schema}.{tmp_table_name}: {e_drop_tmp}", "ERROR")
                print_message(f"  You may need to drop it manually: {stmt_drop_tmp_table.as_string(conn)}", "INFO")
        else:
            print_message(f"\n[Step 15] [Dry Run] Would drop temporary table {actual_schema}.{tmp_table_name}.", "STEP_INFO")


        print_message(f"\n{Style.SUCCESS} Full partitioning process for {actual_schema}.{new_partitioned_table_name} {'simulated.' if dry_run else 'completed successfully.'}", "SUCCESS")
        return related_tables_result # Return the result from Step 1.5 (should be success if we got here)

    except Exception as e:
        error_msg = f"ERROR during partitioning of {actual_schema if 'actual_schema' in locals() else ''}.{table_to_partition_name if 'table_to_partition_name' in locals() else 'UNKNOWN'}: {e}"
        print_message(error_msg, "ERROR")
        traceback.print_exc()
        if conn and not conn.closed:
            try:
                if not dry_run: conn.rollback()
                print_message("Rolled back transaction due to error.", "DB_CONNECT")
            except Exception as rb_e: print_message(f"Rollback error: {rb_e}", "ERROR")
        # Return the related_tables_result if it exists and indicates failure, otherwise None for main partitioning error
        if related_tables_result and not related_tables_result.get('success', True):
            return related_tables_result # Propagate earlier failure
        return None # Indicates a failure within this function's main steps

def print_final_summary(summary_data):
    """Prints the final summary of the migration."""
    global _use_ansi_output

    # Main flow
    print_header()
    fmt_plain, fmt_ansi = print_table_header()

    success = failure = skipped = 0
    rel_proc = rel_fail = rel_upd = 0

    for item in summary_data:
        status, p, f, u = print_row(item, fmt_plain, fmt_ansi)
        if status == 'SUCCESS':
            success += 1
        elif status == 'FAILURE':
            failure += 1
        elif status == 'SKIPPED':
            skipped += 1
        rel_proc += p
        rel_fail += f
        rel_upd += u

    print_message("-" * (120 if _use_ansi_output else len(fmt_plain.format("", "", "", ""))), "IMPORTANT")
    print_totals(len(summary_data), success, failure, skipped)
    print_related_summary(rel_proc, rel_fail, rel_upd)
    print_message("=" * 80, "HEADER")

def print_header():
    section_line = "=" * 80
    print_message(f"\n\n{Style.SUMMARY} {section_line}", "HEADER")
    print_message(f"{' ' * 25}{Style.BOLD}TABLE MIGRATION SUMMARY{Style.RESET}", "HEADER")
    print_message(section_line, "HEADER")

def print_table_header():
    header_plain = "{:<30} | {:<10} | {:<15} | {}"
    header_ansi = "{:<30} | {:<20} | {:<25} | {}"

    if _use_ansi_output:
        header = header_ansi.format(
            f"{Style.BOLD}Table{Style.RESET}",
            f"{Style.BOLD}Status{Style.RESET}",
            f"{Style.BOLD}Rel. Tables (Proc/Fail){Style.RESET}",
            f"{Style.BOLD}Message/Details{Style.RESET}"
        )
        print_message(header, "IMPORTANT")
        print_message("-" * 120, "IMPORTANT")
    else:
        header = header_plain.format("Table", "Status", "Rel. Tables", "Message/Details")
        print_message(header, "IMPORTANT")
        print_message("-" * len(header), "IMPORTANT")
    return header_plain, header_ansi

def format_status(status):
    if status == 'SUCCESS':
        return f"{Style.GREEN}{Style.SUCCESS} SUCCESS{Style.RESET}", 'SUCCESS'
    elif status == 'FAILURE':
        return f"{Style.RED}{Style.FAILURE} FAILURE{Style.RESET}", 'FAILURE'
    elif status == 'SKIPPED':
        return f"{Style.YELLOW}{Style.WARNING} SKIPPED{Style.RESET}", 'SKIPPED'
    return status, status

def format_related_info(info):
    if not info:
        return "N/A", f"{Style.BLUE}N/A{Style.RESET}", 0, 0, 0

    processed = info.get('processed_tables', 0)
    failed = info.get('failed_tables', 0)
    updated = info.get('total_records_updated', 0)

    plain = f"{processed}✓ / {failed}✗ ({updated} upd)"
    if failed > 0:
        ansi = f"{Style.GREEN}{processed}✓{Style.RESET} / {Style.RED}{Style.BOLD}{failed}✗{Style.RESET} ({Style.CYAN}{updated} upd{Style.RESET})"
    elif processed > 0:
        ansi = f"{Style.GREEN}{processed}✓{Style.RESET} / 0✗ ({Style.CYAN}{updated} upd{Style.RESET})"
    else:
        ansi = f"{Style.BLUE}0✓ / 0✗ (0 upd){Style.RESET}"
    return plain, ansi, processed, failed, updated

def print_row(item, fmt_plain, fmt_ansi):
    table = item['table_name']
    status = item['status']
    message = item.get('message', '').replace('\n', ' ')[:100]
    if len(message) > 100:
        message += '...'

    rel_plain, rel_ansi, p, f, u = format_related_info(item.get('related_tables_result'))
    status_ansi, status_plain = format_status(status)

    if _use_ansi_output:
        print(fmt_ansi.format(table, status_ansi, rel_ansi, message))
    else:
        print(fmt_plain.format(table, status_plain, rel_plain, message))
    return status_plain, p, f, u

def print_totals(total, success, failure, skipped):
    print_message(f"Total Tables Considered: {total}", "INFO")
    print_message(f"  {Style.SUCCESS} Successful: {success}", "SUCCESS")
    print_message(f"  {Style.FAILURE} Failed: {failure}", "FAILURE" if failure > 0 else "INFO")
    if skipped > 0:
        print_message(f"  {Style.WARNING} Skipped: {skipped}", "WARNING")

def print_related_summary(proc, fail, upd):
    if proc == 0 and fail == 0:
        return
    print_message("\nRelated Tables Processing Summary (Global):", "SUBHEADER")
    print_message(f"  Total Related Tables Processing Instances: {proc}", "INFO")
    print_message(f"  Total Failures in Related Tables: {fail}", "FAILURE" if fail > 0 else "INFO")
    print_message(f"  Total Records Updated in Related Tables: {upd}", "INFO")


# --- Main Program Flow ---
def main_flow(dry_run=False):
    if not validate_config_directory():
        return False

    properties = read_properties_file("config/Openbravo.properties")
    if not properties or not properties.get(BBDD_URL):
        print_message("Failed to read properties file, or essential properties are missing. Exiting.", "FAILURE")
        return False

    if not is_supported_rdbms(properties):
        print_message(f"Unsupported RDBMS: {properties.get(BBDD_RDBMS)}. Only POSTGRE is supported.", "FAILURE")
        return False

    db_params = parse_db_params(properties)
    migration_summary = []

    try:
        conn = connect_to_database(db_params)
        apply_session_config(conn, properties, dry_run)

        table_fields_config = get_tables_and_fields_from_database(conn)
        if not table_fields_config:
            print_message("No tables configured for processing from database. Script will exit.", "INFO")
            return False

        migration_summary = process_all_tables(conn, table_fields_config, dry_run)
    except Exception as e:
        handle_global_exception(e, migration_summary)
        return False
    finally:
        print_final_summary(migration_summary)
        close_connection_if_open(locals().get('conn'))

    return all(entry['status'] == 'SUCCESS' for entry in migration_summary)


# --- Funciones auxiliares ---

def validate_config_directory():
    config_dir = Path("config")
    if not config_dir.exists():
        print_message(f"Configuration directory '{config_dir}' does not exist. Please create it and place 'Openbravo.properties' inside.", "ERROR")
        return False
    return True

def is_supported_rdbms(properties):
    return properties.get(BBDD_RDBMS, '').upper() == 'POSTGRE'

def connect_to_database(params):
    print_message(f"Connecting to PostgreSQL: Host={params['host']}, Port={params['port']}, DB={params['database']}", "DB_CONNECT")
    return psycopg2.connect(**params)

def apply_session_config(conn, properties, dry_run):
    session_config = properties.get('bbdd.sessionConfig')
    if session_config:
        with conn.cursor() as cur:
            if dry_run:
                print_message(f"[Dry Run] Would apply session config: {session_config}", "INFO")
            else:
                print_message(f"Applying session configuration: {session_config}", "DB_CONNECT")
                cur.execute(session_config)
                conn.commit()
                print_message("Session configuration applied.", "SUCCESS")

def process_all_tables(conn, table_fields_config, dry_run):
    summary = []
    for table_name, fields in table_fields_config.items():
        result = process_single_table(conn, table_name, fields, dry_run)
        summary.append(result)
    return summary

def process_single_table(conn, table_name, fields, dry_run):
    if not fields:
        print_message(f"\n--- Skipping table: {table_name} (No partition fields defined in config.) ---", "WARNING")
        return {
            'table_name': table_name, 'status': 'SKIPPED',
            'message': 'No partition fields specified in DB config.',
            'related_tables_result': None
        }

    partition_field = next(iter(fields))
    if len(fields) > 1:
        print_message(f"Warning: Multiple partition fields defined for {table_name}: {fields}. Using '{partition_field}'.", "WARNING")

    header_line = "=" * 80
    print_message(f"\n\n{header_line}", "TABLE_PROCESS")
    print_message(f"=== Processing Table: {Style.BOLD}{table_name}{Style.RESET} (Partition Field: {Style.UNDERLINE}{partition_field}{Style.RESET}) ===", "TABLE_PROCESS")
    print_message(header_line, "TABLE_PROCESS")

    current_table_outcome = None
    try:
        current_table_outcome = execute_partition_steps(conn, table_name, partition_field, dry_run)
        return build_table_result(table_name, current_table_outcome, dry_run)
    except Exception as e:
        return handle_table_exception(e, conn, table_name, current_table_outcome, dry_run)
    finally:
        print_message(f"=== Finished processing for table: {table_name} ===", "TABLE_PROCESS")
        print_message(f"{header_line}\n", "TABLE_PROCESS")

def build_table_result(table_name, outcome, dry_run):
    if outcome is None:
        return {
            'table_name': table_name, 'status': 'FAILURE',
            'message': 'Error during main partitioning steps (see logs above).',
            'related_tables_result': None
        }
    elif outcome.get('success', False):
        return {
            'table_name': table_name, 'status': 'SUCCESS',
            'message': 'Partitioning process completed successfully.' if not dry_run else 'Partitioning process simulated successfully.',
            'related_tables_result': outcome
        }
    else:
        return {
            'table_name': table_name, 'status': 'FAILURE',
            'message': 'Related table processing failed; main partitioning aborted. Check related table details.',
            'related_tables_result': outcome
        }

def handle_table_exception(exception, conn, table_name, outcome, dry_run):
    error_msg = str(exception).replace('\n', ' ')
    print_message(f"!!!!!!!! CRITICAL FAILURE processing table {table_name}: {exception} !!!!!!!!", "FAILURE")
    traceback.print_exc()
    if not dry_run and conn and not conn.closed:
        conn.rollback()
    return {
        'table_name': table_name, 'status': 'FAILURE',
        'message': f"Critical unhandled error for table: {error_msg[:150]}",
        'related_tables_result': outcome
    }

def handle_global_exception(e, summary):
    print_message(f"UNHANDLED GLOBAL SCRIPT ERROR: {e}", "ERROR")
    traceback.print_exc()
    summary.append({
        'table_name': 'GLOBAL_EXECUTION',
        'status': 'FAILURE',
        'message': f'Error no controlado: {str(e)}',
        'related_tables_result': None
    })

def close_connection_if_open(conn):
    if conn and not conn.closed:
        conn.close()
        print_message("Database connection closed.", "DB_CONNECT")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Automated script to migrate PostgreSQL tables to a range-partitioned structure by a date/timestamp field. "
                    "Key operations: Modifies PK, handles dependencies (drops, some recreations), migrates data, drops temp table. "
                    "Configuration is sourced from database tables (ETARC_TABLE_CONFIG, AD_TABLE, AD_COLUMN). "
                    "Related tables (with FKs to the source table) will have a corresponding 'etarc_<partition_field>' added and populated. "
                    "Indexes (non-PK, non-FK supporting on source) are NOT automatically copied. FKs are NOT automatically recreated. Triggers on source table are dropped, basic recreation might be attempted but needs review."
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help="Simulate all DDL/DML operations. No actual changes are made to the database. Useful for verification."
    )
    parser.add_argument(
        '--plain-output',
        action='store_true',
        help="Disable colored/emoji output, show plain text only."
    )
    args = parser.parse_args()

    _use_ansi_output = not args.plain_output

    script_version_message = f"Script Version: PK Modified, Dependencies Handled (No Cascade), Data Migrated, Temp Table Dropped, Related Tables Processed (etarc_field added/populated). Output: {'Enhanced ANSI' if _use_ansi_output else 'Plain Text'}"
    print_message(script_version_message, "SCRIPT_INFO")
    print_message(f"Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", "SCRIPT_INFO")

    run_mode_header = "*" * 50
    if args.dry_run:
        print_message(f"\n{run_mode_header}", "RUN_MODE")
        print_message(f"{Style.DRY_RUN} *** DRY RUN MODE ENABLED     ***", "RUN_MODE")
        print_message(f"*** No actual changes will be made to the database. ***", "RUN_MODE")
        print_message(run_mode_header, "RUN_MODE")
    else:
        print_message(f"\n{run_mode_header}", "RUN_MODE")
        print_message(f"{Style.LIVE_RUN} *** LIVE RUN MODE ENABLED    ***", "RUN_MODE")
        print_message(f"{Style.BOLD}{Style.RED}*** Changes WILL be made to the database. Review script and backup data before proceeding! ***{Style.RESET}", "RUN_MODE")
        print_message(run_mode_header, "RUN_MODE")
        try:
            user_confirmation = input(f"{Style.YELLOW}Are you sure you want to proceed with the LIVE RUN? (yes/no): {Style.RESET}")
            if user_confirmation.lower() != 'yes':
                print_message("Live run cancelled by user. Exiting.", "WARNING")
                exit()
            print_message("User confirmed LIVE RUN. Proceeding...", "INFO")
        except KeyboardInterrupt:
            print_message("\nLive run cancelled by user (Ctrl+C). Exiting.", "WARNING")
            exit()

    result = main_flow(dry_run=args.dry_run)
    exit(0 if result else 1)
