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

# Variable global para controlar el uso de salida ANSI (colores/emojis)
_use_ansi_output = True

class Style:
    """Clase para definir estilos de consola (colores ANSI y emojis)."""
    # Colores ANSI
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = ' '
    MAGENTA = ' '
    CYAN = ' '
    WHITE = ' '
    RESET = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = ' '

    # Emojis (asegúrate de que tu terminal los soporte y esté configurada para UTF-8)
    SUCCESS = '✅'
    FAILURE = '❌'
    WARNING = '⚠️'
    INFO = ' '
    STEP = ' '
    PROCESSING = ' '
    HEADER = ' '
    FINISH = '🏁'
    TABLE = ' '
    SCRIPT_VERSION = ' '
    DRY_RUN = '🧪'
    LIVE_RUN = ' '
    DATABASE = ' '
    CONFIGURATION = ' '
    SUMMARY = ' '
    ERROR_ICON = ' ' # Para errores en el resumen

def print_message(message, level="INFO", end="\n"):
    """
    Imprime un mensaje con el estilo apropiado basado en el nivel y la configuración ANSI.
    """
    global _use_ansi_output
    if not _use_ansi_output:
        print(message, end=end)
        return

    prefix = ""
    color = Style.RESET
    emoji = ""

    if level == "HEADER":
        color = Style.MAGENTA + Style.BOLD
        emoji = Style.HEADER + " "
    elif level == "SUBHEADER":
        color = Style.CYAN + Style.BOLD
        emoji = Style.PROCESSING + " "
    elif level == "STEP_INFO": # Para información dentro de un paso
        color = Style.BLUE
        emoji = "  " # Indentación para sub-pasos o información detallada
    elif level == "STEP":
        color = Style.BLUE + Style.BOLD
        emoji = Style.STEP + " "
    elif level == "SUCCESS":
        color = Style.GREEN + Style.BOLD
        emoji = Style.SUCCESS + " "
    elif level == "FAILURE":
        color = Style.RED + Style.BOLD
        emoji = Style.FAILURE + " "
    elif level == "ERROR": # Para errores críticos o excepciones
        color = Style.RED + Style.BOLD
        emoji = Style.ERROR_ICON + " "
    elif level == "WARNING":
        color = Style.YELLOW
        emoji = Style.WARNING + " "
    elif level == "INFO":
        color = Style.WHITE
        emoji = Style.INFO + " "
    elif level == "DEBUG":
        color = Style.CYAN
        emoji = "  "
    elif level == "IMPORTANT":
        color = Style.YELLOW + Style.BOLD
        emoji = "  "
    elif level == "SCRIPT_INFO":
        color = Style.MAGENTA
        emoji = Style.SCRIPT_VERSION + " "
    elif level == "RUN_MODE":
        color = Style.YELLOW + Style.BOLD
    elif level == "DB_CONNECT":
        color = Style.GREEN
        emoji = Style.DATABASE + " "
    elif level == "CONFIG_LOAD":
        color = Style.CYAN
        emoji = Style.CONFIGURATION + " "
    elif level == "TABLE_PROCESS":
        color = Style.MAGENTA + Style.BOLD
        emoji = Style.TABLE + " "


    print(f"{color}{emoji}{message}{Style.RESET}", end=end)

# --- Funciones auxiliares ---
def read_properties_file(file_path):
    try:
        config = ConfigParser()
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f"[default]\n{f.read()}"
        config.read_string(content)
        return {
            'source.path': config.get('default', 'source.path', fallback=None),
            'bbdd.rdbms': config.get('default', 'bbdd.rdbms', fallback=None),
            'bbdd.url': config.get('default', 'bbdd.url', fallback=None),
            'bbdd.sid': config.get('default', 'bbdd.sid', fallback=None),
            'bbdd.user': config.get('default', 'bbdd.user', fallback=None),
            'bbdd.password': config.get('default', 'bbdd.password', fallback=None),
            'bbdd.sessionConfig': config.get('default', 'bbdd.sessionConfig', fallback=None)
        }
    except Exception as e:
        print_message(f"Error reading {file_path}: {e}", "ERROR")
        return {}

def parse_db_params(properties):
    if not all([properties.get('bbdd.url'), properties.get('bbdd.sid'), properties.get('bbdd.user'), properties.get('bbdd.password')]):
        raise ValueError("Missing required database connection properties (bbdd.url, bbdd.sid, bbdd.user, bbdd.password)")
    url_match = re.match(r'jdbc:postgresql://([^:]+):(\d+)', properties['bbdd.url'])
    if not url_match:
        raise ValueError(f"Invalid bbdd.url format: {properties['bbdd.url']}. Expected format: jdbc:postgresql://host:port")
    host, port = url_match.groups()
    return {'host': host, 'port': port, 'database': properties['bbdd.sid'],
            'user': properties.get('bbdd.user'), 'password': properties.get('bbdd.password')}

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

        with conn.cursor() as cur:
            cur.execute(query)
            results = cur.fetchall()

            for table_name, column_name in results:
                table_fields_config[table_name].add(column_name)
                print_message(f"Found configuration: Table={table_name}, Partition Field={column_name}", "CONFIG_LOAD")

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
        with conn.cursor() as cur:
            cur.execute("SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position", (schema, table_name))
            schema_info = cur.fetchall()
            print_message(f"Schema for {schema}.{table_name}:", "INFO")
            for col_name, col_type, nullable, default in schema_info:
                print_message(f"  - Column: {col_name}, Type: {col_type}, Nullable: {nullable}, Default: {default}", "DEBUG")
            return schema_info
    except Exception as e:
        print_message(f"Error retrieving schema for {schema}.{table_name}: {e}", "ERROR")
        return []

def table_exists(conn, table_name_to_check, schema_to_check=None):
    try:
        with conn.cursor() as cur:
            if schema_to_check:
                cur.execute("SELECT table_schema FROM information_schema.tables WHERE table_name = %s AND table_schema = %s", (table_name_to_check, schema_to_check))
            else:
                cur.execute("SELECT table_schema FROM information_schema.tables WHERE table_name = %s AND table_schema NOT IN ('pg_catalog', 'information_schema')", (table_name_to_check,))
            result = cur.fetchone()
            return result[0] if result else None
    except Exception as e:
        print_message(f"Error checking if table '{table_name_to_check}' exists (schema: {schema_to_check if schema_to_check else 'any'}): {e}", "ERROR")
        return None

def is_table_partitioned(conn, table_name, schema):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = %s AND n.nspname = %s", (table_name, schema))
            result = cur.fetchone()
            return result[0] == 'p' if result else False
    except Exception as e:
        print_message(f"Error checking if table {schema}.{table_name} is partitioned: {e}", "ERROR")
        return False

def get_dependent_views(conn, table_name, schema):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT dependent_ns.nspname AS view_schema, dependent_view.relname AS view_name, pg_get_viewdef(dependent_view.oid) AS view_definition
                FROM pg_depend AS d JOIN pg_rewrite AS r ON r.oid = d.objid JOIN pg_class AS dependent_view ON dependent_view.oid = r.ev_class
                JOIN pg_namespace AS dependent_ns ON dependent_ns.oid = dependent_view.relnamespace JOIN pg_class AS source_table ON source_table.oid = d.refobjid
                JOIN pg_namespace AS source_ns ON source_ns.oid = source_table.relnamespace
                WHERE d.refclassid = 'pg_class'::REGCLASS AND d.classid = 'pg_rewrite'::REGCLASS AND dependent_view.relkind = 'v' AND source_table.relname = %s AND source_ns.nspname = %s;
            """, (table_name, schema))
            return cur.fetchall()
    except Exception as e:
        print_message(f"Error finding dependent views for {schema}.{table_name}: {e}", "ERROR")
        return []

def get_triggers(conn, table_name, schema):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT n.nspname AS schema_name, tg.tgname AS trigger_name, pg_get_triggerdef(tg.oid) as trigger_definition FROM pg_trigger tg JOIN pg_class t ON tg.tgrelid = t.oid JOIN pg_namespace n ON t.relnamespace = n.oid WHERE t.relname = %s AND n.nspname = %s", (table_name, schema))
            return cur.fetchall()
    except Exception as e:
        print_message(f"Error finding triggers for {schema}.{table_name}: {e}", "ERROR")
        return []

def get_foreign_keys_on_table(conn, table_name, schema):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT pc.conname AS constraint_name, pg_get_constraintdef(pc.oid) AS constraint_definition,
                       ARRAY(SELECT att.attname FROM unnest(pc.conkey) WITH ORDINALITY k(attnum, ord)
                             JOIN pg_attribute att ON att.attrelid = pc.conrelid AND att.attnum = k.attnum ORDER BY k.ord) AS fk_columns
                FROM pg_constraint pc JOIN pg_namespace n ON n.oid = pc.connamespace JOIN pg_class cl ON cl.oid = pc.conrelid
                WHERE pc.contype = 'f' AND cl.relname = %s AND n.nspname = %s;
            """, (table_name, schema))
            return [{'name': r[0], 'definition': r[1], 'columns': list(r[2])} for r in cur.fetchall()]
    except Exception as e: print_message(f"Error retrieving FKs defined ON {schema}.{table_name}: {e}", "ERROR"); return []


def process_related_tables_for_partitioning(conn, source_table_name, source_schema, partition_field, dry_run=False):
    """
    Main orchestration function to process all related tables for partitioning.
    
    Args:
        conn: Database connection
        source_table_name: Name of the source table being partitioned
        source_schema: Schema of the source table
        partition_field: Name of the partition field
        dry_run: If True, only simulate operations
        
    Returns:
        Dictionary with processing results:
        {
            'success': True/False,
            'total_related_tables': int,
            'processed_tables': int,
            'failed_tables': int,
            'total_records_updated': int,
            'details': [...]  # List of per-table results
        }
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
        print_message(f"\n--- Processing Related Tables for {source_schema}.{source_table_name} ---", "SUBHEADER")

        # Step 1: Get partition field type information
        try:
            field_type_info = get_partition_field_type(conn, source_table_name, source_schema, partition_field)
        except Exception as e:
            print_message(f"Failed to get partition field type: {e}", "ERROR")
            result['success'] = False
            return result

        # Step 2: Discover related tables
        related_tables = get_related_tables(conn, source_table_name, source_schema)
        result['total_related_tables'] = len(related_tables)

        if not related_tables:
            print_message("No related tables found. Skipping related table processing.", "INFO")
            return result

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
                print_message(f"\n[{i}/{len(related_tables)}] Processing {table_schema}.{table_name}...", "STEP")

                # Step 3a: Add partition field to related table
                if add_partition_field_to_related_table(conn, related_table_info, partition_field, field_type_info, dry_run):
                    print_message(f"  ✓ Field addition successful for {table_schema}.{table_name}", "STEP_INFO")

                    # Step 3b: Populate the field with data
                    records_updated = populate_partition_field_in_related_table(
                        conn, source_table_name, source_schema, related_table_info, partition_field, dry_run
                    )

                    if records_updated >= 0:
                        table_detail['records_updated'] = records_updated
                        result['total_records_updated'] += records_updated
                        print_message(f"  ✓ Data population successful for {table_schema}.{table_name}", "STEP_INFO")

                        # Step 3c: Validate population (only in live mode)
                        if not dry_run:
                            validation_result = validate_partition_field_population(conn, related_table_info, partition_field)
                            table_detail['validation_result'] = validation_result

                            if validation_result['success']:
                                print_message(f"  ✓ Validation successful for {table_schema}.{table_name}", "STEP_INFO")
                                table_detail['success'] = True
                                result['processed_tables'] += 1
                            else:
                                print_message(f"  ⚠ Validation failed for {table_schema}.{table_name}: {validation_result['message']}", "WARNING")
                                table_detail['error_message'] = f"Validation failed: {validation_result['message']}"
                                result['failed_tables'] += 1
                        else:
                            # In dry run mode, consider it successful if we got this far
                            table_detail['success'] = True
                            result['processed_tables'] += 1
                            print_message(f"  ✓ [Dry Run] Processing simulation successful for {table_schema}.{table_name}", "STEP_INFO")
                    else:
                        table_detail['error_message'] = "Data population failed"
                        result['failed_tables'] += 1
                        print_message(f"  ❌ Data population failed for {table_schema}.{table_name}", "FAILURE")
                else:
                    table_detail['error_message'] = "Field addition failed"
                    result['failed_tables'] += 1
                    print_message(f"  ❌ Field addition failed for {table_schema}.{table_name}", "FAILURE")

            except Exception as e:
                table_detail['error_message'] = str(e)
                result['failed_tables'] += 1
                print_message(f"  ❌ Error processing {table_schema}.{table_name}: {e}", "ERROR")

            result['details'].append(table_detail)

        # Final status
        if result['failed_tables'] > 0:
            result['success'] = False
            print_message(f"\n⚠ Related table processing completed with {result['failed_tables']} failure(s)", "WARNING")
        else:
            print_message(f"\n✓ All related tables processed successfully", "SUCCESS")

        print_message(f"Summary: {result['processed_tables']} successful, {result['failed_tables']} failed, {result['total_records_updated']} total records updated", "INFO")

        return result

    except Exception as e:
        print_message(f"Fatal error in related table processing: {e}", "ERROR")
        result['success'] = False
        result['details'].append({
            'error_message': f"Fatal error: {e}",
            'success': False
        })
        return result

def validate_partition_field_population(conn, related_table_info, partition_field):
    """
    Validate that partition field population was successful.
    
    Args:
        conn: Database connection
        related_table_info: Dictionary with related table details
        partition_field: Name of the partition field
        
    Returns:
        Dictionary with validation results:
        {
            'success': True/False,
            'total_records': int,
            'null_records': int,
            'populated_records': int,
            'message': str
        }
    """
    try:
        table_schema = related_table_info['table_schema']
        table_name = related_table_info['table_name']
        new_field_name = f"etarc_{partition_field}"

        print_message(f"Validating {new_field_name} population in {table_schema}.{table_name}...", "STEP_INFO")

        with conn.cursor() as cur:
            # Count total records
            cur.execute(
                sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                    sql.Identifier(table_schema),
                    sql.Identifier(table_name)
                )
            )
            total_records = cur.fetchone()[0]

            # Count NULL records in the partition field
            cur.execute(
                sql.SQL("SELECT COUNT(*) FROM {}.{} WHERE {} IS NULL").format(
                    sql.Identifier(table_schema),
                    sql.Identifier(table_name),
                    sql.Identifier(new_field_name)
                )
            )
            null_records = cur.fetchone()[0]

            populated_records = total_records - null_records

            if null_records == 0:
                success = True
                message = f"All {total_records} record(s) successfully populated"
                print_message(f"✓ {message} in {table_schema}.{table_name}", "SUCCESS")
            else:
                success = False
                message = f"{null_records} record(s) remain NULL out of {total_records} total"
                print_message(f"⚠ {message} in {table_schema}.{table_name}", "WARNING")

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
        return {
            'success': False,
            'total_records': 0,
            'null_records': 0,
            'populated_records': 0,
            'message': error_msg
        }
    except Exception as e:
        error_msg = f"Error validating {table_schema}.{table_name}: {e}"
        print_message(error_msg, "ERROR")
        return {
            'success': False,
            'total_records': 0,
            'null_records': 0,
            'populated_records': 0,
            'message': error_msg
        }

def populate_partition_field_in_related_table(conn, source_table, source_schema, related_table_info, partition_field, dry_run=False):
    """
    Populate the partition field in related tables with data from source table.
    
    Args:
        conn: Database connection
        source_table: Name of the source partitioned table
        source_schema: Schema of the source table
        related_table_info: Dictionary with related table details
        partition_field: Name of the partition field
        dry_run: If True, only simulate the operation
        
    Returns:
        int: Number of records updated, -1 if error
    """
    try:
        table_schema = related_table_info['table_schema']
        table_name = related_table_info['table_name']
        new_field_name = f"etarc_{partition_field}"
        fk_columns = related_table_info['fk_columns']
        referenced_columns = related_table_info['referenced_columns']

        print_message(f"Populating {new_field_name} in {table_schema}.{table_name}...", "STEP_INFO")

        # Build the JOIN conditions for the UPDATE statement
        join_conditions = []
        for fk_col, ref_col in zip(fk_columns, referenced_columns):
            join_conditions.append(
                sql.SQL("r.{} = s.{}").format(
                    sql.Identifier(fk_col),
                    sql.Identifier(ref_col)
                )
            )

        join_condition = sql.SQL(" AND ").join(join_conditions)

        # Build UPDATE statement with JOIN
        update_stmt = sql.SQL("""
            UPDATE {}.{} AS r 
            SET {} = s.{}
            FROM {}.{} AS s
            WHERE {}
        """).format(
            sql.Identifier(table_schema),
            sql.Identifier(table_name),
            sql.Identifier(new_field_name),
            sql.Identifier(partition_field),
            sql.Identifier(source_schema),
            sql.Identifier(source_table),
            join_condition
        )

        if dry_run:
            print_message(f"[Dry Run] Would execute: {update_stmt.as_string(conn)}", "STEP_INFO")

            # In dry run, count how many records would be updated
            count_stmt = sql.SQL("""
                SELECT COUNT(*)
                FROM {}.{} AS r 
                JOIN {}.{} AS s ON {}
                WHERE r.{} IS NULL
            """).format(
                sql.Identifier(table_schema),
                sql.Identifier(table_name),
                sql.Identifier(source_schema),
                sql.Identifier(source_table),
                join_condition,
                sql.Identifier(new_field_name)
            )

            with conn.cursor() as cur:
                cur.execute(count_stmt)
                count = cur.fetchone()[0]
                print_message(f"[Dry Run] Would update {count} record(s) in {table_schema}.{table_name}", "STEP_INFO")
                return count
        else:
            with conn.cursor() as cur:
                cur.execute(update_stmt)
                updated_count = cur.rowcount
                print_message(f"Updated {updated_count} record(s) in {table_schema}.{table_name}", "SUCCESS")
                return updated_count

    except psycopg2.Error as db_err:
        print_message(f"Database error populating field in {table_schema}.{table_name}: {db_err}", "ERROR")
        return -1
    except Exception as e:
        print_message(f"Error populating field in {table_schema}.{table_name}: {e}", "ERROR")
        return -1

def add_partition_field_to_related_table(conn, related_table_info, partition_field, field_type_info, dry_run=False):
    """
    Add the etarc_ partition field to a related table.
    
    Args:
        conn: Database connection
        related_table_info: Dictionary with related table details
        partition_field: Name of the original partition field
        field_type_info: Dictionary with field type information from get_partition_field_type
        dry_run: If True, only simulate the operation
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        table_schema = related_table_info['table_schema']
        table_name = related_table_info['table_name']
        new_field_name = f"etarc_{partition_field}"

        print_message(f"Adding field {new_field_name} to {table_schema}.{table_name}...", "STEP_INFO")

        # Check if field already exists
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = %s AND table_name = %s AND column_name = %s
            """, (table_schema, table_name, new_field_name))

            if cur.fetchone():
                print_message(f"Field {new_field_name} already exists in {table_schema}.{table_name}, skipping...", "WARNING")
                return True

        # Build the data type string
        data_type = field_type_info['data_type']

        # Handle data types that need precision/scale
        if field_type_info['character_maximum_length']:
            data_type += f"({field_type_info['character_maximum_length']})"
        elif field_type_info['numeric_precision'] and field_type_info['numeric_scale'] is not None:
            data_type += f"({field_type_info['numeric_precision']},{field_type_info['numeric_scale']})"
        elif field_type_info['datetime_precision']:
            data_type += f"({field_type_info['datetime_precision']})"

        # Create ALTER TABLE statement
        alter_stmt = sql.SQL("ALTER TABLE {}.{} ADD COLUMN {} {} NULL").format(
            sql.Identifier(table_schema),
            sql.Identifier(table_name),
            sql.Identifier(new_field_name),
            sql.SQL(data_type)
        )

        if dry_run:
            print_message(f"[Dry Run] Would execute: {alter_stmt.as_string(conn)}", "STEP_INFO")
        else:
            with conn.cursor() as cur:
                cur.execute(alter_stmt)
            print_message(f"Successfully added field {new_field_name} to {table_schema}.{table_name}", "SUCCESS")

        return True

    except psycopg2.Error as db_err:
        print_message(f"Database error adding field to {table_schema}.{table_name}: {db_err}", "ERROR")
        return False
    except Exception as e:
        print_message(f"Error adding field to {table_schema}.{table_name}: {e}", "ERROR")
        return False

def get_partition_field_type(conn, table_name, schema, field_name):
    """
    Get the exact data type of the partition field from the source table.
    
    Args:
        conn: Database connection
        table_name: Name of the source table
        schema: Schema of the source table  
        field_name: Name of the partition field
        
    Returns:
        Dictionary with type information:
        {
            'data_type': 'timestamp without time zone',
            'is_nullable': 'NO',
            'column_default': None,
            'character_maximum_length': None,
            'numeric_precision': None,
            'numeric_scale': None
        }
    """
    try:
        print_message(f"Getting data type for field {field_name} in {schema}.{table_name}...", "CONFIG_LOAD")

        with conn.cursor() as cur:
            query = """
                SELECT 
                    data_type,
                    is_nullable,
                    column_default,
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale,
                    datetime_precision
                FROM information_schema.columns 
                WHERE table_schema = %s 
                    AND table_name = %s 
                    AND column_name = %s
            """

            cur.execute(query, (schema, table_name, field_name))
            result = cur.fetchone()

            if not result:
                raise Exception(f"Partition field '{field_name}' not found in {schema}.{table_name}")

            field_type_info = {
                'data_type': result[0],
                'is_nullable': result[1],
                'column_default': result[2],
                'character_maximum_length': result[3],
                'numeric_precision': result[4],
                'numeric_scale': result[5],
                'datetime_precision': result[6]
            }

            print_message(
                f"Partition field type: {field_type_info['data_type']} "
                f"(nullable: {field_type_info['is_nullable']})",
                "CONFIG_LOAD"
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
    Discover all tables that have foreign key relationships pointing to the source table.
    
    Args:
        conn: Database connection
        table_name: Name of the source table being partitioned
        schema: Schema of the source table
        
    Returns:
        List of dictionaries with related table information:
        [
            {
                'table_name': 'related_table_name',
                'table_schema': 'related_table_schema', 
                'constraint_name': 'fk_constraint_name',
                'fk_columns': ['col1', 'col2'],  # columns in related table
                'referenced_columns': ['ref_col1', 'ref_col2']  # columns in source table
            }
        ]
    """
    related_tables = []

    try:
        print_message(f"Discovering tables related to {schema}.{table_name}...", "CONFIG_LOAD")

        with conn.cursor() as cur:
            # Query to find all foreign keys that reference the source table
            query = """
                SELECT 
                    tc.table_schema AS referencing_schema,
                    tc.table_name AS referencing_table,
                    tc.constraint_name,
                    ARRAY_AGG(kcu.column_name ORDER BY kcu.ordinal_position) AS fk_columns,
                    ARRAY_AGG(ccu.column_name ORDER BY kcu.ordinal_position) AS referenced_columns
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name 
                    AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage ccu
                    ON tc.constraint_name = ccu.constraint_name
                WHERE tc.constraint_type = 'FOREIGN KEY'
                    AND ccu.table_schema = %s 
                    AND ccu.table_name = %s
                GROUP BY tc.table_schema, tc.table_name, tc.constraint_name
                ORDER BY tc.table_schema, tc.table_name
            """

            cur.execute(query, (schema, table_name))
            results = cur.fetchall()

            for row in results:
                related_table_info = {
                    'table_schema': row[0],
                    'table_name': row[1],
                    'constraint_name': row[2],
                    'fk_columns': list(row[3]),
                    'referenced_columns': list(row[4])
                }
                related_tables.append(related_table_info)

                print_message(
                    f"Found related table: {row[0]}.{row[1]} "
                    f"(FK: {row[2]}, Columns: {list(row[3])} -> {list(row[4])})",
                    "CONFIG_LOAD"
                )

        if not related_tables:
            print_message(f"No related tables found for {schema}.{table_name}", "INFO")
        else:
            print_message(f"Found {len(related_tables)} related table(s) for {schema}.{table_name}", "SUCCESS")

        return related_tables

    except psycopg2.Error as db_err:
        print_message(f"Database error while finding related tables for {schema}.{table_name}: {db_err}", "ERROR")
        return []
    except Exception as e:
        print_message(f"Error finding related tables for {schema}.{table_name}: {e}", "ERROR")
        return []

def get_foreign_keys_referencing_table(conn, referenced_table_name, referenced_schema_name):
    fks_referencing = []
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT con.conname AS constraint_name,
                       cl_ref.relname AS referencing_table_name,
                       ns_ref.nspname AS referencing_table_schema,
                       pg_get_constraintdef(con.oid) AS constraint_definition,
                       ARRAY(SELECT att.attname
                             FROM unnest(con.conkey) WITH ORDINALITY k(attnum, ord)
                             JOIN pg_attribute att ON att.attrelid = con.conrelid AND att.attnum = k.attnum
                             ORDER BY k.ord
                       ) AS fk_columns_on_referencing_table
                FROM pg_constraint con
                JOIN pg_class cl_ref ON cl_ref.oid = con.conrelid
                JOIN pg_namespace ns_ref ON ns_ref.oid = cl_ref.relnamespace
                WHERE con.confrelid = (SELECT oid FROM pg_class WHERE relname = %s AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = %s))
                  AND con.contype = 'f';
            """, (referenced_table_name, referenced_schema_name))
            for row in cur.fetchall():
                fks_referencing.append({'name': row[0], 'referencing_table': row[1], 'referencing_schema': row[2],
                                        'definition': row[3], 'columns': list(row[4])})
        return fks_referencing
    except Exception as e: print_message(f"Error retrieving FKs REFERENCING {referenced_schema_name}.{referenced_table_name}: {e}", "ERROR"); return []

def get_indexes_for_table(conn, schema, table_name_idx):
    indexes = {}
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT i.relname AS indexname,
                       ARRAY_AGG(a.attname ORDER BY array_position(ix.indkey::int[], a.attnum::int)) AS columnnames
                FROM pg_class t
                JOIN pg_index ix ON t.oid = ix.indrelid
                JOIN pg_class i ON i.oid = ix.indexrelid
                JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE n.nspname = %s AND t.relname = %s AND ix.indisprimary = FALSE
                GROUP BY i.relname;
            """, (schema, table_name_idx))
            for row in cur.fetchall(): indexes[row[0]] = list(row[1])
    except Exception as e: print_message(f"Error retrieving indexes for {schema}.{table_name_idx}: {e}", "ERROR")
    return indexes

def drop_triggers(conn, table_name, schema, dry_run=False):
    dropped_triggers_info = []
    try:
        triggers = get_triggers(conn, table_name, schema)
        for trigger_schema, trigger_name, trigger_def in triggers:
            if trigger_name.startswith('RI_ConstraintTrigger') or trigger_name.startswith('zombodb_'):
                print_message(f"Skipping system/special trigger {trigger_schema}.{trigger_name}", "INFO")
                continue
            sql_stmt_str = sql.SQL("DROP TRIGGER {} ON {}.{}").format(
                sql.Identifier(trigger_name), sql.Identifier(schema), sql.Identifier(table_name)
            ).as_string(conn)
            if dry_run: print_message(f"[Dry Run] Would execute: {sql_stmt_str}", "INFO")
            else:
                with conn.cursor() as cur: cur.execute(sql_stmt_str)
                print_message(f"Dropped trigger {trigger_schema}.{trigger_name} on {schema}.{table_name}", "SUCCESS")
            dropped_triggers_info.append({'name': trigger_name, 'schema': trigger_schema, 'definition': trigger_def})
        return dropped_triggers_info
    except Exception as e:
        print_message(f"Error dropping triggers for {schema}.{table_name}: {e}", "ERROR")
        raise

def validate_partition_field(conn, table_to_validate, schema_of_table, field_to_validate):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT data_type FROM information_schema.columns WHERE table_schema = %s AND table_name = %s AND column_name = %s", (schema_of_table, table_to_validate, field_to_validate))
            result = cur.fetchone()
            if not result: raise Exception(f"Partition field '{field_to_validate}' not found in {schema_of_table}.{table_to_validate}")
            data_type = result[0].lower()
            if not ('timestamp' in data_type or 'date' == data_type): raise Exception(f"Partition field '{field_to_validate}' must be timestamp or date, found {data_type}")
            print_message(f"Validation successful: Partition field '{field_to_validate}' in {schema_of_table}.{table_to_validate} is type {data_type}.", "SUCCESS")
    except Exception as e:
        print_message(f"Error validating partition field for {schema_of_table}.{table_to_validate}: {e}", "ERROR")
        raise

def get_primary_key_info(conn, table_name_pk, schema_pk):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT tc.constraint_name, array_agg(kcu.column_name::text ORDER BY kcu.ordinal_position) AS columns FROM information_schema.table_constraints tc JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema WHERE tc.table_schema = %s AND tc.table_name = %s AND tc.constraint_type = 'PRIMARY KEY' GROUP BY tc.constraint_name", (schema_pk, table_name_pk))
            result = cur.fetchone()
            return {'name': result[0], 'columns': list(result[1])} if result else {'name': None, 'columns': []}
    except Exception as e:
        print_message(f"Error retrieving PK info for {schema_pk}.{table_name_pk}: {e}", "ERROR")
        return {'name': None, 'columns': []}

def get_year_range(conn, table_name_for_range, schema_for_range, partition_by_field):
    try:
        with conn.cursor() as cur:
            query = sql.SQL("SELECT EXTRACT(YEAR FROM MIN({}))::int, EXTRACT(YEAR FROM MAX({}))::int FROM {}.{}").format(
                sql.Identifier(partition_by_field), sql.Identifier(partition_by_field),
                sql.Identifier(schema_for_range), sql.Identifier(table_name_for_range)
            )
            cur.execute(query)
            min_year, max_year = cur.fetchone()
            current_year = datetime.now().year
            min_year = min_year if min_year is not None else current_year
            max_year = max_year if max_year is not None else current_year
            if min_year > max_year: max_year = min_year
            return min_year, max_year
    except Exception as e:
        print_message(f"Error determining year range for {schema_for_range}.{table_name_for_range} (field {partition_by_field}): {e}. Using default range.", "WARNING")
        current_year = datetime.now().year
        return current_year, current_year + 1

# --- Main Execution Function ---
def execute_partition_steps(conn, table_to_partition_name, partition_by_field, dry_run=False):
    new_partitioned_table_name = table_to_partition_name
    tmp_table_name = f"{table_to_partition_name}_tmp"
    partitions_schema_name = "partitions"
    dropped_referencing_fks_info = []
    related_tables_result = None

    try:
        actual_schema = table_exists(conn, table_to_partition_name)
        if not actual_schema:
            raise Exception(f"Table '{table_to_partition_name}' does not exist.")

        print_message(f"Processing table {Style.BOLD}{actual_schema}.{table_to_partition_name}{Style.RESET} for partitioning by field '{Style.UNDERLINE}{partition_by_field}{Style.RESET}'.", "SUBHEADER")

        if is_table_partitioned(conn, new_partitioned_table_name, actual_schema):
            print_message(f"Table {actual_schema}.{new_partitioned_table_name} is already a partitioned table. Skipping.", "INFO")
            return

        print_message(f"\n--- Validating and Preparing Original Table: {actual_schema}.{table_to_partition_name} ---", "SUBHEADER")
        get_table_schema(conn, table_to_partition_name, actual_schema)
        validate_partition_field(conn, table_to_partition_name, actual_schema, partition_by_field)

        pk_info = get_primary_key_info(conn, table_to_partition_name, actual_schema)
        original_pk_name = pk_info['name']
        original_pk_columns = pk_info['columns']
        if original_pk_name: print_message(f"Original Primary Key found: Name='{original_pk_name}', Columns={original_pk_columns}", "INFO")
        else: print_message(f"No Primary Key found on {actual_schema}.{table_to_partition_name}.", "WARNING")

        print_message(f"\n[Step 1] Dropping dependent views on {actual_schema}.{table_to_partition_name}...", "STEP")
        dependent_views = get_dependent_views(conn, table_to_partition_name, actual_schema)
        if not dependent_views: print_message("No dependent views found.", "STEP_INFO")
        for vs, vn, _ in dependent_views:
            s = sql.SQL("DROP VIEW IF EXISTS {}.{}").format(sql.Identifier(vs), sql.Identifier(vn))
            if dry_run: print_message(f"[Dry Run] Would execute: {s.as_string(conn)}", "STEP_INFO")
            else: conn.cursor().execute(s); print_message(f"Dropped view {vs}.{vn}", "STEP_INFO")

        print_message(f"\n[Step 2] Dropping Foreign Keys from OTHER tables that reference {actual_schema}.{table_to_partition_name} (and their supporting indexes)...", "STEP")
        fks_referencing = get_foreign_keys_referencing_table(conn, table_to_partition_name, actual_schema)
        if not fks_referencing: print_message("No external FKs found referencing this table.", "STEP_INFO")
        for fk in fks_referencing:
            fk_referencing_table_schema = fk['referencing_schema']
            fk_referencing_table_name = fk['referencing_table']
            fk_constraint_name = fk['name']
            fk_columns_on_ref_table = fk['columns']
            s_fk = sql.SQL("ALTER TABLE {}.{} DROP CONSTRAINT IF EXISTS {}").format(
                sql.Identifier(fk_referencing_table_schema), sql.Identifier(fk_referencing_table_name), sql.Identifier(fk_constraint_name))
            if dry_run: print_message(f"[Dry Run] External FK Drop: {s_fk.as_string(conn)}", "STEP_INFO")
            else: conn.cursor().execute(s_fk); print_message(f"Dropped external FK '{fk_constraint_name}' from {fk_referencing_table_schema}.{fk_referencing_table_name}", "STEP_INFO")
            dropped_referencing_fks_info.append(fk)
            indexes_on_ref_table = get_indexes_for_table(conn, fk_referencing_table_schema, fk_referencing_table_name)
            for index_name, index_cols in indexes_on_ref_table.items():
                if sorted(index_cols) == sorted(fk_columns_on_ref_table):
                    s_idx = sql.SQL("DROP INDEX IF EXISTS {}.{}").format(sql.Identifier(fk_referencing_table_schema), sql.Identifier(index_name))
                    if dry_run: print_message(f"[Dry Run] External FK Index Drop: {s_idx.as_string(conn)}", "STEP_INFO")
                    else: conn.cursor().execute(s_idx); print_message(f"Dropped index '{index_name}' on {fk_referencing_table_schema}.{fk_referencing_table_name} (was for FK {fk_constraint_name})", "STEP_INFO")
                    break

        print_message(f"\n[Step 3] Dropping FKs defined ON {actual_schema}.{table_to_partition_name} and related indexes...", "STEP")
        fks_on_table = get_foreign_keys_on_table(conn, table_to_partition_name, actual_schema)
        if not fks_on_table: print_message("No FKs found defined on this table.", "STEP_INFO")
        for fk in fks_on_table:
            s_fk = sql.SQL("ALTER TABLE {}.{} DROP CONSTRAINT IF EXISTS {}").format(
                sql.Identifier(actual_schema), sql.Identifier(table_to_partition_name), sql.Identifier(fk['name']))
            if dry_run: print_message(f"[Dry Run] Own FK Drop: {s_fk.as_string(conn)}", "STEP_INFO")
            else: conn.cursor().execute(s_fk); print_message(f"Dropped own FK '{fk['name']}'", "STEP_INFO")
            indexes_on_orig_table = get_indexes_for_table(conn, actual_schema, table_to_partition_name)
            for idx_name, idx_cols in indexes_on_orig_table.items():
                if sorted(idx_cols) == sorted(fk['columns']):
                    s_idx = sql.SQL("DROP INDEX IF EXISTS {}.{}").format(sql.Identifier(actual_schema), sql.Identifier(idx_name))
                    if dry_run: print_message(f"[Dry Run] Own FK Index Drop: {s_idx.as_string(conn)}", "STEP_INFO")
                    else: conn.cursor().execute(s_idx); print_message(f"Dropped index '{idx_name}' on {actual_schema}.{table_to_partition_name}", "STEP_INFO")
                    break

        print_message(f"\n[Step 4] Dropping user-defined triggers on {actual_schema}.{table_to_partition_name}...", "STEP")
        dropped_tr = drop_triggers(conn, table_to_partition_name, actual_schema, dry_run)
        if not dropped_tr and not dry_run : print_message("No user-defined triggers found to drop.", "STEP_INFO")
        elif dry_run and not get_triggers(conn, table_to_partition_name, actual_schema): print_message("[Dry Run] No user-defined triggers would be dropped.", "STEP_INFO")


        if original_pk_name:
            print_message(f"\n[Step 5] Dropping PK '{original_pk_name}'...", "STEP")
            s = sql.SQL("ALTER TABLE {}.{} DROP CONSTRAINT IF EXISTS {}").format(
                sql.Identifier(actual_schema), sql.Identifier(table_to_partition_name), sql.Identifier(original_pk_name))
            if dry_run: print_message(f"[Dry Run] {s.as_string(conn)}", "STEP_INFO")
            else: conn.cursor().execute(s); print_message(f"Dropped PK {original_pk_name}", "STEP_INFO")
        else:
            print_message(f"\n[Step 5] Skipping PK drop as no PK was found.", "STEP_INFO")


        print_message(f"\n[Step 6] Renaming {actual_schema}.{table_to_partition_name} to {actual_schema}.{tmp_table_name}...", "STEP")
        if table_exists(conn, tmp_table_name, actual_schema): raise Exception(f"Temp table {actual_schema}.{tmp_table_name} already exists.")
        stmt_rename = sql.SQL("ALTER TABLE {}.{} RENAME TO {}").format(sql.Identifier(actual_schema), sql.Identifier(table_to_partition_name), sql.Identifier(tmp_table_name))
        if dry_run: print_message(f"[Dry Run] {stmt_rename.as_string(conn)}", "STEP_INFO")
        else: conn.cursor().execute(stmt_rename); print_message(f"Renamed to {actual_schema}.{tmp_table_name}", "STEP_INFO")

        print_message(f"\n--- Creating new partitioned table {actual_schema}.{new_partitioned_table_name} and migrating data ---", "SUBHEADER")

        print_message(f"\n[Step 7] Creating new partitioned table {actual_schema}.{new_partitioned_table_name}...", "STEP")
        stmt_create_part = sql.SQL("CREATE TABLE {}.{} (LIKE {}.{} INCLUDING DEFAULTS INCLUDING STORAGE INCLUDING COMMENTS) PARTITION BY RANGE ({})").format(
            sql.Identifier(actual_schema), sql.Identifier(new_partitioned_table_name),
            sql.Identifier(actual_schema), sql.Identifier(tmp_table_name), sql.Identifier(partition_by_field))
        if dry_run: print_message(f"[Dry Run] {stmt_create_part.as_string(conn)}", "STEP_INFO")
        else: conn.cursor().execute(stmt_create_part); print_message(f"Created partitioned table {actual_schema}.{new_partitioned_table_name}", "STEP_INFO")

        if original_pk_name and original_pk_columns:
            print_message(f"\n[Step 8] Recreating PK on {actual_schema}.{new_partitioned_table_name}...", "STEP")
            new_pk_cols = list(original_pk_columns)
            if partition_by_field not in new_pk_cols: new_pk_cols.append(partition_by_field)
            print_message(f"  Using original PK name: {original_pk_name}, New PK columns: {new_pk_cols}", "STEP_INFO")
            if partition_by_field not in new_pk_cols: raise Exception(f"FATAL: Partition field '{partition_by_field}' MUST be part of new PK columns {new_pk_cols}")
            stmt_add_pk = sql.SQL("ALTER TABLE {}.{} ADD CONSTRAINT {} PRIMARY KEY ({})").format(
                sql.Identifier(actual_schema), sql.Identifier(new_partitioned_table_name),
                sql.Identifier(original_pk_name), sql.SQL(', ').join(map(sql.Identifier, new_pk_cols)))
            if dry_run: print_message(f"[Dry Run] {stmt_add_pk.as_string(conn)}", "STEP_INFO")
            else: conn.cursor().execute(stmt_add_pk); print_message(f"Created PK {original_pk_name} with columns {new_pk_cols}", "STEP_INFO")
        else: print_message(f"\n[Step 8] No original PK to recreate.", "STEP_INFO")

        print_message(f"\n[Step 9] Ensuring schema '{partitions_schema_name}' exists...", "STEP")
        stmt_create_schema = sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(partitions_schema_name))
        if dry_run: print_message(f"[Dry Run] {stmt_create_schema.as_string(conn)}", "STEP_INFO")
        else: conn.cursor().execute(stmt_create_schema); print_message(f"Schema '{partitions_schema_name}' ensured.", "STEP_INFO")

        print_message(f"\n[Step 10] Determining year range for partitions from {actual_schema}.{tmp_table_name}...", "STEP")
        start_year, end_year = get_year_range(conn, tmp_table_name, actual_schema, partition_by_field)
        print_message(f"Data range found: {start_year} to {end_year}.", "STEP_INFO")

        print_message(f"\n[Step 11] Creating yearly partitions for {actual_schema}.{new_partitioned_table_name}...", "STEP")
        if start_year > end_year :
            print_message(f"Warning: start_year ({start_year}) is greater than end_year ({end_year}). No partitions will be created based on data range. Creating a default partition for current year.", "WARNING")
            current_year_for_default_part = datetime.now().year
            start_year = current_year_for_default_part
            end_year = current_year_for_default_part

        for year_val in range(start_year, end_year + 2):
            part_table_name = f"{new_partitioned_table_name}_y{year_val}"
            from_date_str = f"{year_val}-01-01"
            to_date_str = f"{year_val + 1}-01-01"
            stmt_create_actual_part = sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} PARTITION OF {}.{} FOR VALUES FROM (%s) TO (%s)").format(
                sql.Identifier(partitions_schema_name), sql.Identifier(part_table_name),
                sql.Identifier(actual_schema), sql.Identifier(new_partitioned_table_name))
            if dry_run: print_message(f"[Dry Run] Would execute: {stmt_create_actual_part.as_string(conn)} with ('{from_date_str}', '{to_date_str}')", "STEP_INFO")
            else:
                try:
                    conn.cursor().execute(stmt_create_actual_part, (from_date_str, to_date_str))
                    print_message(f"Ensured partition {partitions_schema_name}.{part_table_name} for year {year_val} ({from_date_str} to {to_date_str})", "STEP_INFO")
                except psycopg2.Error as e_part:
                    print_message(f"Warning: Could not create/ensure partition for year {year_val}: {e_part}", "WARNING")
                    conn.rollback()
                    conn.autocommit = False

        print_message(f"\n[Step 12] Copying data from {actual_schema}.{tmp_table_name} to {actual_schema}.{new_partitioned_table_name}...", "STEP")
        stmt_copy_data = sql.SQL("INSERT INTO {}.{} SELECT * FROM {}.{}").format(
            sql.Identifier(actual_schema), sql.Identifier(new_partitioned_table_name),
            sql.Identifier(actual_schema), sql.Identifier(tmp_table_name))
        if dry_run: print_message(f"[Dry Run] {stmt_copy_data.as_string(conn)}", "STEP_INFO")
        else:
            with conn.cursor() as cur:
                cur.execute(stmt_copy_data)
                print_message(f"Copied {cur.rowcount} rows to {actual_schema}.{new_partitioned_table_name}", "STEP_INFO")

        print_message(f"\n--- Data migration to {actual_schema}.{new_partitioned_table_name} complete. ---", "SUCCESS")

        if dropped_referencing_fks_info:
            print_message("\nINFO: Following external FKs were dropped and NOT recreated:", "IMPORTANT")
            for fk_info in dropped_referencing_fks_info: print_message(f"  - FK '{fk_info['name']}' on {fk_info['referencing_schema']}.{fk_info['referencing_table']}", "INFO")

        if not dry_run: conn.commit()
        else: conn.rollback()
        print_message(f"\n{'[Dry Run] Simulated' if dry_run else 'Successfully completed'} partitioning process for {actual_schema}.{new_partitioned_table_name}.", "SUCCESS" if not dry_run else "INFO")
        print_message(f"The original data remains in {actual_schema}.{tmp_table_name} for verification or manual rollback if needed.", "INFO")

        print_message(f"\n[Step 13] Dropping temporary table {actual_schema}.{tmp_table_name}...", "STEP")
        stmt_drop_tmp_table = sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
            sql.Identifier(actual_schema), sql.Identifier(tmp_table_name)
        )
        if dry_run:
            print_message(f"[Dry Run] Would execute: {stmt_drop_tmp_table.as_string(conn)}", "STEP_INFO")
        else:
            try:
                original_autocommit_state = conn.autocommit
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute(stmt_drop_tmp_table)
                print_message(f"Successfully dropped temporary table {actual_schema}.{tmp_table_name}.", "STEP_INFO")
            except Exception as e_drop_tmp:
                print_message(f"Error dropping temporary table {actual_schema}.{tmp_table_name}: {e_drop_tmp}", "ERROR")
                print_message(f"  You may need to drop it manually: {stmt_drop_tmp_table.as_string(conn)}", "INFO")
            finally:
                if not dry_run:
                    conn.autocommit = original_autocommit_state

        print_message(f"\n{Style.SUCCESS} Full partitioning process for {actual_schema}.{new_partitioned_table_name} {'simulated.' if dry_run else 'completed successfully.'}", "SUCCESS")

        print_message(f"\n[Step 14] Processing related tables for partition field propagation...", "STEP")
        try:
            related_tables_result = process_related_tables_for_partitioning(
                conn, table_to_partition_name, actual_schema, partition_by_field, dry_run
            )

            if related_tables_result['success']:
                print_message(f"Related tables processing completed successfully", "SUCCESS")
                print_message(f"  - Tables processed: {related_tables_result['processed_tables']}", "STEP_INFO")
                print_message(f"  - Records updated: {related_tables_result['total_records_updated']}", "STEP_INFO")
            else:
                print_message(f"Related tables processing completed with errors", "WARNING")
                print_message(f"  - Tables processed: {related_tables_result['processed_tables']}", "STEP_INFO")
                print_message(f"  - Tables failed: {related_tables_result['failed_tables']}", "STEP_INFO")
                print_message(f"  - Records updated: {related_tables_result['total_records_updated']}", "STEP_INFO")

        except Exception as e_related:
            print_message(f"Error in related tables processing: {e_related}", "ERROR")
            print_message("Main partitioning was successful, but related table processing failed", "WARNING")
            related_tables_result = {
                'success': False,
                'total_related_tables': 0,
                'processed_tables': 0,
                'failed_tables': 1,
                'total_records_updated': 0,
                'details': [{'error_message': str(e_related)}]
            }

        # Return the related tables result for summary reporting
        return related_tables_result

    except Exception as e:
        print_message(f"\n{Style.ERROR_ICON} ERROR processing table {table_to_partition_name if 'table_to_partition_name' in locals() else 'UNKNOWN'}: {e}", "ERROR")
        traceback.print_exc() # Esto imprimirá a stderr, lo cual está bien.
        if conn and not conn.closed:
            try: conn.rollback()
            except Exception as rb_e: print_message(f"Rollback error: {rb_e}", "ERROR")
        return None

def print_final_summary(summary_data):
    """Imprime el resumen final de la migración."""
    global _use_ansi_output

    section_line = "=" * 70
    print_message(f"\n\n{Style.SUMMARY} {section_line}", "HEADER")
    print_message(f"{' ' * 20}RESUMEN DE MIGRACIÓN DE TABLAS", "HEADER")
    print_message(section_line, "HEADER")

    # Encabezados de la tabla del resumen
    table_header_plain = f"{'Tabla':<35} | {'Estado':<10} | {'Rel. Tables':<10} | {'Mensaje'}"
    table_header_ansi = f"{Style.BOLD}{'Tabla':<35}{Style.RESET} | {Style.BOLD}{'Estado':<18}{Style.RESET} | {Style.BOLD}{'Rel. Tables':<10}{Style.RESET} | {Style.BOLD}{'Mensaje'}{Style.RESET}"

    if _use_ansi_output:
        print_message(table_header_ansi, "IMPORTANT")
        separator_len = len(table_header_plain) + 5 # Ajuste por emojis/colores en "Estado"
        print_message("-" * separator_len, "IMPORTANT")
    else:
        print_message(table_header_plain, "IMPORTANT")
        print_message("-" * len(table_header_plain), "IMPORTANT")

    success_count = 0
    failure_count = 0
    total_related_tables_processed = 0
    total_records_updated = 0

    for item in summary_data:
        table_name = item['table_name']
        status = item['status']
        message = item['message'][:80] + '...' if len(item['message']) > 80 else item['message']
        message = message.replace('\n', ' ') # Evitar saltos de línea en el mensaje del resumen

        # Information about related tables
        related_tables_info = item.get('related_tables_result', {})
        related_tables_str = "N/A"

        if related_tables_info:
            processed = related_tables_info.get('processed_tables', 0)
            failed = related_tables_info.get('failed_tables', 0)
            total_related_tables_processed += processed
            total_records_updated += related_tables_info.get('total_records_updated', 0)

            if failed > 0:
                related_tables_str = f"{processed}✓/{failed}✗"
            else:
                related_tables_str = f"{processed}✓" if processed > 0 else "0"

        if status == 'SUCCESS':
            status_str_plain = "SUCCESS"
            status_str_ansi = f"{Style.GREEN}{Style.SUCCESS} SUCCESS{Style.RESET}"
            success_count += 1
        else:
            status_str_plain = "FAILURE"
            status_str_ansi = f"{Style.RED}{Style.FAILURE} FAILURE{Style.RESET}"
            failure_count += 1

        if _use_ansi_output:
            print(f"  {table_name:<35} | {status_str_ansi:<{len(status_str_ansi) + (10 - len(status_str_plain))}} | {related_tables_str:<10} | {message}")
        else:
            print(f"  {table_name:<35} | {status_str_plain:<10} | {related_tables_str:<10} | {message}")

    if _use_ansi_output:
        print_message("-" * separator_len, "IMPORTANT")
    else:
        print_message("-" * len(table_header_plain), "IMPORTANT")

    print_message(f"Total Tablas Procesadas: {len(summary_data)}", "INFO")
    print_message(f"Exitosas: {success_count}", "SUCCESS")
    print_message(f"Fallidas: {failure_count}", "FAILURE" if failure_count > 0 else "INFO")

    # Related tables summary
    if total_related_tables_processed > 0:
        print_message(f"Total Tablas Relacionadas Procesadas: {total_related_tables_processed}", "INFO")
        print_message(f"Total Registros Actualizados en Tablas Relacionadas: {total_records_updated}", "INFO")

    print_message(section_line, "HEADER")


# --- Main Program Flow ---
def main(dry_run=False):
    properties_file = "config/Openbravo.properties"
    migration_summary = [] # Almacena el resumen de cada tabla

    try:
        print_message(f"Reading properties from: {properties_file}", "CONFIG_LOAD")
        properties = read_properties_file(properties_file)
        if not properties:
            print_message("Failed to read properties file or file is empty. Exiting.", "FAILURE")
            return

        if properties.get('bbdd.rdbms') != 'POSTGRE':
            raise ValueError(f"Unsupported RDBMS: {properties.get('bbdd.rdbms')}. Only POSTGRE is supported.")

        print_message("Parsing database connection parameters...", "CONFIG_LOAD")
        db_params = parse_db_params(properties)

        conn = None
        try:
            print_message(f"Connecting to PostgreSQL database: Host={db_params['host']}, Port={db_params['port']}, DBName={db_params['database']}", "DB_CONNECT")
            conn = psycopg2.connect(**db_params)
            print_message("Database connection successful.", "SUCCESS")

            session_config = properties.get('bbdd.sessionConfig')
            if session_config:
                with conn.cursor() as cur:
                    if dry_run: print_message(f"[Dry Run] Session config: {session_config}", "INFO")
                    else:
                        print_message(f"Applying session configuration: {session_config}", "DB_CONNECT")
                        cur.execute(session_config); conn.commit()
                        print_message("Session configuration applied.", "SUCCESS")

            # Obtener configuración de tablas desde la base de datos
            table_fields_config = get_tables_and_fields_from_database(conn)

            if not table_fields_config:
                print_message("No tables configured for processing from database configuration. Script will exit.", "INFO")
                return

            for table_name, fields in table_fields_config.items():
                if not fields:
                    print_message(f"\n--- Skipping table: {table_name} (No partition fields.) ---", "WARNING")
                    migration_summary.append({
                        'table_name': table_name,
                        'status': 'SKIPPED',
                        'message': 'No partition fields specified.',
                        'related_tables_result': None
                    })
                    continue

                partition_field = next(iter(fields))
                header_line = "="*70
                print_message(f"\n\n{header_line}", "TABLE_PROCESS")
                print_message(f"=== Processing table: {Style.BOLD}{table_name}{Style.RESET} (Partition field: {Style.UNDERLINE}{partition_field}{Style.RESET}) ===", "TABLE_PROCESS")
                print_message(header_line, "TABLE_PROCESS")
                try:
                    related_tables_result = execute_partition_steps(conn, table_name, partition_field, dry_run)
                    migration_summary.append({
                        'table_name': table_name,
                        'status': 'SUCCESS',
                        'message': '',
                        'related_tables_result': related_tables_result
                    })
                except Exception as e_table:
                    error_msg = str(e_table).replace('\n', ' ') # Limpiar mensaje para el resumen
                    migration_summary.append({
                        'table_name': table_name,
                        'status': 'FAILURE',
                        'message': error_msg[:200],  # Truncar mensaje largo
                        'related_tables_result': None
                    })
                    print_message(f"!!!!!!!! FAILED to process table {table_name}: {e_table} !!!!!!!!", "FAILURE")
                    print_message(f"Continuing with next table if any...", "INFO")
                    if conn.closed:
                        print_message("CRITICAL: Database connection was closed. Cannot continue.", "ERROR")
                        raise
                    else:
                        conn.rollback() # Asegurar rollback para la tabla fallida
                finally:
                    print_message(f"=== Finished processing for table: {table_name} ===", "TABLE_PROCESS")
                    print_message(f"{header_line}\n", "TABLE_PROCESS")

        finally:
            if conn and not conn.closed: conn.close(); print_message("DB connection closed.", "DB_CONNECT")

    except ValueError as ve:
        print_message(f"Configuration Error: {ve}", "ERROR")
    except psycopg2.Error as db_err:
        print_message(f"Database Error: {db_err}", "ERROR")
        traceback.print_exc()
    except Exception as e_main:
        print_message(f"MAIN SCRIPT ERROR: {e_main}", "ERROR")
        traceback.print_exc()
    finally:
        print_final_summary(migration_summary) # Imprimir resumen al final, incluso si hay errores mayores


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Automated script to migrate PostgreSQL tables to a range-partitioned structure based on a date/timestamp field. "
                    "This version modifies the Primary Key to include the partition field, drops dependencies without CASCADE, "
                    "migrates data to the new partitioned table, and then drops the temporary original table. "
                    "Table configuration is obtained from database tables ETARC_TABLE_CONFIG, AD_TABLE, and AD_COLUMN. "
                    "Other indexes (non-PK) are NOT copied from the original table."
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help="Simulate all DDL and DML operations without actually executing them against the database. "
             "Useful for verifying the script's actions."
    )
    parser.add_argument(
        '--plain-output',
        action='store_true',
        help="Disable colored/emoji output and show plain text only."
    )
    args = parser.parse_args()

    _use_ansi_output = not args.plain_output # Configurar la variable global

    script_version_message = f"Script version: PK Modified, No CASCADE, Data Migrated, Temp Table Dropped, No Index Copy, DB Config Source. Output: {'Enhanced' if _use_ansi_output else 'Plain'}"
    print_message(script_version_message, "SCRIPT_INFO")
    print_message(f"Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", "SCRIPT_INFO")

    if args.dry_run:
        print_message("\n*************************************", "RUN_MODE")
        print_message(f"{Style.DRY_RUN} *** DRY RUN MODE ENABLED     ***", "RUN_MODE")
        print_message("*** No actual changes will be made to the database. ***", "RUN_MODE")
        print_message("*************************************", "RUN_MODE")
    else:
        print_message("\n*************************************", "RUN_MODE")
        print_message(f"{Style.LIVE_RUN} *** LIVE RUN MODE ENABLED    ***", "RUN_MODE")
        print_message("*** Changes WILL be made to the database. Review carefully! ***", "RUN_MODE")
        print_message("*************************************", "RUN_MODE")
        try:
            user_confirmation = input("Are you sure you want to proceed with the LIVE RUN? (yes/no): ")
            if user_confirmation.lower() != 'yes':
                print_message("Live run cancelled by user. Exiting.", "WARNING")
                exit()
        except KeyboardInterrupt:
            print_message("\nLive run cancelled by user (Ctrl+C). Exiting.", "WARNING")
            exit()

    main(dry_run=args.dry_run)
