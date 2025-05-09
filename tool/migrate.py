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

def read_yaml_config(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f) or {}
            tables = config.get('tables', {})
            return {table: set(fields) for table, fields in tables.items()}
    except Exception as e:
        print_message(f"Error reading YAML configuration from {file_path}: {e}", "ERROR")
        return {}

def get_all_tables_and_fields(root_path):
    all_table_fields = defaultdict(set)
    if not root_path:
        print_message("Warning: 'source.path' is undefined. Cannot get tables and fields from YAML files.", "WARNING")
        return {}
    modules_path = Path(root_path) / 'modules'
    if not modules_path.exists() or not modules_path.is_dir():
        print_message(f"Warning: 'modules' directory not found at {modules_path}. Cannot read archiving.yaml files.", "WARNING")
        return {}
    for yaml_file in modules_path.rglob('archiving.yaml'):
        print_message(f"Reading archiving config from: {yaml_file}", "CONFIG_LOAD")
        table_fields = read_yaml_config(yaml_file)
        for table, fields in table_fields.items():
            all_table_fields[table].update(fields)
    return dict(all_table_fields)

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
def execute_partition_steps(conn, table_to_partition_name, partition_by_field, root_path=None, dry_run=False):
    new_partitioned_table_name = table_to_partition_name
    tmp_table_name = f"{table_to_partition_name}_tmp"
    partitions_schema_name = "partitions"
    dropped_referencing_fks_info = []

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

    except Exception as e:
        print_message(f"\n{Style.ERROR_ICON} ERROR processing table {table_to_partition_name if 'table_to_partition_name' in locals() else 'UNKNOWN'}: {e}", "ERROR")
        traceback.print_exc() # Esto imprimirá a stderr, lo cual está bien.
        if conn and not conn.closed:
            try: conn.rollback()
            except Exception as rb_e: print_message(f"Rollback error: {rb_e}", "ERROR")
        raise

def print_final_summary(summary_data):
    """Imprime el resumen final de la migración."""
    global _use_ansi_output

    section_line = "=" * 70
    print_message(f"\n\n{Style.SUMMARY} {section_line}", "HEADER")
    print_message(f"{' ' * 20}RESUMEN DE MIGRACIÓN DE TABLAS", "HEADER")
    print_message(section_line, "HEADER")

    # Encabezados de la tabla del resumen
    table_header_plain = f"{'Tabla':<35} | {'Estado':<10} | {'Mensaje'}"
    table_header_ansi = f"{Style.BOLD}{'Tabla':<35}{Style.RESET} | {Style.BOLD}{'Estado':<18}{Style.RESET} | {Style.BOLD}{'Mensaje'}{Style.RESET}"

    if _use_ansi_output:
        print_message(table_header_ansi, "IMPORTANT")
        # La longitud del separador debe considerar que los códigos ANSI no son visibles
        # Esta es una aproximación; puede necesitar ajustes finos.
        separator_len = len(table_header_plain) + 5 # Ajuste por emojis/colores en "Estado"
        print_message("-" * separator_len, "IMPORTANT")

    else:
        print_message(table_header_plain, "IMPORTANT")
        print_message("-" * len(table_header_plain), "IMPORTANT")


    success_count = 0
    failure_count = 0

    for item in summary_data:
        table_name = item['table_name']
        status = item['status']
        message = item['message'][:100] + '...' if len(item['message']) > 100 else item['message']
        message = message.replace('\n', ' ') # Evitar saltos de línea en el mensaje del resumen

        if status == 'SUCCESS':
            status_str_plain = "SUCCESS"
            status_str_ansi = f"{Style.GREEN}{Style.SUCCESS} SUCCESS{Style.RESET}"
            success_count += 1
        else:
            status_str_plain = "FAILURE"
            status_str_ansi = f"{Style.RED}{Style.FAILURE} FAILURE{Style.RESET}"
            failure_count += 1

        if _use_ansi_output:
            # El padding para status_str_ansi (<18) intenta compensar los caracteres no visibles de ANSI.
            print(f"  {table_name:<35} | {status_str_ansi:<{len(status_str_ansi) + (10 - len(status_str_plain))}} | {message}")
        else:
            print(f"  {table_name:<35} | {status_str_plain:<10} | {message}")

    if _use_ansi_output:
        print_message("-" * separator_len, "IMPORTANT")
    else:
        print_message("-" * len(table_header_plain), "IMPORTANT")

    print_message(f"Total Tablas Procesadas: {len(summary_data)}", "INFO")
    print_message(f"Exitosas: {success_count}", "SUCCESS")
    print_message(f"Fallidas: {failure_count}", "FAILURE" if failure_count > 0 else "INFO")
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

        source_path = properties.get('source.path')
        print_message(f"Source path from properties: {source_path if source_path else 'Not defined'}", "INFO")

        if properties.get('bbdd.rdbms') != 'POSTGRE':
            raise ValueError(f"Unsupported RDBMS: {properties.get('bbdd.rdbms')}. Only POSTGRE is supported.")

        print_message("Parsing database connection parameters...", "CONFIG_LOAD")
        db_params = parse_db_params(properties)

        print_message("Attempting to load table configurations from YAML files (archiving.yaml)...", "CONFIG_LOAD")
        table_fields_config = get_all_tables_and_fields(source_path)
        if not table_fields_config:
            if source_path:
                print_message("Warning: No tables or partition fields found in any 'archiving.yaml' files...", "WARNING")
            else:
                print_message("Warning: 'source.path' not set in properties. Skipping YAML configuration loading.", "WARNING")
        else:
            print_message(f"Found {len(table_fields_config)} table(s) configured in YAML files for potential partitioning.", "INFO")

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

            if not table_fields_config:
                print_message("No tables configured for processing from YAML. Script will exit.", "INFO")
                return

            for table_name, fields in table_fields_config.items():
                if not fields:
                    print_message(f"\n--- Skipping table: {table_name} (No partition fields.) ---", "WARNING")
                    migration_summary.append({'table_name': table_name, 'status': 'SKIPPED', 'message': 'No partition fields specified.'})
                    continue

                partition_field = next(iter(fields))
                header_line = "="*70
                print_message(f"\n\n{header_line}", "TABLE_PROCESS")
                print_message(f"=== Processing table: {Style.BOLD}{table_name}{Style.RESET} (Partition field: {Style.UNDERLINE}{partition_field}{Style.RESET}) ===", "TABLE_PROCESS")
                print_message(header_line, "TABLE_PROCESS")
                try:
                    execute_partition_steps(conn, table_name, partition_field, source_path, dry_run)
                    migration_summary.append({'table_name': table_name, 'status': 'SUCCESS', 'message': ''})
                except Exception as e_table:
                    error_msg = str(e_table).replace('\n', ' ') # Limpiar mensaje para el resumen
                    migration_summary.append({'table_name': table_name, 'status': 'FAILURE', 'message': error_msg[:200]}) # Truncar mensaje largo
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

    script_version_message = f"Script version: PK Modified, No CASCADE, Data Migrated, Temp Table Dropped, No Index Copy. Output: {'Enhanced' if _use_ansi_output else 'Plain'}"
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
