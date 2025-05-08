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

# --- Funciones auxiliares (sin cambios respecto a la versión anterior) ---
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
        print(f"Error reading {file_path}: {e}")
        return {}

def parse_db_params(properties):
    if not all([properties.get('bbdd.url'), properties.get('bbdd.sid'), properties.get('bbdd.user'), properties.get('bbdd.password')]):
        raise ValueError("Missing required database connection properties")
    url_match = re.match(r'jdbc:postgresql://([^:]+):(\d+)', properties['bbdd.url'])
    if not url_match:
        raise ValueError(f"Invalid bbdd.url format: {properties['bbdd.url']}")
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
        print(f"Error reading {file_path}: {e}")
        return {}

def get_all_tables_and_fields(root_path):
    all_table_fields = defaultdict(set)
    if not root_path:
        print("Warning: 'source.path' undefined, cannot get tables/fields from YAML.")
        return {}
    modules_path = Path(root_path) / 'modules'
    if not modules_path.exists() or not modules_path.is_dir():
        print(f"Warning: 'modules' dir not found at {modules_path}. Cannot read archiving.yaml.")
        return {}
    for yaml_file in modules_path.rglob('archiving.yaml'):
        table_fields = read_yaml_config(yaml_file)
        for table, fields in table_fields.items():
            all_table_fields[table].update(fields)
    return dict(all_table_fields)

def get_table_schema(conn, table_name, schema='public'):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position", (schema, table_name))
            schema_info = cur.fetchall()
            print(f"Schema for {schema}.{table_name}:")
            for col_name, col_type, nullable, default in schema_info:
                print(f"  {col_name}: {col_type} (Nullable: {nullable}, Default: {default})")
            return schema_info
    except Exception as e:
        print(f"Error retrieving schema for {schema}.{table_name}: {e}")
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
        print(f"Error checking if table {table_name_to_check} exists: {e}")
        return None

def is_table_partitioned(conn, table_name, schema):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = %s AND n.nspname = %s", (table_name, schema))
            result = cur.fetchone()
            return result[0] == 'p' if result else False
    except Exception as e:
        print(f"Error checking if {schema}.{table_name} is partitioned: {e}")
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
        print(f"Error finding dependent views for {schema}.{table_name}: {e}")
        return []

def get_triggers(conn, table_name, schema):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT n.nspname AS schema_name, tg.tgname AS trigger_name, pg_get_triggerdef(tg.oid) as trigger_definition FROM pg_trigger tg JOIN pg_class t ON tg.tgrelid = t.oid JOIN pg_namespace n ON t.relnamespace = n.oid WHERE t.relname = %s AND n.nspname = %s", (table_name, schema))
            return cur.fetchall()
    except Exception as e:
        print(f"Error finding triggers for {schema}.{table_name}: {e}")
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
    except Exception as e: print(f"Error retrieving FKs ON {schema}.{table_name}: {e}"); return []


def get_foreign_keys_referencing_table(conn, referenced_table_name, referenced_schema_name):
    """
    Retrieves FKs in OTHER tables that reference the given table, and the columns on THOSE OTHER tables.
    """
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
    except Exception as e: print(f"Error retrieving FKs REFERENCING {referenced_schema_name}.{referenced_table_name}: {e}"); return []

def get_indexes_for_table(conn, schema, table_name_idx):
    """Returns a dict: index_name -> list of ordered column names for non-PK indexes."""
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
    except Exception as e: print(f"Error retrieving indexes for {schema}.{table_name_idx}: {e}")
    return indexes

def drop_triggers(conn, table_name, schema, dry_run=False):
    dropped_triggers_info = []
    try:
        triggers = get_triggers(conn, table_name, schema)
        for trigger_schema, trigger_name, trigger_def in triggers:
            if trigger_name.startswith('RI_ConstraintTrigger') or trigger_name.startswith('zombodb_'):
                print(f"Skipping system/special trigger {trigger_schema}.{trigger_name}")
                continue
            sql_stmt_str = f"DROP TRIGGER {sql.Identifier(trigger_name).as_string(conn)} ON {sql.Identifier(schema).as_string(conn)}.{sql.Identifier(table_name).as_string(conn)}"
            if dry_run: print(f"[Dry Run] Would execute: {sql_stmt_str}")
            else:
                with conn.cursor() as cur: cur.execute(sql_stmt_str)
                print(f"Dropped trigger {trigger_schema}.{trigger_name} on {schema}.{table_name}")
            dropped_triggers_info.append({'name': trigger_name, 'schema': trigger_schema, 'definition': trigger_def})
        return dropped_triggers_info
    except Exception as e:
        print(f"Error dropping triggers for {schema}.{table_name}: {e}")
        raise

def validate_partition_field(conn, table_to_validate, schema_of_table, field_to_validate):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT data_type FROM information_schema.columns WHERE table_schema = %s AND table_name = %s AND column_name = %s", (schema_of_table, table_to_validate, field_to_validate))
            result = cur.fetchone()
            if not result: raise Exception(f"Partition field '{field_to_validate}' not found in {schema_of_table}.{table_to_validate}")
            data_type = result[0].lower()
            if not ('timestamp' in data_type or 'date' == data_type): raise Exception(f"Partition field '{field_to_validate}' must be timestamp or date, found {data_type}")
            print(f"Partition field '{field_to_validate}' in {schema_of_table}.{table_to_validate} is type {data_type}, valid.")
    except Exception as e:
        print(f"Error validating partition field for {schema_of_table}.{table_to_validate}: {e}")
        raise

def get_primary_key_info(conn, table_name_pk, schema_pk):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT tc.constraint_name, array_agg(kcu.column_name::text ORDER BY kcu.ordinal_position) AS columns FROM information_schema.table_constraints tc JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema WHERE tc.table_schema = %s AND tc.table_name = %s AND tc.constraint_type = 'PRIMARY KEY' GROUP BY tc.constraint_name", (schema_pk, table_name_pk))
            result = cur.fetchone()
            return {'name': result[0], 'columns': list(result[1])} if result else {'name': None, 'columns': []}
    except Exception as e:
        print(f"Error retrieving PK info for {schema_pk}.{table_name_pk}: {e}")
        return {'name': None, 'columns': []}

def find_table_xml_file(root_path, table_name):
    table_name_upper = table_name.upper()
    possible_extensions = ['.XML', '.xml']
    found_files = []
    if not root_path or not Path(root_path).is_dir(): return None
    for ext in possible_extensions:
        xml_filename = f"{table_name_upper}{ext}"
        base_path = Path(root_path) / 'src-db' / 'database' / 'model' / 'tables' / xml_filename
        if base_path.exists(): found_files.append(base_path)
    if not found_files:
        modules_path = Path(root_path) / 'modules'
        if modules_path.is_dir():
            try:
                for ext in possible_extensions:
                    xml_filename = f"{table_name_upper}{ext}"
                    for xml_file in modules_path.rglob(f"*/src-db/database/model/tables/{xml_filename}"):
                        if xml_file.exists(): found_files.append(xml_file)
            except Exception as e: print(f"Error searching XML in {modules_path}: {e}")
    if not found_files: return None
    if len(found_files) > 1: print(f"Warning: Multiple XML for {table_name_upper}: {found_files}. Using {found_files[0]}")
    return found_files[0]

def rename_table_xml_file(xml_file_path_obj, dry_run=False):
    if not xml_file_path_obj: return False
    try:
        original_stem = xml_file_path_obj.stem
        if original_stem.upper().endswith("_PARTITIONED"): original_stem = original_stem[:-len("_PARTITIONED")]
        new_name_stem = f"{original_stem}_PARTITIONED"
        new_name = xml_file_path_obj.with_name(f"{new_name_stem}{xml_file_path_obj.suffix}")
        if new_name.exists():
            print(f"Cannot rename {xml_file_path_obj} to {new_name}: Target exists")
            return False
        if dry_run: print(f"[Dry Run] Would rename {xml_file_path_obj} to {new_name}")
        else:
            xml_file_path_obj.rename(new_name)
            print(f"Renamed XML {xml_file_path_obj} to {new_name}")
        return True
    except Exception as e:
        print(f"Error renaming XML {xml_file_path_obj}: {e}")
        return False

def get_year_range(conn, table_name_for_range, schema_for_range, partition_by_field):
    """Determine the min and max years from data in a table for partitioning."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("SELECT EXTRACT(YEAR FROM MIN({}))::int, EXTRACT(YEAR FROM MAX({}))::int FROM {}.{}").format(
                    sql.Identifier(partition_by_field), sql.Identifier(partition_by_field),
                    sql.Identifier(schema_for_range), sql.Identifier(table_name_for_range)
                )
            )
            min_year, max_year = cur.fetchone()
            current_year = datetime.now().year
            # Handle cases where table might be empty or field has only NULLs
            min_year = min_year if min_year is not None else current_year
            max_year = max_year if max_year is not None else current_year
            if min_year > max_year: max_year = min_year # Ensure max_year is not less than min_year
            return min_year, max_year
    except Exception as e:
        print(f"Error determining year range for {schema_for_range}.{table_name_for_range} (field {partition_by_field}): {e}. Using default range.")
        current_year = datetime.now().year
        return current_year, current_year + 1 # Default to current and next year on error

# --- Main Execution Function ---
def execute_partition_steps(conn, table_to_partition_name, partition_by_field, root_path=None, dry_run=False):
    new_partitioned_table_name = table_to_partition_name
    tmp_table_name = f"{table_to_partition_name}_tmp"
    partitions_schema_name = "partitions" # Schema for storing partition tables
    dropped_referencing_fks_info = []

    try:
        actual_schema = table_exists(conn, table_to_partition_name)
        if not actual_schema:
            raise Exception(f"Table '{table_to_partition_name}' does not exist.")

        print(f"Processing table {actual_schema}.{table_to_partition_name} for partitioning by '{partition_by_field}'.")

        if is_table_partitioned(conn, new_partitioned_table_name, actual_schema):
            print(f"Table {actual_schema}.{new_partitioned_table_name} is already a partitioned table. Skipping.")
            return

        # --- Initial Preparations and Validations ---
        get_table_schema(conn, table_to_partition_name, actual_schema)
        validate_partition_field(conn, table_to_partition_name, actual_schema, partition_by_field)

        print(f"\n--- Preparing original table {actual_schema}.{table_to_partition_name} ---")
        pk_info = get_primary_key_info(conn, table_to_partition_name, actual_schema)
        original_pk_name = pk_info['name']
        original_pk_columns = pk_info['columns']
        if original_pk_name: print(f"Original PK: Name='{original_pk_name}', Columns={original_pk_columns}")
        else: print(f"No PK found on {actual_schema}.{table_to_partition_name}.")

        # --- Step 1-5: Drop dependencies and PK from original table (NO CASCADE) ---
        print(f"\n[Step 1] Dropping dependent views...")
        for vs, vn, _ in get_dependent_views(conn, table_to_partition_name, actual_schema):
            s = f"DROP VIEW IF EXISTS {sql.Identifier(vs).as_string(conn)}.{sql.Identifier(vn).as_string(conn)}";
            if dry_run: print(f"[Dry Run] {s}")
            else: conn.cursor().execute(s); print(f"Dropped view {vs}.{vn}")

        # Step 2: Drop FKs from OTHER tables referencing current table's PK + their supporting indexes
        print(f"\n[Step 2] Dropping external FKs referencing {actual_schema}.{table_to_partition_name} and their indexes...")
        fks_referencing = get_foreign_keys_referencing_table(conn, table_to_partition_name, actual_schema)
        for fk in fks_referencing:
            fk_referencing_table_schema = fk['referencing_schema']
            fk_referencing_table_name = fk['referencing_table']
            fk_constraint_name = fk['name']
            fk_columns_on_ref_table = fk['columns']

            s_fk = (f"ALTER TABLE {sql.Identifier(fk_referencing_table_schema).as_string(conn)}.{sql.Identifier(fk_referencing_table_name).as_string(conn)} "
                    f"DROP CONSTRAINT IF EXISTS {sql.Identifier(fk_constraint_name).as_string(conn)}")
            if dry_run: print(f"[Dry Run] External FK Drop: {s_fk}")
            else: conn.cursor().execute(s_fk); print(f"Dropped external FK '{fk_constraint_name}' from {fk_referencing_table_schema}.{fk_referencing_table_name}")

            # Now find and drop exact matching index for these FK columns on the referencing table
            indexes_on_ref_table = get_indexes_for_table(conn, fk_referencing_table_schema, fk_referencing_table_name)
            for index_name, index_cols in indexes_on_ref_table.items():
                if index_cols == fk_columns_on_ref_table:
                    s_idx = f"DROP INDEX IF EXISTS {sql.Identifier(fk_referencing_table_schema).as_string(conn)}.{sql.Identifier(index_name).as_string(conn)}"
                    if dry_run: print(f"[Dry Run] External FK Index Drop: {s_idx}")
                    else: conn.cursor().execute(s_idx); print(f"Dropped index '{index_name}' on {fk_referencing_table_schema}.{fk_referencing_table_name} (was for FK {fk_constraint_name})")
                    break # Assuming only one such exact index

        # Step 3: Drop FKs defined ON the original table + their supporting indexes
        print(f"\n[Step 3] Dropping FKs defined ON {actual_schema}.{table_to_partition_name} and related indexes...")
        for fk in get_foreign_keys_on_table(conn, table_to_partition_name, actual_schema):
            s_fk = (f"ALTER TABLE {sql.Identifier(actual_schema).as_string(conn)}.{sql.Identifier(table_to_partition_name).as_string(conn)} "
                    f"DROP CONSTRAINT IF EXISTS {sql.Identifier(fk['name']).as_string(conn)}")
            if dry_run: print(f"[Dry Run] Own FK Drop: {s_fk}")
            else: conn.cursor().execute(s_fk); print(f"Dropped own FK '{fk['name']}'")
            for idx_name, idx_cols in get_indexes_for_table(conn, actual_schema, table_to_partition_name).items():
                if idx_cols == fk['columns']:
                    s_idx = f"DROP INDEX IF EXISTS {sql.Identifier(actual_schema).as_string(conn)}.{sql.Identifier(idx_name).as_string(conn)}"
                    if dry_run: print(f"[Dry Run] Own FK Index Drop: {s_idx}")
                    else: conn.cursor().execute(s_idx); print(f"Dropped index '{idx_name}' on {actual_schema}.{table_to_partition_name}")
                    break

        print(f"\n[Step 4] Dropping triggers...")
        drop_triggers(conn, table_to_partition_name, actual_schema, dry_run)

        if original_pk_name:
            print(f"\n[Step 5] Dropping PK '{original_pk_name}'...")
            s = (f"ALTER TABLE {sql.Identifier(actual_schema).as_string(conn)}.{sql.Identifier(table_to_partition_name).as_string(conn)} "
                 f"DROP CONSTRAINT IF EXISTS {sql.Identifier(original_pk_name).as_string(conn)}")
            if dry_run: print(f"[Dry Run] {s}")
            else: conn.cursor().execute(s); print(f"Dropped PK {original_pk_name}")

        # --- Step 6: Rename original table to _tmp ---
        print(f"\n[Step 6] Renaming {actual_schema}.{table_to_partition_name} to {actual_schema}.{tmp_table_name}...")
        if table_exists(conn, tmp_table_name, actual_schema): raise Exception(f"Temp table {actual_schema}.{tmp_table_name} already exists.")
        stmt_rename = sql.SQL("ALTER TABLE {}.{} RENAME TO {}").format(sql.Identifier(actual_schema), sql.Identifier(table_to_partition_name), sql.Identifier(tmp_table_name))
        if dry_run: print(f"[Dry Run] {stmt_rename.as_string(conn)}")
        else: conn.cursor().execute(stmt_rename); print(f"Renamed to {actual_schema}.{tmp_table_name}")

        print(f"\n--- Creating new partitioned table {actual_schema}.{new_partitioned_table_name} and migrating data ---")

        # --- Step 7: Create new partitioned table ---
        print(f"\n[Step 7] Creating new partitioned table {actual_schema}.{new_partitioned_table_name}...")
        stmt_create_part = sql.SQL("CREATE TABLE {}.{} (LIKE {}.{} INCLUDING DEFAULTS INCLUDING INDEXES INCLUDING STORAGE INCLUDING COMMENTS) PARTITION BY RANGE ({})").format(
            sql.Identifier(actual_schema), sql.Identifier(new_partitioned_table_name),
            sql.Identifier(actual_schema), sql.Identifier(tmp_table_name), sql.Identifier(partition_by_field))
        if dry_run: print(f"[Dry Run] {stmt_create_part.as_string(conn)}")
        else: conn.cursor().execute(stmt_create_part); print(f"Created partitioned table {actual_schema}.{new_partitioned_table_name}")

        # --- Step 8: Recreate PK on new partitioned table (Modified Logic) ---
        if original_pk_name and original_pk_columns:
            print(f"\n[Step 8] Recreating PK on {actual_schema}.{new_partitioned_table_name}...")
            new_pk_cols = list(original_pk_columns)
            if partition_by_field not in new_pk_cols: new_pk_cols.append(partition_by_field)
            print(f"  Using original PK name: {original_pk_name}, New PK columns: {new_pk_cols}")
            if partition_by_field not in new_pk_cols: raise Exception(f"FATAL: Partition field '{partition_by_field}' MUST be part of new PK columns {new_pk_cols}")
            stmt_add_pk = sql.SQL("ALTER TABLE {}.{} ADD CONSTRAINT {} PRIMARY KEY ({})").format(
                sql.Identifier(actual_schema), sql.Identifier(new_partitioned_table_name),
                sql.Identifier(original_pk_name), sql.SQL(', ').join(map(sql.Identifier, new_pk_cols)))
            if dry_run: print(f"[Dry Run] {stmt_add_pk.as_string(conn)}")
            else: conn.cursor().execute(stmt_add_pk); print(f"Created PK {original_pk_name} with columns {new_pk_cols}")
        else: print(f"\n[Step 8] No original PK to recreate.")

        # --- Step 9: Create schema for partitions ---
        print(f"\n[Step 9] Ensuring schema '{partitions_schema_name}' exists...")
        stmt_create_schema = sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(partitions_schema_name))
        if dry_run: print(f"[Dry Run] {stmt_create_schema.as_string(conn)}")
        else: conn.cursor().execute(stmt_create_schema); print(f"Schema '{partitions_schema_name}' ensured.")

        # --- Step 10: Determine year range from _tmp table ---
        print(f"\n[Step 10] Determining year range for partitions from {actual_schema}.{tmp_table_name}...")
        start_year, end_year = get_year_range(conn, tmp_table_name, actual_schema, partition_by_field)
        print(f"Data range found: {start_year} to {end_year}.")

        # --- Step 11: Create partitions for each year ---
        print(f"\n[Step 11] Creating yearly partitions for {actual_schema}.{new_partitioned_table_name}...")
        if start_year > end_year :
            print(f"Warning: start_year ({start_year}) is greater than end_year ({end_year}). No partitions will be created based on data range. Creating a default partition for current year.")
            # Handle case with no data or very narrow range - create at least one partition for safety / future inserts
            current_year_for_default_part = datetime.now().year
            start_year = current_year_for_default_part
            end_year = current_year_for_default_part

        for year_val in range(start_year, end_year + 1): # Loop through inclusive years with data
            part_table_name = f"{new_partitioned_table_name}_y{year_val}"
            from_date_str = f"{year_val}-01-01"
            to_date_str = f"{year_val + 1}-01-01"
            stmt_create_actual_part = sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} PARTITION OF {}.{} FOR VALUES FROM (%s) TO (%s)").format(
                sql.Identifier(partitions_schema_name), sql.Identifier(part_table_name),
                sql.Identifier(actual_schema), sql.Identifier(new_partitioned_table_name))
            # Added IF NOT EXISTS to allow re-running or handling existing partitions more gracefully
            if dry_run: print(f"[Dry Run] Would execute: {stmt_create_actual_part.as_string(conn)} with ('{from_date_str}', '{to_date_str}')")
            else:
                try:
                    conn.cursor().execute(stmt_create_actual_part, (from_date_str, to_date_str))
                    print(f"Ensured partition {partitions_schema_name}.{part_table_name} for year {year_val} ({from_date_str} to {to_date_str})")
                except psycopg2.Error as e_part: # Catch specific partition creation errors like overlap
                    print(f"Warning: Could not create/ensure partition for year {year_val}: {e_part}")
                    conn.rollback() # Rollback the failed partition DDL
                    conn.autocommit = False # Ensure we are back in a valid transaction state for the next iteration/operation
                    # Depending on the error, might want to skip or halt. For now, try to continue.

        # --- Step 12: Copy data from _tmp to new partitioned table ---
        print(f"\n[Step 12] Copying data from {actual_schema}.{tmp_table_name} to {actual_schema}.{new_partitioned_table_name}...")
        stmt_copy_data = sql.SQL("INSERT INTO {}.{} SELECT * FROM {}.{}").format(
            sql.Identifier(actual_schema), sql.Identifier(new_partitioned_table_name),
            sql.Identifier(actual_schema), sql.Identifier(tmp_table_name))
        if dry_run: print(f"[Dry Run] {stmt_copy_data.as_string(conn)}")
        else:
            with conn.cursor() as cur:
                cur.execute(stmt_copy_data)
                print(f"Copied {cur.rowcount} rows to {actual_schema}.{new_partitioned_table_name}")

        print(f"\n--- Data migration to {actual_schema}.{new_partitioned_table_name} complete. ---")

        if dropped_referencing_fks_info:
            print("\nINFO: Following external FKs were dropped and NOT recreated:")
            for fk_info in dropped_referencing_fks_info: print(f"  - FK '{fk_info['name']}' on {fk_info['referencing_schema']}.{fk_info['referencing_table']}")

        if not dry_run: conn.commit()
        else: conn.rollback()
        print(f"\n{'[Dry Run] Simulated' if dry_run else 'Successfully completed'} partitioning process for {actual_schema}.{new_partitioned_table_name}.")
        print(f"The original data remains in {actual_schema}.{tmp_table_name} for verification or manual rollback if needed.")
        print(f"Dropping {actual_schema}.{tmp_table_name} is a separate, subsequent step.")


    except Exception as e:
        ## print stack trace
        traceback.print_exc()
        current_table = table_to_partition_name if 'table_to_partition_name' in locals() else "UNKNOWN"
        print(f"ERROR processing table {current_table}: {e}")
        if conn and not conn.closed:
            try: conn.rollback()
            except Exception as rb_e: print(f"Rollback error: {rb_e}")
        raise

# --- Main Program Flow ---
def main(dry_run=False):
    properties_file = "config/Openbravo.properties"
    try:
        properties = read_properties_file(properties_file)
        source_path = properties.get('source.path')
        if properties.get('bbdd.rdbms') != 'POSTGRE':
            raise ValueError(f"Unsupported RDBMS: {properties.get('bbdd.rdbms')}. Only POSTGRE is supported.")
        db_params = parse_db_params(properties)

        table_fields_config = get_all_tables_and_fields(source_path)
        if not table_fields_config and source_path:
            print("No tables/fields in archiving.yaml or 'source.path' misconfigured for YAML.")
        elif not source_path:
            print("Note: 'source.path' not set in properties, skipping YAML configuration loading for tables.")

        conn = None
        try:
            conn = psycopg2.connect(**db_params)
            session_config = properties.get('bbdd.sessionConfig')
            if session_config:
                with conn.cursor() as cur:
                    if dry_run: print(f"[Dry Run] Session config: {session_config}")
                    else:
                        cur.execute(session_config); conn.commit()
                        print(f"Applied session config: {session_config}")

            if not table_fields_config:
                print("No tables configured for processing from YAML. Script will exit unless tables are specified differently.")
                return

            for table_name, fields in table_fields_config.items():
                if not fields:
                    print(f"Skipping {table_name}: No partition fields.")
                    continue
                partition_field = next(iter(fields))
                print(f"\n=== Processing table: {table_name} (Partition field: {partition_field}) ===")
                try:
                    execute_partition_steps(conn, table_name, partition_field, source_path, dry_run)
                except Exception as e_table: # Catch exception per table to allow loop to continue
                    print(f"!!!!!!!! FAILED to process table {table_name}: {e_table} !!!!!!!!")
                    print(f"Continuing with next table if any...")
                print(f"=== Finished processing for table: {table_name} ===")
        finally:
            if conn and not conn.closed: conn.close(); print("DB connection closed.")
    except Exception as e_main:
        print(f"MAIN SCRIPT ERROR: {e_main}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate PostgreSQL tables to partitioned tables (PK modified, No CASCADE, Data Migrated).")
    parser.add_argument('--dry-run', action='store_true', help="Simulate changes without execution.")
    args = parser.parse_args()
    print(f"Script version: PK Mod+DataMig, No CASCADE, No update_exclude_tables_xml. Time: {datetime.now()}")
    if args.dry_run: print("*** DRY RUN MODE ENABLED ***")
    main(dry_run=args.dry_run)
    print("Script finished.")
