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
    """
    Reads a Java properties file and returns a dictionary of key-value pairs.
    Handles potential errors during file reading.
    """
    try:
        config = ConfigParser()
        with open(file_path, 'r', encoding='utf-8') as f:
            # ConfigParser needs a section header, so we add a default one
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
    """
    Parses database connection parameters from the properties dictionary.
    Raises ValueError if required parameters are missing or format is invalid.
    """
    if not all([properties.get('bbdd.url'), properties.get('bbdd.sid'), properties.get('bbdd.user'), properties.get('bbdd.password')]):
        raise ValueError("Missing required database connection properties (bbdd.url, bbdd.sid, bbdd.user, bbdd.password)")
    # Regex to extract host and port from a JDBC PostgreSQL URL
    url_match = re.match(r'jdbc:postgresql://([^:]+):(\d+)', properties['bbdd.url'])
    if not url_match:
        raise ValueError(f"Invalid bbdd.url format: {properties['bbdd.url']}. Expected format: jdbc:postgresql://host:port")
    host, port = url_match.groups()
    return {'host': host, 'port': port, 'database': properties['bbdd.sid'],
            'user': properties.get('bbdd.user'), 'password': properties.get('bbdd.password')}

def read_yaml_config(file_path):
    """
    Reads a YAML configuration file and extracts table and field information.
    Returns a dictionary where keys are table names and values are sets of fields.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f) or {} # Ensure config is a dict even if YAML is empty
            tables = config.get('tables', {})
            # Convert list of fields to a set for efficient operations
            return {table: set(fields) for table, fields in tables.items()}
    except Exception as e:
        print(f"Error reading YAML configuration from {file_path}: {e}")
        return {}

def get_all_tables_and_fields(root_path):
    """
    Scans for 'archiving.yaml' files within a specified root path (typically 'modules' subdirectory).
    Aggregates table and field configurations from all found YAML files.
    """
    all_table_fields = defaultdict(set)
    if not root_path:
        print("Warning: 'source.path' is undefined. Cannot get tables and fields from YAML files.")
        return {}

    modules_path = Path(root_path) / 'modules'
    if not modules_path.exists() or not modules_path.is_dir():
        print(f"Warning: 'modules' directory not found at {modules_path}. Cannot read archiving.yaml files.")
        return {}

    # Recursively find all 'archiving.yaml' files
    for yaml_file in modules_path.rglob('archiving.yaml'):
        print(f"Reading archiving config from: {yaml_file}")
        table_fields = read_yaml_config(yaml_file)
        for table, fields in table_fields.items():
            all_table_fields[table].update(fields)
    return dict(all_table_fields)

def get_table_schema(conn, table_name, schema='public'):
    """
    Retrieves and prints the schema information for a given table.
    Includes column name, data type, nullability, and default value.
    """
    try:
        with conn.cursor() as cur:
            # Query information_schema.columns for table structure details
            cur.execute("""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (schema, table_name))
            schema_info = cur.fetchall()
            print(f"Schema for {schema}.{table_name}:")
            for col_name, col_type, nullable, default in schema_info:
                print(f"  - Column: {col_name}, Type: {col_type}, Nullable: {nullable}, Default: {default}")
            return schema_info
    except Exception as e:
        print(f"Error retrieving schema for {schema}.{table_name}: {e}")
        return []

def table_exists(conn, table_name_to_check, schema_to_check=None):
    """
    Checks if a table exists in the database.
    If schema_to_check is provided, looks only in that schema.
    Otherwise, searches in all schemas except system schemas.
    Returns the schema name if found, None otherwise.
    """
    try:
        with conn.cursor() as cur:
            if schema_to_check:
                cur.execute("""
                    SELECT table_schema
                    FROM information_schema.tables
                    WHERE table_name = %s AND table_schema = %s
                """, (table_name_to_check, schema_to_check))
            else:
                # Exclude system schemas like pg_catalog and information_schema
                cur.execute("""
                    SELECT table_schema
                    FROM information_schema.tables
                    WHERE table_name = %s AND table_schema NOT IN ('pg_catalog', 'information_schema')
                """, (table_name_to_check,))
            result = cur.fetchone()
            return result[0] if result else None
    except Exception as e:
        print(f"Error checking if table '{table_name_to_check}' exists (schema: {schema_to_check if schema_to_check else 'any'}): {e}")
        return None

def is_table_partitioned(conn, table_name, schema):
    """
    Checks if a given table is a partitioned table (not a partition itself).
    Uses pg_class.relkind where 'p' indicates a partitioned table.
    """
    try:
        with conn.cursor() as cur:
            # pg_class stores metadata about database objects
            # relkind = 'p' for partitioned tables
            cur.execute("""
                SELECT c.relkind
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relname = %s AND n.nspname = %s
            """, (table_name, schema))
            result = cur.fetchone()
            return result[0] == 'p' if result else False
    except Exception as e:
        print(f"Error checking if table {schema}.{table_name} is partitioned: {e}")
        return False

def get_dependent_views(conn, table_name, schema):
    """
    Finds all views that depend on the specified table.
    Queries pg_depend and pg_rewrite to identify dependencies.
    """
    try:
        with conn.cursor() as cur:
            # This query traces dependencies from views to the specified table
            cur.execute("""
                SELECT DISTINCT dependent_ns.nspname AS view_schema,
                                dependent_view.relname AS view_name,
                                pg_get_viewdef(dependent_view.oid) AS view_definition
                FROM pg_depend AS d
                JOIN pg_rewrite AS r ON r.oid = d.objid
                JOIN pg_class AS dependent_view ON dependent_view.oid = r.ev_class
                JOIN pg_namespace AS dependent_ns ON dependent_ns.oid = dependent_view.relnamespace
                JOIN pg_class AS source_table ON source_table.oid = d.refobjid
                JOIN pg_namespace AS source_ns ON source_ns.oid = source_table.relnamespace
                WHERE d.refclassid = 'pg_class'::REGCLASS
                  AND d.classid = 'pg_rewrite'::REGCLASS
                  AND dependent_view.relkind = 'v'  -- 'v' for view
                  AND source_table.relname = %s
                  AND source_ns.nspname = %s;
            """, (table_name, schema))
            return cur.fetchall()
    except Exception as e:
        print(f"Error finding dependent views for {schema}.{table_name}: {e}")
        return []

def get_triggers(conn, table_name, schema):
    """
    Retrieves all triggers defined on the specified table.
    Excludes system-generated triggers (e.g., for foreign keys).
    """
    try:
        with conn.cursor() as cur:
            # pg_trigger stores information about triggers
            cur.execute("""
                SELECT n.nspname AS schema_name, tg.tgname AS trigger_name, pg_get_triggerdef(tg.oid) as trigger_definition
                FROM pg_trigger tg
                JOIN pg_class t ON tg.tgrelid = t.oid
                JOIN pg_namespace n ON t.relnamespace = n.oid
                WHERE t.relname = %s AND n.nspname = %s
            """, (table_name, schema))
            return cur.fetchall()
    except Exception as e:
        print(f"Error finding triggers for {schema}.{table_name}: {e}")
        return []

def get_foreign_keys_on_table(conn, table_name, schema):
    """
    Retrieves foreign key constraints defined ON the specified table.
    Includes constraint name, definition, and the columns involved in the FK on this table.
    """
    try:
        with conn.cursor() as cur:
            # pg_constraint stores check, primary key, unique, foreign key, and exclusion constraints
            # contype = 'f' for foreign key
            cur.execute("""
                SELECT pc.conname AS constraint_name,
                       pg_get_constraintdef(pc.oid) AS constraint_definition,
                       ARRAY(
                           SELECT att.attname
                           FROM unnest(pc.conkey) WITH ORDINALITY k(attnum, ord)
                           JOIN pg_attribute att ON att.attrelid = pc.conrelid AND att.attnum = k.attnum
                           ORDER BY k.ord
                       ) AS fk_columns
                FROM pg_constraint pc
                JOIN pg_namespace n ON n.oid = pc.connamespace
                JOIN pg_class cl ON cl.oid = pc.conrelid
                WHERE pc.contype = 'f' AND cl.relname = %s AND n.nspname = %s;
            """, (table_name, schema))
            return [{'name': r[0], 'definition': r[1], 'columns': list(r[2])} for r in cur.fetchall()]
    except Exception as e:
        print(f"Error retrieving FKs defined ON {schema}.{table_name}: {e}")
        return []

def get_foreign_keys_referencing_table(conn, referenced_table_name, referenced_schema_name):
    """
    Retrieves FKs in OTHER tables that reference the given table.
    Includes constraint name, the referencing table/schema, definition, and the columns on THE REFERENCING TABLE.
    """
    fks_referencing = []
    try:
        with conn.cursor() as cur:
            # confrelid is the OID of the referenced table
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
                WHERE con.confrelid = (
                    SELECT oid FROM pg_class
                    WHERE relname = %s AND relnamespace = (
                        SELECT oid FROM pg_namespace WHERE nspname = %s
                    )
                )
                AND con.contype = 'f';
            """, (referenced_table_name, referenced_schema_name))
            for row in cur.fetchall():
                fks_referencing.append({
                    'name': row[0],
                    'referencing_table': row[1],
                    'referencing_schema': row[2],
                    'definition': row[3],
                    'columns': list(row[4]) # Columns on the table that HAS the FK
                })
        return fks_referencing
    except Exception as e:
        print(f"Error retrieving FKs REFERENCING {referenced_schema_name}.{referenced_table_name}: {e}")
        return []

def get_indexes_for_table(conn, schema, table_name_idx):
    """
    Returns a dictionary of non-Primary Key indexes for a table.
    Key: index_name, Value: list of ordered column names.
    Excludes primary key indexes (indisprimary = FALSE).
    """
    indexes = {}
    try:
        with conn.cursor() as cur:
            # pg_index contains information about indexes
            # indisprimary = FALSE to exclude PK constraints (which often have associated indexes)
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
            for row in cur.fetchall():
                indexes[row[0]] = list(row[1])
    except Exception as e:
        print(f"Error retrieving indexes for {schema}.{table_name_idx}: {e}")
    return indexes

def drop_triggers(conn, table_name, schema, dry_run=False):
    """
    Drops all user-defined triggers on a specified table.
    Skips system triggers (e.g., 'RI_ConstraintTrigger', 'zombodb_').
    Returns a list of dictionaries, each containing info about a dropped trigger.
    """
    dropped_triggers_info = []
    try:
        triggers = get_triggers(conn, table_name, schema)
        for trigger_schema, trigger_name, trigger_def in triggers:
            # Skip system-generated or special triggers
            if trigger_name.startswith('RI_ConstraintTrigger') or trigger_name.startswith('zombodb_'):
                print(f"Skipping system/special trigger {trigger_schema}.{trigger_name} on {schema}.{table_name}")
                continue

            # Use sql.Identifier for safe quoting of names
            sql_stmt_str = sql.SQL("DROP TRIGGER {} ON {}.{}").format(
                sql.Identifier(trigger_name),
                sql.Identifier(schema),
                sql.Identifier(table_name)
            ).as_string(conn)

            if dry_run:
                print(f"[Dry Run] Would execute: {sql_stmt_str}")
            else:
                with conn.cursor() as cur:
                    cur.execute(sql_stmt_str)
                print(f"Dropped trigger {trigger_schema}.{trigger_name} on {schema}.{table_name}")
            dropped_triggers_info.append({'name': trigger_name, 'schema': trigger_schema, 'definition': trigger_def})
        return dropped_triggers_info
    except Exception as e:
        print(f"Error dropping triggers for {schema}.{table_name}: {e}")
        raise # Re-raise the exception to be handled by the caller

def validate_partition_field(conn, table_to_validate, schema_of_table, field_to_validate):
    """
    Validates if the specified partition field exists in the table and is of a suitable type (timestamp or date).
    Raises an exception if validation fails.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT data_type
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s AND column_name = %s
            """, (schema_of_table, table_to_validate, field_to_validate))
            result = cur.fetchone()
            if not result:
                raise Exception(f"Partition field '{field_to_validate}' not found in table {schema_of_table}.{table_to_validate}.")

            data_type = result[0].lower()
            if not ('timestamp' in data_type or 'date' == data_type):
                raise Exception(f"Partition field '{field_to_validate}' in {schema_of_table}.{table_to_validate} must be of type timestamp or date, but found type {data_type}.")
            print(f"Validation successful: Partition field '{field_to_validate}' in {schema_of_table}.{table_to_validate} is of type {data_type}.")
    except Exception as e:
        print(f"Error validating partition field for {schema_of_table}.{table_to_validate}: {e}")
        raise

def get_primary_key_info(conn, table_name_pk, schema_pk):
    """
    Retrieves information about the primary key of a table.
    Returns a dictionary with 'name' (PK constraint name) and 'columns' (list of PK column names).
    """
    try:
        with conn.cursor() as cur:
            # Query table_constraints and key_column_usage for PK details
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
        print(f"Error retrieving PK information for {schema_pk}.{table_name_pk}: {e}")
        return {'name': None, 'columns': []} # Return default on error

def find_table_xml_file(root_path, table_name):
    """
    Searches for a table's XML definition file within a project structure.
    Looks in standard paths and handles potential case variations in filenames.
    Returns a Path object to the XML file if found, otherwise None.
    """
    table_name_upper = table_name.upper()
    possible_extensions = ['.XML', '.xml'] # Check for both upper and lower case extensions
    found_files = []

    if not root_path or not Path(root_path).is_dir():
        print(f"Warning: Root path '{root_path}' is not valid or not a directory. Cannot search for XML file for table '{table_name}'.")
        return None

    # Standard path for table XMLs
    base_search_path = Path(root_path) / 'src-db' / 'database' / 'model' / 'tables'
    for ext in possible_extensions:
        xml_filename = f"{table_name_upper}{ext}"
        xml_file_path = base_search_path / xml_filename
        if xml_file_path.exists():
            found_files.append(xml_file_path)

    # Fallback search in 'modules' subdirectories if not found in the primary location
    if not found_files:
        modules_path = Path(root_path) / 'modules'
        if modules_path.is_dir():
            try:
                for ext in possible_extensions:
                    xml_filename = f"{table_name_upper}{ext}"
                    # Recursively search for the XML file pattern within modules
                    for xml_file in modules_path.rglob(f"*/src-db/database/model/tables/{xml_filename}"):
                        if xml_file.exists():
                            found_files.append(xml_file)
            except Exception as e:
                print(f"Error occurred while searching for XML in modules path {modules_path}: {e}")

    if not found_files:
        print(f"No XML file found for table {table_name_upper} under '{root_path}'.")
        return None
    if len(found_files) > 1:
        # If multiple files are found, warn the user and use the first one.
        # This could indicate a misconfiguration or duplicate files.
        print(f"Warning: Multiple XML files found for table {table_name_upper}: {found_files}. Using the first one: {found_files[0]}")
    return found_files[0]

def rename_table_xml_file(xml_file_path_obj, dry_run=False):
    """
    Renames a table's XML file to indicate it's partitioned (e.g., MYTABLE.xml -> MYTABLE_PARTITIONED.xml).
    Handles existing files and potential errors.
    Returns True if successful or if dry_run is True and rename would occur, False otherwise.
    """
    if not xml_file_path_obj or not xml_file_path_obj.exists():
        print(f"Cannot rename XML: File path '{xml_file_path_obj}' is invalid or file does not exist.")
        return False
    try:
        original_stem = xml_file_path_obj.stem
        # Remove existing "_PARTITIONED" suffix if present to avoid double suffixing
        if original_stem.upper().endswith("_PARTITIONED"):
            original_stem = original_stem[:-len("_PARTITIONED")]

        new_name_stem = f"{original_stem}_PARTITIONED"
        new_xml_file_name = xml_file_path_obj.with_name(f"{new_name_stem}{xml_file_path_obj.suffix}")

        if new_xml_file_name.exists():
            print(f"Cannot rename XML file {xml_file_path_obj} to {new_xml_file_name}: Target file already exists.")
            return False

        if dry_run:
            print(f"[Dry Run] Would rename XML file: {xml_file_path_obj} TO {new_xml_file_name}")
        else:
            xml_file_path_obj.rename(new_xml_file_name)
            print(f"Successfully renamed XML file: {xml_file_path_obj} TO {new_xml_file_name}")
        return True
    except Exception as e:
        print(f"Error renaming XML file {xml_file_path_obj}: {e}")
        return False

def get_year_range(conn, table_name_for_range, schema_for_range, partition_by_field):
    """
    Determines the minimum and maximum years from the data in a table's partition field.
    This range is used to create yearly partitions.
    Handles cases with no data or NULLs in the partition field by defaulting to current/next year.
    """
    try:
        with conn.cursor() as cur:
            # SQL to extract min and max year from the partition_by_field
            # Cast to ::int as EXTRACT returns numeric/double precision
            query = sql.SQL("""
                SELECT EXTRACT(YEAR FROM MIN({}))::int, EXTRACT(YEAR FROM MAX({}))::int
                FROM {}.{}
            """).format(
                sql.Identifier(partition_by_field),
                sql.Identifier(partition_by_field),
                sql.Identifier(schema_for_range),
                sql.Identifier(table_name_for_range)
            )
            cur.execute(query)
            min_year, max_year = cur.fetchone()

            current_year = datetime.now().year

            # Handle cases where table is empty or partition field has only NULLs
            min_year = min_year if min_year is not None else current_year
            max_year = max_year if max_year is not None else current_year

            # Ensure max_year is not less than min_year (e.g., if table was empty and defaults were used)
            if min_year > max_year:
                max_year = min_year

            return min_year, max_year
    except Exception as e:
        print(f"Error determining year range for {schema_for_range}.{table_name_for_range} (field: {partition_by_field}): {e}. Using default range.")
        # Default to current year and next year if an error occurs
        current_year = datetime.now().year
        return current_year, current_year + 1


# --- Main Execution Function ---
def execute_partition_steps(conn, table_to_partition_name, partition_by_field, root_path=None, dry_run=False):
    """
    Orchestrates the entire process of partitioning a single table.
    Includes dependency dropping, table renaming, creating partitioned table, data migration, and PK recreation.
    """
    new_partitioned_table_name = table_to_partition_name # The final name of the partitioned table
    tmp_table_name = f"{table_to_partition_name}_tmp" # Temporary name for the original table
    partitions_schema_name = "partitions" # Schema for storing individual partition tables

    dropped_referencing_fks_info = [] # To keep track of FKs from other tables that were dropped

    try:
        # Determine the actual schema of the table to be partitioned
        actual_schema = table_exists(conn, table_to_partition_name)
        if not actual_schema:
            raise Exception(f"Table '{table_to_partition_name}' does not exist in any user schema.")

        print(f"Processing table {actual_schema}.{table_to_partition_name} for partitioning by field '{partition_by_field}'.")

        # Check if the target table is already a partitioned table (not a partition itself)
        if is_table_partitioned(conn, new_partitioned_table_name, actual_schema):
            print(f"Table {actual_schema}.{new_partitioned_table_name} is already a partitioned table. Skipping further processing for this table.")
            return # Exit if already partitioned

        # --- Initial Preparations and Validations ---
        print(f"\n--- Validating and Preparing Original Table: {actual_schema}.{table_to_partition_name} ---")
        get_table_schema(conn, table_to_partition_name, actual_schema) # Display schema for context
        validate_partition_field(conn, table_to_partition_name, actual_schema, partition_by_field) # Validate partition field type

        # Get original Primary Key information
        pk_info = get_primary_key_info(conn, table_to_partition_name, actual_schema)
        original_pk_name = pk_info['name']
        original_pk_columns = pk_info['columns']
        if original_pk_name:
            print(f"Original Primary Key found: Name='{original_pk_name}', Columns={original_pk_columns}")
        else:
            print(f"No Primary Key found on {actual_schema}.{table_to_partition_name}.")

        # --- Step 1-5: Drop dependencies and PK from original table (NO CASCADE) ---
        # These steps prepare the original table for renaming by removing constraints and dependencies.

        print(f"\n[Step 1] Dropping dependent views on {actual_schema}.{table_to_partition_name}...")
        dependent_views = get_dependent_views(conn, table_to_partition_name, actual_schema)
        if not dependent_views: print("No dependent views found.")
        for view_schema, view_name, _view_def in dependent_views:
            stmt_drop_view = sql.SQL("DROP VIEW IF EXISTS {}.{}").format(
                sql.Identifier(view_schema), sql.Identifier(view_name)
            )
            if dry_run:
                print(f"[Dry Run] Would execute: {stmt_drop_view.as_string(conn)}")
            else:
                conn.cursor().execute(stmt_drop_view)
                print(f"Dropped view {view_schema}.{view_name}")

        print(f"\n[Step 2] Dropping Foreign Keys from OTHER tables that reference {actual_schema}.{table_to_partition_name} (and their supporting indexes)...")
        fks_referencing_original_table = get_foreign_keys_referencing_table(conn, table_to_partition_name, actual_schema)
        if not fks_referencing_original_table: print("No external FKs found referencing this table.")
        for fk in fks_referencing_original_table:
            ref_table_schema = fk['referencing_schema']
            ref_table_name = fk['referencing_table']
            fk_constraint_name = fk['name']
            fk_columns_on_ref_table = fk['columns'] # Columns on the referencing table that form the FK

            stmt_drop_external_fk = sql.SQL("ALTER TABLE {}.{} DROP CONSTRAINT IF EXISTS {}").format(
                sql.Identifier(ref_table_schema), sql.Identifier(ref_table_name), sql.Identifier(fk_constraint_name)
            )
            if dry_run:
                print(f"[Dry Run] External FK Drop: {stmt_drop_external_fk.as_string(conn)}")
            else:
                conn.cursor().execute(stmt_drop_external_fk)
                print(f"Dropped external FK '{fk_constraint_name}' from table {ref_table_schema}.{ref_table_name}")
            dropped_referencing_fks_info.append(fk) # Store info about dropped FK

            # Attempt to find and drop an exact index supporting this FK on the referencing table
            indexes_on_referencing_table = get_indexes_for_table(conn, ref_table_schema, ref_table_name)
            for index_name, index_columns in indexes_on_referencing_table.items():
                if sorted(index_columns) == sorted(fk_columns_on_ref_table): # Match columns regardless of order in definition
                    stmt_drop_fk_index = sql.SQL("DROP INDEX IF EXISTS {}.{}").format(
                        sql.Identifier(ref_table_schema), sql.Identifier(index_name)
                    )
                    if dry_run:
                        print(f"[Dry Run] External FK Index Drop: {stmt_drop_fk_index.as_string(conn)}")
                    else:
                        conn.cursor().execute(stmt_drop_fk_index)
                        print(f"Dropped index '{index_name}' on {ref_table_schema}.{ref_table_name} (was supporting FK '{fk_constraint_name}')")
                    break # Assume one such index is sufficient

        print(f"\n[Step 3] Dropping Foreign Keys defined ON {actual_schema}.{table_to_partition_name} (and their supporting indexes)...")
        fks_on_original_table = get_foreign_keys_on_table(conn, table_to_partition_name, actual_schema)
        if not fks_on_original_table: print("No FKs found defined on this table.")
        for fk in fks_on_original_table:
            fk_constraint_name_on_orig = fk['name']
            fk_columns_on_orig_table = fk['columns'] # Columns on the original table that form this FK

            stmt_drop_own_fk = sql.SQL("ALTER TABLE {}.{} DROP CONSTRAINT IF EXISTS {}").format(
                sql.Identifier(actual_schema), sql.Identifier(table_to_partition_name), sql.Identifier(fk_constraint_name_on_orig)
            )
            if dry_run:
                print(f"[Dry Run] Own FK Drop: {stmt_drop_own_fk.as_string(conn)}")
            else:
                conn.cursor().execute(stmt_drop_own_fk)
                print(f"Dropped FK '{fk_constraint_name_on_orig}' defined on {actual_schema}.{table_to_partition_name}")

            # Attempt to find and drop an exact index supporting this FK on the original table
            indexes_on_original_table = get_indexes_for_table(conn, actual_schema, table_to_partition_name)
            for index_name, index_columns in indexes_on_original_table.items():
                if sorted(index_columns) == sorted(fk_columns_on_orig_table):
                    stmt_drop_own_fk_index = sql.SQL("DROP INDEX IF EXISTS {}.{}").format(
                        sql.Identifier(actual_schema), sql.Identifier(index_name)
                    )
                    if dry_run:
                        print(f"[Dry Run] Own FK Index Drop: {stmt_drop_own_fk_index.as_string(conn)}")
                    else:
                        conn.cursor().execute(stmt_drop_own_fk_index)
                        print(f"Dropped index '{index_name}' on {actual_schema}.{table_to_partition_name} (was supporting FK '{fk_constraint_name_on_orig}')")
                    break

        print(f"\n[Step 4] Dropping user-defined triggers on {actual_schema}.{table_to_partition_name}...")
        dropped_triggers = drop_triggers(conn, table_to_partition_name, actual_schema, dry_run)
        if not dropped_triggers and not dry_run: print("No user-defined triggers found to drop.")
        elif dry_run and not get_triggers(conn, table_to_partition_name, actual_schema): print("[Dry Run] No user-defined triggers would be dropped.")


        if original_pk_name:
            print(f"\n[Step 5] Dropping Primary Key '{original_pk_name}' from {actual_schema}.{table_to_partition_name}...")
            stmt_drop_pk = sql.SQL("ALTER TABLE {}.{} DROP CONSTRAINT IF EXISTS {}").format(
                sql.Identifier(actual_schema), sql.Identifier(table_to_partition_name), sql.Identifier(original_pk_name)
            )
            if dry_run:
                print(f"[Dry Run] Would execute: {stmt_drop_pk.as_string(conn)}")
            else:
                conn.cursor().execute(stmt_drop_pk)
                print(f"Dropped Primary Key '{original_pk_name}' from {actual_schema}.{table_to_partition_name}")
        else:
            print(f"\n[Step 5] Skipping PK drop as no PK was found on {actual_schema}.{table_to_partition_name}.")


        # --- Step 6: Rename original table to _tmp ---
        print(f"\n[Step 6] Renaming original table {actual_schema}.{table_to_partition_name} to {actual_schema}.{tmp_table_name}...")
        if table_exists(conn, tmp_table_name, actual_schema):
            # This check is crucial to prevent errors if the script is re-run partially
            raise Exception(f"Temporary table {actual_schema}.{tmp_table_name} already exists. Manual cleanup might be required.")
        stmt_rename_to_tmp = sql.SQL("ALTER TABLE {}.{} RENAME TO {}").format(
            sql.Identifier(actual_schema), sql.Identifier(table_to_partition_name), sql.Identifier(tmp_table_name)
        )
        if dry_run:
            print(f"[Dry Run] Would execute: {stmt_rename_to_tmp.as_string(conn)}")
        else:
            conn.cursor().execute(stmt_rename_to_tmp)
            print(f"Renamed table {actual_schema}.{table_to_partition_name} to {actual_schema}.{tmp_table_name}")

        print(f"\n--- Creating New Partitioned Table {actual_schema}.{new_partitioned_table_name} and Migrating Data ---")

        # --- Step 7: Create new partitioned table (parent table) ---
        # This table will be like the _tmp table (structure, defaults, etc.) but partitioned.
        # INCLUDING INDEXES will copy index definitions, but not PK constraints here.
        print(f"\n[Step 7] Creating new partitioned parent table {actual_schema}.{new_partitioned_table_name}...")
        stmt_create_partitioned_table = sql.SQL(
            "CREATE TABLE {}.{} (LIKE {}.{} INCLUDING DEFAULTS INCLUDING STORAGE INCLUDING COMMENTS) PARTITION BY RANGE ({})"
        ).format(
            sql.Identifier(actual_schema), sql.Identifier(new_partitioned_table_name),
            sql.Identifier(actual_schema), sql.Identifier(tmp_table_name), # Likeness from _tmp table
            sql.Identifier(partition_by_field) # Partition key
        )
        if dry_run:
            print(f"[Dry Run] Would execute: {stmt_create_partitioned_table.as_string(conn)}")
        else:
            conn.cursor().execute(stmt_create_partitioned_table)
            print(f"Created new partitioned parent table {actual_schema}.{new_partitioned_table_name}")

        # --- Step 8: Recreate PK on new partitioned table (Modified Logic) ---
        # The PK on a partitioned table *must* include the partition key.
        if original_pk_name and original_pk_columns:
            print(f"\n[Step 8] Recreating Primary Key on new partitioned table {actual_schema}.{new_partitioned_table_name}...")
            new_pk_columns = list(original_pk_columns) # Start with original PK columns
            if partition_by_field not in new_pk_columns:
                new_pk_columns.append(partition_by_field) # Add partition field if not already part of PK
            print(f"  Attempting to use original PK name: '{original_pk_name}'. New PK columns: {new_pk_columns}")

            # Crucial validation: Partition key must be part of the new PK
            if partition_by_field not in new_pk_columns: # This should ideally be caught by the append above, but double check
                raise Exception(f"FATAL: Partition field '{partition_by_field}' MUST be part of the new Primary Key columns {new_pk_columns} for table {actual_schema}.{new_partitioned_table_name}")

            stmt_add_pk_to_partitioned = sql.SQL("ALTER TABLE {}.{} ADD CONSTRAINT {} PRIMARY KEY ({})").format(
                sql.Identifier(actual_schema), sql.Identifier(new_partitioned_table_name),
                sql.Identifier(original_pk_name), # Use original PK constraint name
                sql.SQL(', ').join(map(sql.Identifier, new_pk_columns)) # Comma-separated list of new PK columns
            )
            if dry_run:
                print(f"[Dry Run] Would execute: {stmt_add_pk_to_partitioned.as_string(conn)}")
            else:
                conn.cursor().execute(stmt_add_pk_to_partitioned)
                print(f"Successfully created Primary Key '{original_pk_name}' with columns {new_pk_columns} on {actual_schema}.{new_partitioned_table_name}")
        else:
            print(f"\n[Step 8] No original Primary Key to recreate on {actual_schema}.{new_partitioned_table_name}.")


        # --- Step 9: Create schema for partitions if it doesn't exist ---
        print(f"\n[Step 9] Ensuring schema '{partitions_schema_name}' exists for storing partitions...")
        stmt_create_partitions_schema = sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(partitions_schema_name))
        if dry_run:
            print(f"[Dry Run] Would execute: {stmt_create_partitions_schema.as_string(conn)}")
        else:
            conn.cursor().execute(stmt_create_partitions_schema)
            print(f"Schema '{partitions_schema_name}' ensured (created if it did not exist).")

        # --- Step 10: Determine year range from _tmp table data ---
        print(f"\n[Step 10] Determining year range for partitions from data in {actual_schema}.{tmp_table_name}...")
        start_year, end_year = get_year_range(conn, tmp_table_name, actual_schema, partition_by_field)
        print(f"Data year range found in {tmp_table_name}: {start_year} to {end_year}.")

        # --- Step 11: Create partitions for each year in the determined range ---
        print(f"\n[Step 11] Creating yearly partitions for {actual_schema}.{new_partitioned_table_name} in schema '{partitions_schema_name}'...")
        if start_year > end_year:
            # This case can happen if the table is empty or only has NULLs in partition_by_field
            # and get_year_range defaulted. Create a partition for the current year at least.
            print(f"Warning: Calculated start_year ({start_year}) is greater than end_year ({end_year}). This might occur for empty tables. Creating a default partition for the current year.")
            current_year_for_default_part = datetime.now().year
            start_year = current_year_for_default_part
            end_year = current_year_for_default_part # Create one partition for the current year

        for year_val in range(start_year, end_year + 2): # Loop through inclusive years, +2 for current year and one future year
            partition_table_name_for_year = f"{new_partitioned_table_name}_y{year_val}"
            from_date_str = f"{year_val}-01-01" # Start of the year
            to_date_str = f"{year_val + 1}-01-01"   # Start of the next year (exclusive upper bound)

            # Statement to create the actual partition table
            stmt_create_actual_partition = sql.SQL(
                "CREATE TABLE IF NOT EXISTS {}.{} PARTITION OF {}.{} FOR VALUES FROM (%s) TO (%s)"
            ).format(
                sql.Identifier(partitions_schema_name), sql.Identifier(partition_table_name_for_year),
                sql.Identifier(actual_schema), sql.Identifier(new_partitioned_table_name)
            )
            if dry_run:
                print(f"[Dry Run] Would execute: {stmt_create_actual_partition.as_string(conn)} with bounds ('{from_date_str}', '{to_date_str}')")
            else:
                try:
                    with conn.cursor() as cur:
                        cur.execute(stmt_create_actual_partition, (from_date_str, to_date_str))
                    print(f"Ensured partition {partitions_schema_name}.{partition_table_name_for_year} for year {year_val} (range: {from_date_str} to {to_date_str})")
                except psycopg2.Error as e_partition_create:
                    # Catch errors like partition overlap if IF NOT EXISTS isn't fully effective or other issues
                    print(f"Warning: Could not create/ensure partition for year {year_val} ({partitions_schema_name}.{partition_table_name_for_year}): {e_partition_create}")
                    conn.rollback() # Rollback the specific DDL that failed
                    conn.autocommit = False # Reset autocommit state if it was changed by error handling
                    # This is important to ensure subsequent operations are in a valid transaction state.


        # --- Step 12: Copy data from _tmp table to the new partitioned table ---
        print(f"\n[Step 12] Copying data from {actual_schema}.{tmp_table_name} to {actual_schema}.{new_partitioned_table_name}...")
        stmt_copy_data_to_partitioned = sql.SQL("INSERT INTO {}.{} SELECT * FROM {}.{}").format(
            sql.Identifier(actual_schema), sql.Identifier(new_partitioned_table_name),
            sql.Identifier(actual_schema), sql.Identifier(tmp_table_name)
        )
        if dry_run:
            print(f"[Dry Run] Would execute: {stmt_copy_data_to_partitioned.as_string(conn)}")
            # Simulate row count for dry run if needed, or just skip
        else:
            with conn.cursor() as cur:
                cur.execute(stmt_copy_data_to_partitioned)
                print(f"Copied {cur.rowcount} rows from {actual_schema}.{tmp_table_name} to {actual_schema}.{new_partitioned_table_name}.")

        print(f"\n--- Data migration to {actual_schema}.{new_partitioned_table_name} complete. ---")

        # Information about FKs that were dropped from other tables and NOT recreated by this script.
        # These might need manual review or recreation depending on application logic.
        if dropped_referencing_fks_info:
            print("\nINFO: The following external Foreign Keys (referencing the original table) were dropped and NOT automatically recreated by this script:")
            for fk_info in dropped_referencing_fks_info:
                print(f"  - FK Name: '{fk_info['name']}' on Table: {fk_info['referencing_schema']}.{fk_info['referencing_table']} (Columns: {fk_info['columns']})")
            print("  These may need to be manually recreated if still required, ensuring they are compatible with the new partitioned table structure.")

        # --- Finalizing Transaction ---
        if not dry_run:
            conn.commit()
            print(f"\nPartitioning process for {actual_schema}.{new_partitioned_table_name} committed successfully.")
        else:
            conn.rollback() # Rollback changes if it was a dry run
            print(f"\n[Dry Run] All changes for {actual_schema}.{new_partitioned_table_name} were simulated and have been rolled back.")

        print(f"The original data (pre-partitioning) remains in the temporary table: {actual_schema}.{tmp_table_name}.")
        print(f"This table can be used for verification or manual rollback if necessary.")

        # --- Step 13: Drop the temporary table ---
        print(f"\n[Step 13] Dropping temporary table {actual_schema}.{tmp_table_name}...")
        stmt_drop_tmp_table = sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
            sql.Identifier(actual_schema), sql.Identifier(tmp_table_name)
        )
        if dry_run:
            print(f"[Dry Run] Would execute: {stmt_drop_tmp_table.as_string(conn)}")
        else:
            # This operation should be outside the main transaction if it was committed.
            # If the main transaction was rolled back (dry_run), this also shouldn't actually drop.
            # For a real run, we need a new transaction or autocommit for this DDL.
            # Simplest is to execute it after commit/rollback of the main work.
            try:
                # Ensure autocommit is True for this isolated DDL operation if not in dry_run
                original_autocommit_state = conn.autocommit
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute(stmt_drop_tmp_table)
                print(f"Successfully dropped temporary table {actual_schema}.{tmp_table_name}.")
            except Exception as e_drop_tmp:
                print(f"Error dropping temporary table {actual_schema}.{tmp_table_name}: {e_drop_tmp}")
                print(f"  You may need to drop it manually: {stmt_drop_tmp_table.as_string(conn)}")
            finally:
                if not dry_run: # Restore autocommit state only if we changed it
                    conn.autocommit = original_autocommit_state


        print(f"\n{'[Dry Run] Simulated' if dry_run else 'Successfully completed'} full partitioning process for {actual_schema}.{new_partitioned_table_name}.")


    except Exception as e:
        # Detailed error reporting including stack trace
        print("\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print(f"ERROR during partitioning process for table '{table_to_partition_name}':")
        print(str(e))
        print("------------------- STACK TRACE -------------------")
        traceback.print_exc()
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n")

        if conn and not conn.closed:
            try:
                conn.rollback() # Attempt to rollback any partial changes from the failed transaction
                print("Transaction rolled back due to error.")
            except Exception as rb_e:
                print(f"Critical: Error during rollback attempt: {rb_e}")
        raise # Re-raise the exception to be caught by the main loop or script runner

# --- Main Program Flow ---
def main(dry_run=False):
    """
    Main function to drive the script.
    Reads configuration, connects to the database, and processes tables for partitioning.
    """
    properties_file = "config/Openbravo.properties" # Path to the properties file
    try:
        print(f"Reading properties from: {properties_file}")
        properties = read_properties_file(properties_file)
        if not properties:
            print("Failed to read properties file or file is empty. Exiting.")
            return

        source_path = properties.get('source.path')
        print(f"Source path from properties: {source_path if source_path else 'Not defined'}")

        # Validate RDBMS type
        if properties.get('bbdd.rdbms') != 'POSTGRE':
            raise ValueError(f"Unsupported RDBMS: {properties.get('bbdd.rdbms')}. This script is designed for POSTGRE only.")

        print("Parsing database connection parameters...")
        db_params = parse_db_params(properties)

        print("Attempting to load table configurations from YAML files (archiving.yaml)...")
        table_fields_config = get_all_tables_and_fields(source_path)
        if not table_fields_config:
            if source_path:
                print("Warning: No tables or partition fields found in any 'archiving.yaml' files under the specified source path, or 'source.path' might be misconfigured for YAML discovery.")
            else:
                print("Warning: 'source.path' not set in properties. Skipping YAML configuration loading for tables. No tables will be processed unless specified by other means (if script is modified).")
        else:
            print(f"Found {len(table_fields_config)} table(s) configured in YAML files for potential partitioning.")

        conn = None # Initialize connection variable
        try:
            print(f"Connecting to PostgreSQL database: Host={db_params['host']}, Port={db_params['port']}, DBName={db_params['database']}")
            conn = psycopg2.connect(**db_params)
            print("Database connection successful.")

            # Apply session configuration if specified
            session_config = properties.get('bbdd.sessionConfig')
            if session_config:
                with conn.cursor() as cur:
                    if dry_run:
                        print(f"[Dry Run] Would apply session configuration: {session_config}")
                    else:
                        print(f"Applying session configuration: {session_config}")
                        cur.execute(session_config)
                        conn.commit() # Commit session config changes
                        print("Session configuration applied.")

            if not table_fields_config:
                print("No tables configured for processing from YAML files. The script will now exit as there is no work to do.")
                return # Exit if no tables are configured

            # Iterate through each table configured in YAML and attempt partitioning
            for table_name, fields in table_fields_config.items():
                if not fields:
                    print(f"\n--- Skipping table: {table_name} (No partition fields specified in YAML) ---")
                    continue

                # Assuming the first field in the set is the partition_by_field
                # Modify this logic if multiple fields can be specified and one needs to be chosen
                partition_field = next(iter(fields))
                print(f"\n\n==================================================================================")
                print(f"=== Processing table: {table_name} (Partition field from YAML: {partition_field}) ===")
                print(f"==================================================================================")
                try:
                    # Call the main partitioning logic for the current table
                    execute_partition_steps(conn, table_name, partition_field, source_path, dry_run)
                except Exception as e_table_processing:
                    # Catch exceptions per table to allow the script to continue with other tables
                    print(f"!!!!!!!! An unrecoverable error occurred while processing table {table_name}: {e_table_processing} !!!!!!!!")
                    print(f"!!!!!!!! Check logs above for details. Attempting to continue with the next table if any. !!!!!!!!")
                    # Ensure connection is still valid or attempt to reset if necessary (advanced)
                    if conn.closed:
                        print("CRITICAL: Database connection was closed during processing. Cannot continue.")
                        raise # Re-raise to stop main if connection is lost
                    else:
                        conn.rollback() # Rollback any transaction for the failed table
                finally:
                    print(f"=== Finished processing for table: {table_name} ===")
                    print(f"==================================================================================\n")

        finally:
            # Ensure the database connection is closed
            if conn and not conn.closed:
                conn.close()
                print("Database connection closed.")
    except ValueError as ve:
        print(f"Configuration Error: {ve}")
    except psycopg2.Error as db_err:
        print(f"Database Error: {db_err}")
        traceback.print_exc()
    except Exception as e_main:
        print(f"An unexpected error occurred in the main script: {e_main}")
        traceback.print_exc()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Automated script to migrate PostgreSQL tables to a range-partitioned structure based on a date/timestamp field. "
                    "This version modifies the Primary Key to include the partition field, drops dependencies without CASCADE, "
                    "migrates data to the new partitioned table, and then drops the temporary original table."
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help="Simulate all DDL and DML operations without actually executing them against the database. "
             "Useful for verifying the script's actions."
    )
    args = parser.parse_args()

    print(f"Script version: PK Modified, No CASCADE, Data Migrated, Temp Table Dropped. Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if args.dry_run:
        print("\n*************************************")
        print("*** DRY RUN MODE ENABLED     ***")
        print("*** No actual changes will be made to the database. ***")
        print("*************************************\n")
    else:
        print("\n*************************************")
        print("*** LIVE RUN MODE ENABLED    ***")
        print("*** Changes WILL be made to the database. Review carefully! ***")
        print("*************************************\n")
        user_confirmation = input("Are you sure you want to proceed with the LIVE RUN? (yes/no): ")
        if user_confirmation.lower() != 'yes':
            print("Live run cancelled by user. Exiting.")
            exit()


    main(dry_run=args.dry_run)
    print("\nScript execution finished.")
