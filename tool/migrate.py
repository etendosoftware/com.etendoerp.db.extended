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

def read_properties_file(file_path):
    """Read a .properties file and return a dictionary of properties."""
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
    """Parse database connection parameters from properties."""
    if not all([properties.get('bbdd.url'), properties.get('bbdd.sid'), properties.get('bbdd.user'), properties.get('bbdd.password')]):
        raise ValueError("Missing required database connection properties")

    url_match = re.match(r'jdbc:postgresql://([^:]+):(\d+)', properties['bbdd.url'])
    if not url_match:
        raise ValueError(f"Invalid bbdd.url format: {properties['bbdd.url']}")

    host, port = url_match.groups()
    return {
        'host': host,
        'port': port,
        'database': properties['bbdd.sid'],
        'user': properties.get('bbdd.user'),
        'password': properties.get('bbdd.password')
    }

def read_yaml_config(file_path):
    """Read a YAML file and return a dictionary with tables and their fields."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f) or {}
            tables = config.get('tables', {})
            return {table: set(fields) for table, fields in tables.items()}
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return {}

def find_table_xml_file(root_path, table_name):
    """Find the XML file for a given table in src-db directories."""
    xml_filename = f"{table_name.upper()}.XML"
    print(f"Searching for XML file: {xml_filename}")
    print(f"Root path: {root_path}")

    if not Path(root_path).is_dir():
        print(f"Error: Root path {root_path} is not a valid directory")
        return None

    base_path = Path(root_path) / 'src-db' / 'database' / 'model' / 'tables' / xml_filename
    if base_path.exists():
        print(f"Found XML file: {base_path}")
        return base_path

    modules_path = Path(root_path) / 'modules'
    try:
        for xml_file in modules_path.rglob(f"*/src-db/database/model/tables/{xml_filename}"):
            if xml_file.exists():
                print(f"Found XML file: {xml_file}")
                return xml_file
    except Exception as e:
        print(f"Error searching recursively in {modules_path}: {e}")

    print(f"XML file {xml_filename} not found")
    return None

def rename_table_xml_file(xml_file, dry_run=False):
    """Rename the XML file to append _PARTITIONED."""
    if not xml_file:
        return False
    try:
        new_name = xml_file.with_name(f"{xml_file.stem}_PARTITIONED.XML")
        if dry_run:
            print(f"[Dry Run] Would rename {xml_file} to {new_name}")
            return True
        xml_file.rename(new_name)
        print(f"Renamed {xml_file} to {new_name}")
        return True
    except Exception as e:
        print(f"Error renaming {xml_file}: {e}")
        return False

def find_module_for_table(root_path, table_name):
    """Find the module containing the table in its archiving.yaml."""
    modules_path = Path(root_path) / 'modules'
    if not modules_path.exists() or not modules_path.is_dir():
        print(f"Error: 'modules' directory not found at {modules_path}")
        return None

    for yaml_file in modules_path.rglob('archiving.yaml'):
        if not yaml_file.exists():
            continue
        table_fields = read_yaml_config(yaml_file)
        if table_name in table_fields:
            print(f"Found module for table {table_name} at {yaml_file.parent}")
            return yaml_file.parent
    print(f"No module found for table {table_name}")
    return None

def update_exclude_tables_xml(module_path, table_name, dry_run=False):
    """Update or create excludeTables.xml to include the table."""
    exclude_file = module_path / 'src-db' / 'database' / 'model' / 'excludeFilter.xml'
    table_name_upper = table_name.upper()

    try:
        # Prepare XML content
        if exclude_file.exists():
            tree = ET.parse(exclude_file)
            root = tree.getroot()
        else:
            root = ET.Element('vector')

        for excluded_table in root.findall('excludedTable'):
            if excluded_table.get('name') == table_name_upper:
                print(f"Table {table_name_upper} already in {exclude_file}, no update needed")
                return True

        excluded_table = ET.SubElement(root, 'excludedTable')
        excluded_table.set('name', table_name_upper)
        xmlstr = minidom.parseString(ET.tostring(root)).toprettyxml(indent="  ")
        xmlstr = '\n'.join(line for line in xmlstr.splitlines() if not line.strip().startswith('<?xml'))

        if dry_run:
            print(f"[Dry Run] Would update {exclude_file} with content:\n{xmlstr}")
            return True

        exclude_file.parent.mkdir(parents=True, exist_ok=True)
        with open(exclude_file, 'w', encoding='utf-8') as f:
            f.write(xmlstr)
        print(f"Updated {exclude_file} with excluded table {table_name_upper}")
        return True
    except Exception as e:
        print(f"Error updating {exclude_file}: {e}")
        return False

def get_all_tables_and_fields(root_path):
    """Traverse subfolders in 'modules', read archiving.yaml files, and combine tables and fields."""
    all_table_fields = defaultdict(set)
    modules_path = Path(root_path) / 'modules'

    if not modules_path.exists() or not modules_path.is_dir():
        print(f"Error: 'modules' directory not found at {modules_path}")
        return {}

    for yaml_file in modules_path.rglob('archiving.yaml'):
        table_fields = read_yaml_config(yaml_file)
        print(f"Read {yaml_file}: {table_fields}")
        for table, fields in table_fields.items():
            all_table_fields[table].update(fields)

    return dict(all_table_fields)

def get_table_schema(conn, table_name, schema='public'):
    """Retrieve the schema (column names and types) of a table for debugging."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
            """, (schema, table_name))
            schema = cur.fetchall()
            print(f"Schema for {schema}.{table_name}:")
            for col_name, col_type in schema:
                print(f"  {col_name}: {col_type}")
            return schema
    except Exception as e:
        print(f"Error retrieving schema for {schema}.{table_name}: {e}")
        return []

def table_exists(conn, table_name):
    """Check if a table exists in any schema and return the schema if found."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_schema
                FROM information_schema.tables
                WHERE table_name = %s
            """, (table_name,))
            result = cur.fetchall()
            if result:
                return result[0][0]
            return None
    except Exception as e:
        print(f"Error checking if table {table_name} exists: {e}")
        return None

def is_table_partitioned(conn, table_name, schema):
    """Check if a table is already partitioned."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_class c
                    JOIN pg_namespace n ON c.relnamespace = n.oid
                    JOIN pg_partitioned_table pt ON c.oid = pt.partrelid
                    WHERE n.nspname = %s
                    AND c.relname = %s
                )
            """, (schema, table_name))
            return cur.fetchone()[0]
    except Exception as e:
        print(f"Error checking if {schema}.{table_name} is partitioned: {e}")
        return False

def list_all_tables(conn):
    """List all tables in the database for debugging."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
            """)
            tables = cur.fetchall()
            return [(row[0], row[1]) for row in tables]
    except Exception as e:
        print(f"Error listing tables: {e}")
        return []

def get_dependent_views(conn, table_name, schema):
    """Find views that depend on the specified table."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT nt.nspname AS schema_name, t.relname AS view_name, pg_get_viewdef(c.oid) AS view_definition
                FROM pg_class c
                JOIN pg_namespace n ON c.relnamespace = n.oid
                JOIN pg_depend d ON c.oid = d.objid
                JOIN pg_class t ON d.refobjid = t.oid
                JOIN pg_namespace nt ON t.relnamespace = nt.oid
                WHERE c.relkind = 'v'
                AND t.relname = %s
                AND nt.nspname = %s
            """, (table_name, schema))
            return cur.fetchall()
    except Exception as e:
        print(f"Error finding dependent views for {schema}.{table_name}: {e}")
        return []

def get_all_dependencies(conn, table_name, schema):
    """Check all dependencies on a table and return their types and names."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT n.nspname AS schema_name, c.relname AS object_name, c.relkind AS object_type
                FROM pg_depend d
                JOIN pg_class c ON d.objid = c.oid
                JOIN pg_namespace n ON c.relnamespace = n.oid
                JOIN pg_class t ON d.refobjid = t.oid
                JOIN pg_namespace nt ON t.relnamespace = nt.oid
                WHERE t.relname = %s
                AND nt.nspname = %s
                AND c.relkind IN ('v', 'r', 'i')
            """, (table_name, schema))
            dependencies = cur.fetchall()

            cur.execute("""
                SELECT n.nspname AS schema_name, tg.tgname AS object_name, 't' AS object_type
                FROM pg_trigger tg
                JOIN pg_class t ON tg.tgrelid = t.oid
                JOIN pg_namespace n ON t.relnamespace = n.oid
                WHERE t.relname = %s
                AND n.nspname = %s
            """, (table_name, schema))
            trigger_deps = cur.fetchall()

            all_deps = dependencies + trigger_deps
            print(f"Dependencies for {schema}.{table_name}:")
            if not all_deps:
                print("  (none)")
            for dep in all_deps:
                object_type = {'v': 'view', 'r': 'table', 'i': 'index', 't': 'trigger'}.get(dep[2], dep[2])
                print(f"  {dep[0]}.{dep[1]} ({object_type})")
            return all_deps
    except Exception as e:
        print(f"Error checking dependencies for {schema}.{table_name}: {e}")
        return []

def get_triggers(conn, table_name, schema):
    """Get triggers on a table."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT n.nspname AS schema_name, tg.tgname AS trigger_name
                FROM pg_trigger tg
                JOIN pg_class t ON tg.tgrelid = t.oid
                JOIN pg_namespace n ON t.relnamespace = n.oid
                WHERE t.relname = %s
                AND n.nspname = %s
            """, (table_name, schema))
            return cur.fetchall()
    except Exception as e:
        print(f"Error finding triggers for {schema}.{table_name}: {e}")
        return []

def get_foreign_key_constraints(conn, table_name, schema):
    """Retrieve foreign key constraints for a table."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    tc.constraint_name,
                    tc.table_name,
                    kcu.column_name,
                    ccu.table_name AS referenced_table,
                    ccu.column_name AS referenced_column
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage ccu
                    ON tc.constraint_name = ccu.constraint_name
                    AND tc.table_schema = ccu.table_schema
                WHERE tc.table_schema = %s
                    AND tc.table_name = %s
                    AND tc.constraint_type = 'FOREIGN KEY'
            """, (schema, table_name))
            constraints = cur.fetchall()
            return [
                {
                    'constraint_name': row[0],
                    'table_name': row[1],
                    'column_name': row[2],
                    'referenced_table': row[3],
                    'referenced_column': row[4]
                }
                for row in constraints
            ]
    except Exception as e:
        print(f"Error retrieving foreign key constraints for {schema}.{table_name}: {e}")
        return []

def drop_foreign_key_constraints(conn, table_name, schema, dry_run=False):
    """Drop all foreign key constraints on a table."""
    try:
        constraints = get_foreign_key_constraints(conn, table_name, schema)
        if not constraints:
            print(f"No foreign key constraints found on {schema}.{table_name}")
            return []

        dropped_constraints = []
        for constraint in constraints:
            constraint_name = constraint['constraint_name']
            sql_stmt = sql.SQL("ALTER TABLE {}.{} DROP CONSTRAINT {}").format(
                sql.Identifier(schema),
                sql.Identifier(table_name),
                sql.Identifier(constraint_name)
            )
            if dry_run:
                print(f"[Dry Run] Would execute: {sql_stmt.as_string(conn)}")
                dropped_constraints.append(constraint)
            else:
                with conn.cursor() as cur:
                    cur.execute(sql_stmt)
                    print(f"Dropped foreign key constraint {constraint_name} on {schema}.{table_name}")
                    dropped_constraints.append(constraint)
        return dropped_constraints
    except Exception as e:
        print(f"Error dropping foreign key constraints for {schema}.{table_name}: {e}")
        raise

def recreate_foreign_key_constraints(conn, table_name, schema, constraints, dry_run=False):
    """Recreate foreign key constraints on a table."""
    try:
        for constraint in constraints:
            constraint_name = constraint['constraint_name']
            column_name = constraint['column_name']
            referenced_table = constraint['referenced_table']
            referenced_column = constraint['referenced_column']
            sql_stmt = sql.SQL("""
                ALTER TABLE {}.{}
                ADD CONSTRAINT {} FOREIGN KEY ({})
                REFERENCES {}.{} ({})
            """).format(
                sql.Identifier(schema),
                sql.Identifier(table_name),
                sql.Identifier(constraint_name),
                sql.Identifier(column_name),
                sql.Identifier(schema),
                sql.Identifier(referenced_table),
                sql.Identifier(referenced_column)
            )
            if dry_run:
                print(f"[Dry Run] Would execute: {sql_stmt.as_string(conn)}")
            else:
                with conn.cursor() as cur:
                    cur.execute(sql_stmt)
                    print(f"Recreated foreign key constraint {constraint_name} on {schema}.{table_name}")
    except Exception as e:
        print(f"Error recreating foreign key constraints for {schema}.{table_name}: {e}")
        raise

def drop_triggers(conn, table_name, schema, dry_run=False):
    """Drop all non-system triggers on a table."""
    try:
        triggers = get_triggers(conn, table_name, schema)
        dropped_count = 0
        for trigger_schema, trigger_name in triggers:
            if trigger_name.startswith('RI_ConstraintTrigger'):
                print(f"Skipping system trigger {trigger_schema}.{trigger_name} on {schema}.{table_name}")
                continue
            sql_stmt = sql.SQL("DROP TRIGGER {} ON {}.{}").format(
                sql.Identifier(trigger_name),
                sql.Identifier(schema),
                sql.Identifier(table_name)
            )
            if dry_run:
                print(f"[Dry Run] Would execute: {sql_stmt.as_string(conn)}")
            else:
                with conn.cursor() as cur:
                    cur.execute(sql_stmt)
                    print(f"Dropped trigger {trigger_schema}.{trigger_name} on {schema}.{table_name}")
                    dropped_count += 1
        return dropped_count
    except Exception as e:
        print(f"Error dropping triggers for {schema}.{table_name}: {e}")
        raise

def get_year_range(conn, table_name, schema, partition_field):
    """Determine the min and max years for partitioning based on the partition field."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("""
                    SELECT EXTRACT(YEAR FROM MIN({}))::int, EXTRACT(YEAR FROM MAX({}))::int
                    FROM {}.{}
                """).format(
                    sql.Identifier(partition_field),
                    sql.Identifier(partition_field),
                    sql.Identifier(schema),
                    sql.Identifier(table_name)
                )
            )
            min_year, max_year = cur.fetchone()
            return min_year or 2020, max_year or 2025
    except Exception as e:
        print(f"Error determining year range for {schema}.{table_name}: {e}")
        return 2020, 2025

def validate_partition_field(conn, table_name, schema, partition_field):
    """Validate that the partition field is a timestamp type."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT data_type
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s AND column_name = %s
            """, (schema, table_name, partition_field))
            result = cur.fetchone()
            if not result:
                raise Exception(f"Partition field {partition_field} not found in {schema}.{table_name}")
            data_type = result[0]
            if 'timestamp' not in data_type.lower():
                raise Exception(f"Partition field {partition_field} must be a timestamp type, found {data_type}")
    except Exception as e:
        print(f"Error validating partition field for {schema}.{table_name}: {e}")
        raise

def get_primary_key_info(conn, table_name, schema):
    """Retrieve the primary key constraint name and columns for a table."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    tc.constraint_name,
                    array_agg(kcu.column_name) AS columns
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                WHERE tc.table_schema = %s
                    AND tc.table_name = %s
                    AND tc.constraint_type = 'PRIMARY KEY'
                GROUP BY tc.constraint_name
            """, (schema, table_name))
            result = cur.fetchone()
            if result:
                constraint_name, columns = result
                if isinstance(columns, str):
                    columns = [col.strip() for col in columns.strip('{}').split(',') if col.strip()]
                elif not isinstance(columns, list):
                    columns = list(columns)
                return {'name': constraint_name, 'columns': columns}
            return {'name': None, 'columns': []}
    except Exception as e:
        print(f"Error retrieving primary key info for {schema}.{table_name}: {e}")
        return {'name': None, 'columns': []}

def execute_partition_steps(conn, table_name, partition_field, schema='public', root_path=None, dry_run=False):
    """Execute the partitioning steps for the given table and field in the specified schema."""
    try:
        # Step 0: Check if the table is already partitioned
        table_schema = table_exists(conn, table_name)
        if not table_schema:
            all_tables = list_all_tables(conn)
            print(f"Available tables in the database:")
            for sch, tbl in all_tables:
                print(f"  {sch}.{tbl}")
            raise Exception(f"Table {table_name} does not exist in any schema of the database")

        schema = table_schema
        if is_table_partitioned(conn, table_name, schema):
            print(f"Table {schema}.{table_name} is already partitioned, skipping partitioning steps")
            return

        # Step 1: Log schema for debugging
        get_table_schema(conn, table_name, schema)

        # Step 2: Validate partition field
        validate_partition_field(conn, table_name, schema, partition_field)

        # Step 3: Get primary key information
        pk_info = get_primary_key_info(conn, table_name, schema)
        original_pk_name = pk_info['name']
        original_pk_columns = pk_info['columns']
        print(f"Original primary key: {original_pk_name} on columns {original_pk_columns} (type: {type(original_pk_columns)})")

        # Step 4: Get and drop dependent views before renaming
        dependent_views = get_dependent_views(conn, table_name, schema)
        for view_schema, view_name, _ in dependent_views:
            print(f"Found dependent view {view_schema}.{view_name}")
            sql_stmt = sql.SQL("DROP VIEW {}.{}").format(
                sql.Identifier(view_schema),
                sql.Identifier(view_name)
            )
            if dry_run:
                print(f"[Dry Run] Would execute: {sql_stmt.as_string(conn)}")
            else:
                with conn.cursor() as cur:
                    cur.execute(sql_stmt)
                    print(f"Dropped view {view_schema}.{view_name}")

        # Step 5: Drop foreign key constraints
        dropped_constraints = drop_foreign_key_constraints(conn, table_name, schema, dry_run)

        # Step 6: Drop non-system triggers before renaming
        drop_triggers(conn, table_name, schema, dry_run)

        # Step 7: Rename table to tmp
        sql_stmt = sql.SQL("ALTER TABLE {}.{} RENAME TO {}").format(
            sql.Identifier(schema),
            sql.Identifier(table_name),
            sql.Identifier(f"{table_name}_tmp")
        )
        if dry_run:
            print(f"[Dry Run] Would execute: {sql_stmt.as_string(conn)}")
        else:
            with conn.cursor() as cur:
                cur.execute(sql_stmt)
                print(f"Renamed {schema}.{table_name} to {schema}.{table_name}_tmp")

        # Step 8: Create the new partitioned table
        sql_stmt = sql.SQL("""
            CREATE TABLE {}.{} (
                LIKE {}.{} INCLUDING DEFAULTS
            ) PARTITION BY RANGE ({})
        """).format(
            sql.Identifier(schema),
            sql.Identifier(table_name),
            sql.Identifier(schema),
            sql.Identifier(f"{table_name}_tmp"),
            sql.Identifier(partition_field)
        )
        if dry_run:
            print(f"[Dry Run] Would execute: {sql_stmt.as_string(conn)}")
        else:
            with conn.cursor() as cur:
                cur.execute(sql_stmt)
                print(f"Created partitioned table {schema}.{table_name} with schema from {schema}.{table_name}_tmp")

        # Step 9: Create schema if not exists
        sql_stmt = sql.SQL("CREATE SCHEMA IF NOT EXISTS partitions")
        if dry_run:
            print(f"[Dry Run] Would execute: {sql_stmt.as_string(conn)}")
        else:
            with conn.cursor() as cur:
                cur.execute(sql_stmt)
                print("Created schema 'partitions' if it did not exist")

        # Step 10: Determine dynamic year range
        start_year, end_year = get_year_range(conn, f"{table_name}_tmp" if dry_run else table_name, schema, partition_field)
        print(f"Partitioning for years {start_year} to {end_year}")

        # Step 11: Create partitions for each year
        for year in range(start_year, end_year + 1):
            partition_name = f"{table_name}_y{year}"
            sql_stmt = sql.SQL("""
                CREATE TABLE {}.{} PARTITION OF {}.{}
                FOR VALUES FROM (%s) TO (%s)
            """).format(
                sql.Identifier("partitions"),
                sql.Identifier(partition_name),
                sql.Identifier(schema),
                sql.Identifier(table_name),
            )
            if dry_run:
                print(f"[Dry Run] Would execute: {sql_stmt.as_string(conn)} with values ({year}-01-01, {year + 1}-01-01)")
            else:
                with conn.cursor() as cur:
                    cur.execute(sql_stmt, (f"{year}-01-01", f"{year + 1}-01-01"))
                    print(f"Created partition partitions.{partition_name} for year {year}")

        # Step 12: Copy data
        sql_stmt = sql.SQL("""
            INSERT INTO {}.{} SELECT * FROM {}.{}
        """).format(
            sql.Identifier(schema),
            sql.Identifier(table_name),
            sql.Identifier(schema),
            sql.Identifier(f"{table_name}_tmp")
        )
        if dry_run:
            print(f"[Dry Run] Would execute: {sql_stmt.as_string(conn)}")
        else:
            with conn.cursor() as cur:
                cur.execute(sql_stmt)
                print(f"Copied data from {schema}.{table_name}_tmp to {schema}.{table_name}")

        # Step 13: Check dependencies before dropping
        dependencies = get_all_dependencies(conn, f"{table_name}_tmp" if dry_run else table_name, schema)
        if dependencies:
            non_valid_deps = [dep for dep in dependencies if dep[2] not in ('v', 'i', 't')]
            if non_valid_deps:
                print(f"Cannot drop {schema}.{table_name}_tmp: non-valid dependencies found:")
                for dep in non_valid_deps:
                    print(f"  {dep[0]}.{dep[1]} ({dep[2]})")
                raise Exception(f"Non-valid dependencies prevent dropping {schema}.{table_name}_tmp")
            else:
                print(f"Only views, indexes, or triggers found, proceeding with DROP TABLE ... CASCADE")
                sql_stmt = sql.SQL("DROP TABLE {}.{} CASCADE").format(
                    sql.Identifier(schema),
                    sql.Identifier(f"{table_name}_tmp")
                )
                if dry_run:
                    print(f"[Dry Run] Would execute: {sql_stmt.as_string(conn)}")
                    for dep in dependencies:
                        object_type = {'v': 'view', 'i': 'index', 't': 'trigger'}.get(dep[2], dep[2])
                        print(f"[Dry Run] Would drop dependent {object_type} {dep[0]}.{dep[1]}")
                else:
                    with conn.cursor() as cur:
                        cur.execute(sql_stmt)
                        print(f"Dropped temporary table {schema}.{table_name}_tmp with CASCADE")
                        for dep in dependencies:
                            object_type = {'v': 'view', 'i': 'index', 't': 'trigger'}.get(dep[2], dep[2])
                            print(f"Dropped dependent {object_type} {dep[0]}.{dep[1]}")
        else:
            sql_stmt = sql.SQL("DROP TABLE {}.{}").format(
                sql.Identifier(schema),
                sql.Identifier(f"{table_name}_tmp")
            )
            if dry_run:
                print(f"[Dry Run] Would execute: {sql_stmt.as_string(conn)}")
            else:
                with conn.cursor() as cur:
                    cur.execute(sql_stmt)
                    print(f"Dropped temporary table {schema}.{table_name}_tmp (no dependencies)")

        # Step 14: Create the new primary key
        if original_pk_columns:
            new_pk_columns = original_pk_columns + [partition_field]
            print(f"Preparing to create primary key with columns: {new_pk_columns} (type: {type(new_pk_columns)})")
            try:
                if original_pk_name:
                    sql_stmt = sql.SQL("ALTER TABLE {}.{} ADD CONSTRAINT {} PRIMARY KEY ({})").format(
                        sql.Identifier(schema),
                        sql.Identifier(table_name),
                        sql.Identifier(original_pk_name),
                        sql.SQL(', ').join(sql.Identifier(col) for col in new_pk_columns)
                    )
                else:
                    new_pk_name = f"{table_name}_pkey_{uuid.uuid4().hex[:8]}"
                    sql_stmt = sql.SQL("ALTER TABLE {}.{} ADD CONSTRAINT {} PRIMARY KEY ({})").format(
                        sql.Identifier(schema),
                        sql.Identifier(table_name),
                        sql.Identifier(new_pk_name),
                        sql.SQL(', ').join(sql.Identifier(col) for col in new_pk_columns)
                    )
                if dry_run:
                    print(f"[Dry Run] Would execute: {sql_stmt.as_string(conn)}")
                else:
                    with conn.cursor() as cur:
                        cur.execute(sql_stmt)
                        print(f"Created primary key {original_pk_name or new_pk_name} on {schema}.{table_name} with columns {new_pk_columns}")
            except Exception as e:
                print(f"Failed to create primary key on {schema}.{table_name}: {e}")
                raise

        # Step 15: Recreate foreign key constraints
        if dropped_constraints:
            recreate_foreign_key_constraints(conn, table_name, schema, dropped_constraints, dry_run)

        # Step 16: Rename the table's XML file
        if root_path:
            try:
                xml_file = find_table_xml_file(root_path, table_name)
                if xml_file:
                    rename_table_xml_file(xml_file, dry_run)
                else:
                    print(f"Warning: XML file for table {table_name} not found in src-db directories")
            except Exception as e:
                print(f"Warning: Failed to process XML file for {table_name}: {e}")

        # Step 17: Update excludeTables.xml in the module
        if root_path:
            try:
                module_path = find_module_for_table(root_path, table_name)
                if module_path:
                    update_exclude_tables_xml(module_path, table_name, dry_run)
                else:
                    print(f"Warning: Module containing {table_name} in archiving.yaml not found")
            except Exception as e:
                print(f"Warning: Failed to update excludeTables.xml for {table_name}: {e}")

        if not dry_run:
            conn.commit()
        else:
            print(f"[Dry Run] Skipped committing changes for {schema}.{table_name}")
    except Exception as e:
        print(f"Error executing partition steps: {e}")
        if not dry_run:
            conn.rollback()
        else:
            print(f"[Dry Run] Skipped rolling back changes for {schema}.{table_name}")

def main(dry_run=False):
    properties_file = "config/Openbravo.properties"

    try:
        properties = read_properties_file(properties_file)
        if not properties['source.path']:
            print(f"Error: Could not read 'source.path' from {properties_file}")
            return

        if properties['bbdd.rdbms'] != 'POSTGRE':
            print(f"Error: Only PostgreSQL is supported, found {properties['bbdd.rdbms']}")
            return

        db_params = parse_db_params(properties)
        table_fields = get_all_tables_and_fields(properties['source.path'])
        if not table_fields:
            print("No tables found or an error occurred.")
            return

        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cur:
                if properties['bbdd.sessionConfig']:
                    if dry_run:
                        print(f"[Dry Run] Would execute session configuration: {properties['bbdd.sessionConfig']}")
                    else:
                        cur.execute(properties['bbdd.sessionConfig'])
                        print("Applied session configuration")
                if not dry_run:
                    conn.commit()

            for table_name, fields in table_fields.items():
                if not fields:
                    print(f"Skipping table {table_name}: No partition fields defined")
                    continue
                partition_field = next(iter(fields))
                print(f"Processing table {table_name} with partition field {partition_field}")
                execute_partition_steps(
                    conn,
                    table_name,
                    partition_field,
                    root_path=properties['source.path'],
                    dry_run=dry_run
                )

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Partition tables based on configuration.")
    parser.add_argument('--dry-run', action='store_true', help="Run in dry-run mode without making changes")
    args = parser.parse_args()
    main(dry_run=args.dry_run)
