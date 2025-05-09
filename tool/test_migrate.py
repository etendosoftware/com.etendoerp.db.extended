import unittest
from unittest.mock import patch, mock_open, MagicMock, call, ANY
import os
import yaml # Para errores de YAML y para mockear safe_load
from collections import defaultdict
from pathlib import Path
from configparser import ConfigParser, NoSectionError, NoOptionError
import psycopg2 # Para mockear errores y el objeto connection/cursor
from psycopg2 import sql
from datetime import datetime
import argparse

# Importa el script que quieres testear
# Asegúrate de que migrate.py esté en el PYTHONPATH
import migrate as dp

class TestStyleAndPrintMessage(unittest.TestCase):

    def setUp(self):
        # Guardar el estado original de _use_ansi_output y restaurarlo después de cada test
        self.original_ansi_state = dp._use_ansi_output

    def tearDown(self):
        dp._use_ansi_output = self.original_ansi_state

    @patch('builtins.print')
    def test_print_message_ansi_enabled(self, mock_print):
        dp._use_ansi_output = True
        dp.print_message("Test Success", "SUCCESS")
        mock_print.assert_called_once_with(f"{dp.Style.GREEN}{dp.Style.BOLD}{dp.Style.SUCCESS} Test Success{dp.Style.RESET}", end="\n")

        mock_print.reset_mock()
        dp.print_message("Test Header", "HEADER")
        mock_print.assert_called_once_with(f"{dp.Style.MAGENTA}{dp.Style.BOLD}{dp.Style.HEADER} Test Header{dp.Style.RESET}", end="\n")

    @patch('builtins.print')
    def test_print_message_ansi_disabled(self, mock_print):
        dp._use_ansi_output = False
        dp.print_message("Test Info Plain", "INFO")
        mock_print.assert_called_once_with("Test Info Plain", end="\n")

class TestReadPropertiesFile(unittest.TestCase):

    @patch('builtins.open', new_callable=mock_open, read_data="source.path=/opt/app\nbbdd.rdbms=POSTGRE\nbbdd.url=jdbc:postgresql://localhost:5432/mydb\nbbdd.sid=mydb\nbbdd.user=testuser\nbbdd.password=testpass\nbbdd.sessionConfig=SET work_mem='1GB'")
    # Mockear ConfigParser para controlar lo que devuelve get
    @patch('migrate.ConfigParser')
    def test_read_properties_file_success(self, MockConfigParser, mock_file):
        # Configurar el mock de ConfigParser
        mock_config_instance = MockConfigParser.return_value

        # Simular el comportamiento de config.get()
        def mock_get(section, key, fallback=None):
            if section == 'default':
                if key == 'source.path': return '/opt/app'
                if key == 'bbdd.rdbms': return 'POSTGRE'
                if key == 'bbdd.url': return 'jdbc:postgresql://localhost:5432/mydb'
                if key == 'bbdd.sid': return 'mydb'
                if key == 'bbdd.user': return 'testuser'
                if key == 'bbdd.password': return 'testpass'
                if key == 'bbdd.sessionConfig': return "SET work_mem='1GB'"
            return fallback
        mock_config_instance.get.side_effect = mock_get

        props = dp.read_properties_file("dummy.properties")

        mock_file.assert_called_with("dummy.properties", 'r', encoding='utf-8')
        mock_config_instance.read_string.assert_called_once_with(
            "[default]\nsource.path=/opt/app\nbbdd.rdbms=POSTGRE\nbbdd.url=jdbc:postgresql://localhost:5432/mydb\nbbdd.sid=mydb\nbbdd.user=testuser\nbbdd.password=testpass\nbbdd.sessionConfig=SET work_mem='1GB'"
        )
        self.assertEqual(props['source.path'], '/opt/app')
        self.assertEqual(props['bbdd.rdbms'], 'POSTGRE')
        self.assertEqual(props['bbdd.url'], 'jdbc:postgresql://localhost:5432/mydb')
        self.assertEqual(props['bbdd.sid'], 'mydb')

    @patch('builtins.open', side_effect=FileNotFoundError("File not found"))
    @patch('migrate.print_message')
    def test_read_properties_file_not_found(self, mock_print_message, mock_file_open):
        props = dp.read_properties_file("nonexistent.properties")
        self.assertEqual(props, {})
        mock_print_message.assert_called_with("Error reading nonexistent.properties: File not found", "ERROR")

    @patch('builtins.open', new_callable=mock_open, read_data="invalid content")
    @patch('migrate.ConfigParser')
    @patch('migrate.print_message')
    def test_read_properties_file_parse_error(self, mock_print_message, MockConfigParser, mock_file_open):
        # Simular un error durante config.read_string() o config.get()
        MockConfigParser.return_value.read_string.side_effect = Exception("Config parse error")
        props = dp.read_properties_file("invalid.properties")
        self.assertEqual(props, {})
        mock_print_message.assert_called_with("Error reading invalid.properties: Config parse error", "ERROR")

class TestParseDbParams(unittest.TestCase):
    def test_parse_db_params_success(self):
        properties = {
            'bbdd.url': 'jdbc:postgresql://myhost:1234',
            'bbdd.sid': 'testdb',
            'bbdd.user': 'testuser',
            'bbdd.password': 'testpass'
        }
        params = dp.parse_db_params(properties)
        self.assertEqual(params, {'host': 'myhost', 'port': '1234', 'database': 'testdb', 'user': 'testuser', 'password': 'testpass'})

    def test_parse_db_params_missing_key(self):
        properties = {
            'bbdd.url': 'jdbc:postgresql://myhost:1234',
            # 'bbdd.sid': 'testdb', # Missing
            'bbdd.user': 'testuser',
            'bbdd.password': 'testpass'
        }
        with self.assertRaisesRegex(ValueError, "Missing required database connection properties"):
            dp.parse_db_params(properties)

    def test_parse_db_params_invalid_url(self):
        properties = {
            'bbdd.url': 'jdbc:mysql://myhost:1234', # Invalid prefix
            'bbdd.sid': 'testdb',
            'bbdd.user': 'testuser',
            'bbdd.password': 'testpass'
        }
        with self.assertRaisesRegex(ValueError, "Invalid bbdd.url format"):
            dp.parse_db_params(properties)

class TestReadYamlConfig(unittest.TestCase):
    @patch('builtins.open', new_callable=mock_open, read_data="tables:\n  table1:\n    - field1\n    - field2\n  table2:\n    - fieldA")
    @patch('yaml.safe_load')
    def test_read_yaml_config_success(self, mock_yaml_load, mock_file):
        mock_yaml_load.return_value = {'tables': {'table1': ['field1', 'field2'], 'table2': ['fieldA']}}
        config = dp.read_yaml_config("dummy.yaml")
        self.assertEqual(config, {'table1': {'field1', 'field2'}, 'table2': {'fieldA'}})
        mock_file.assert_called_with("dummy.yaml", 'r', encoding='utf-8')

    @patch('builtins.open', new_callable=mock_open, read_data="tables:") # Empty tables node
    @patch('yaml.safe_load')
    def test_read_yaml_config_empty_tables_node(self, mock_yaml_load, mock_file):
        mock_yaml_load.return_value = {'tables': {}} # YAML parser returns empty dict for "tables:"
        config = dp.read_yaml_config("empty_tables.yaml")
        self.assertEqual(config, {})

    @patch('builtins.open', new_callable=mock_open, read_data="") # Empty file
    @patch('yaml.safe_load')
    def test_read_yaml_config_empty_file_results_in_none(self, mock_yaml_load, mock_file):
        mock_yaml_load.return_value = None # yaml.safe_load returns None for empty file
        config = dp.read_yaml_config("empty.yaml")
        self.assertEqual(config, {}) # Function should handle None by returning {}

    @patch('builtins.open', side_effect=FileNotFoundError("YAML not found"))
    @patch('migrate.print_message')
    def test_read_yaml_config_file_not_found(self, mock_print_message, mock_file_open):
        config = dp.read_yaml_config("nonexistent.yaml")
        self.assertEqual(config, {})
        mock_print_message.assert_called_with("Error reading YAML configuration from nonexistent.yaml: YAML not found", "ERROR")

    @patch('builtins.open', new_callable=mock_open, read_data="invalid: yaml: content")
    @patch('yaml.safe_load', side_effect=yaml.YAMLError("Bad YAML"))
    @patch('migrate.print_message')
    def test_read_yaml_config_invalid_yaml(self, mock_print_message, mock_yaml_load, mock_file):
        config = dp.read_yaml_config("invalid.yaml")
        self.assertEqual(config, {})
        mock_print_message.assert_called_with("Error reading YAML configuration from invalid.yaml: Bad YAML", "ERROR")


class TestGetAllTablesAndFields(unittest.TestCase):
    @patch('migrate.Path')
    @patch('migrate.read_yaml_config')
    @patch('migrate.print_message')
    def test_success_multiple_files(self, mock_print_message, mock_read_yaml, MockPath):
        # Setup Path mock
        mock_root_path_obj = MagicMock(spec=Path)
        mock_modules_path_obj = MagicMock(spec=Path)
        mock_yaml_file1_obj = MagicMock(spec=Path, name="file1.yaml") # Add name for better debug
        mock_yaml_file1_obj.__str__.return_value = "/fake/root/modules/modA/archiving.yaml"
        mock_yaml_file2_obj = MagicMock(spec=Path, name="file2.yaml")
        mock_yaml_file2_obj.__str__.return_value = "/fake/root/modules/modB/archiving.yaml"

        MockPath.return_value = mock_root_path_obj # When Path(root_path) is called
        mock_root_path_obj.__truediv__.return_value = mock_modules_path_obj # For root_path / 'modules'

        mock_modules_path_obj.exists.return_value = True
        mock_modules_path_obj.is_dir.return_value = True
        mock_modules_path_obj.rglob.return_value = [mock_yaml_file1_obj, mock_yaml_file2_obj]

        # Simulate return values from read_yaml_config
        mock_read_yaml.side_effect = [
            {'TABLE_A': {'field1', 'field2'}},  # Corresponds to mock_yaml_file1_obj
            {'TABLE_B': {'field3'}, 'TABLE_A': {'field4'}}  # Corresponds to mock_yaml_file2_obj
        ]

        result = dp.get_all_tables_and_fields("/fake/root")
        expected = {
            'TABLE_A': {'field1', 'field2', 'field4'},
            'TABLE_B': {'field3'}
        }
        self.assertEqual(result, expected)
        mock_read_yaml.assert_has_calls([call(mock_yaml_file1_obj), call(mock_yaml_file2_obj)])
        mock_print_message.assert_any_call(f"Reading archiving config from: {mock_yaml_file1_obj}", "CONFIG_LOAD")

    @patch('migrate.print_message')
    def test_no_root_path(self, mock_print_message):
        result = dp.get_all_tables_and_fields(None)
        self.assertEqual(result, {})
        mock_print_message.assert_called_with("Warning: 'source.path' is undefined. Cannot get tables and fields from YAML files.", "WARNING")

    @patch('migrate.Path')
    @patch('migrate.print_message')
    def test_modules_dir_not_found(self, mock_print_message, MockPath):
        mock_root_path_obj = MagicMock(spec=Path)
        mock_modules_path_obj = MagicMock(spec=Path)
        MockPath.return_value = mock_root_path_obj
        mock_root_path_obj.__truediv__.return_value = mock_modules_path_obj
        mock_modules_path_obj.exists.return_value = False # Modules dir does not exist

        result = dp.get_all_tables_and_fields("/fake/root")
        self.assertEqual(result, {})
        mock_print_message.assert_any_call(f"Warning: 'modules' directory not found at {mock_modules_path_obj}. Cannot read archiving.yaml files.", "WARNING")


class TestDatabaseInteractionFunctions(unittest.TestCase):
    def setUp(self):
        self.mock_conn = MagicMock(spec=psycopg2.extensions.connection)
        self.mock_cursor = MagicMock(spec=psycopg2.extensions.cursor)
        self.mock_conn.cursor.return_value.__enter__.return_value = self.mock_cursor
        # Para sql.SQL().as_string(conn)
        self.mock_conn.encoding = "utf-8" # psycopg2 connection has an encoding attribute

    @patch('migrate.print_message')
    def test_get_table_schema_success(self, mock_print_message):
        self.mock_cursor.fetchall.return_value = [
            ('id', 'integer', 'NO', None), ('name', 'text', 'YES', 'default_val')
        ]
        schema = dp.get_table_schema(self.mock_conn, "my_table", "public")
        self.mock_cursor.execute.assert_called_with(
            "SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
            ("public", "my_table")
        )
        self.assertEqual(len(schema), 2)
        self.assertEqual(schema[0][0], 'id')
        mock_print_message.assert_any_call("Schema for public.my_table:", "INFO")

    @patch('migrate.print_message')
    def test_get_table_schema_db_error(self, mock_print_message):
        self.mock_cursor.execute.side_effect = psycopg2.Error("Database query failed")
        schema = dp.get_table_schema(self.mock_conn, "my_table", "public")
        self.assertEqual(schema, [])
        mock_print_message.assert_called_with("Error retrieving schema for public.my_table: Database query failed", "ERROR")

    def test_table_exists_true_with_schema(self):
        self.mock_cursor.fetchone.return_value = ("public",)
        result = dp.table_exists(self.mock_conn, "my_table", "public")
        self.assertEqual(result, "public")
        self.mock_cursor.execute.assert_called_with(
            "SELECT table_schema FROM information_schema.tables WHERE table_name = %s AND table_schema = %s",
            ("my_table", "public")
        )

    def test_table_exists_true_without_schema(self):
        self.mock_cursor.fetchone.return_value = ("some_schema",)
        result = dp.table_exists(self.mock_conn, "my_table")
        self.assertEqual(result, "some_schema")
        self.mock_cursor.execute.assert_called_with(
            "SELECT table_schema FROM information_schema.tables WHERE table_name = %s AND table_schema NOT IN ('pg_catalog', 'information_schema')",
            ("my_table",)
        )

    def test_table_exists_false(self):
        self.mock_cursor.fetchone.return_value = None
        result = dp.table_exists(self.mock_conn, "non_existent_table", "public")
        self.assertIsNone(result)

    @patch('migrate.print_message')
    def test_is_table_partitioned_true(self, mock_print_message):
        self.mock_cursor.fetchone.return_value = ('p',) # 'p' for partitioned table
        self.assertTrue(dp.is_table_partitioned(self.mock_conn, "part_table", "public"))

    @patch('migrate.print_message')
    def test_is_table_partitioned_false(self, mock_print_message):
        self.mock_cursor.fetchone.return_value = ('r',) # 'r' for regular table
        self.assertFalse(dp.is_table_partitioned(self.mock_conn, "reg_table", "public"))

    @patch('migrate.print_message')
    def test_validate_partition_field_success_timestamp(self, mock_print_message):
        self.mock_cursor.fetchone.return_value = ('timestamp without time zone',)
        dp.validate_partition_field(self.mock_conn, "my_table", "public", "created_at")
        mock_print_message.assert_any_call(
            "Validation successful: Partition field 'created_at' in public.my_table is type timestamp without time zone.", "SUCCESS"
        )

    @patch('migrate.print_message')
    def test_validate_partition_field_success_date(self, mock_print_message):
        self.mock_cursor.fetchone.return_value = ('date',)
        dp.validate_partition_field(self.mock_conn, "my_table", "public", "event_date")
        mock_print_message.assert_any_call(
            "Validation successful: Partition field 'event_date' in public.my_table is type date.", "SUCCESS"
        )

    @patch('migrate.print_message')
    def test_validate_partition_field_not_found(self, mock_print_message):
        self.mock_cursor.fetchone.return_value = None
        with self.assertRaisesRegex(Exception, "Partition field 'non_field' not found"):
            dp.validate_partition_field(self.mock_conn, "my_table", "public", "non_field")
        mock_print_message.assert_any_call(
            "Error validating partition field for public.my_table: Partition field 'non_field' not found in public.my_table", "ERROR")

    @patch('migrate.print_message')
    def test_validate_partition_field_invalid_type(self, mock_print_message):
        self.mock_cursor.fetchone.return_value = ('integer',)
        with self.assertRaisesRegex(Exception, "must be timestamp or date, found integer"):
            dp.validate_partition_field(self.mock_conn, "my_table", "public", "id_field")

    @patch('migrate.print_message')
    def test_get_primary_key_info_found(self, mock_print_message):
        self.mock_cursor.fetchone.return_value = ('my_pk', ['id_col', 'type_col'])
        pk_info = dp.get_primary_key_info(self.mock_conn, "test_table", "public")
        self.assertEqual(pk_info, {'name': 'my_pk', 'columns': ['id_col', 'type_col']})

    @patch('migrate.print_message')
    def test_get_primary_key_info_not_found(self, mock_print_message):
        self.mock_cursor.fetchone.return_value = None
        pk_info = dp.get_primary_key_info(self.mock_conn, "test_table_no_pk", "public")
        self.assertEqual(pk_info, {'name': None, 'columns': []})

    @patch('migrate.datetime')
    @patch('migrate.print_message')
    def test_get_year_range_success(self, mock_print_message, mock_datetime):
        mock_datetime.now.return_value.year = 2023
        self.mock_cursor.fetchone.return_value = (2020, 2022)
        min_year, max_year = dp.get_year_range(self.mock_conn, "my_table", "public", "date_col")
        self.assertEqual(min_year, 2020)
        self.assertEqual(max_year, 2022)
        # Check the SQL query construction
        expected_query = sql.SQL("SELECT EXTRACT(YEAR FROM MIN({}))::int, EXTRACT(YEAR FROM MAX({}))::int FROM {}.{}").format(
            sql.Identifier("date_col"), sql.Identifier("date_col"),
            sql.Identifier("public"), sql.Identifier("my_table")
        )
        self.mock_cursor.execute.assert_called_once_with(expected_query)

    @patch('migrate.datetime')
    @patch('migrate.print_message')
    def test_get_year_range_db_error_uses_default(self, mock_print_message, mock_datetime):
        mock_datetime.now.return_value.year = 2023
        self.mock_cursor.execute.side_effect = psycopg2.Error("Query failed")
        min_year, max_year = dp.get_year_range(self.mock_conn, "error_table", "public", "date_col")
        self.assertEqual(min_year, 2023)
        self.assertEqual(max_year, 2024) # current_year + 1
        mock_print_message.assert_called_with(
            "Error determining year range for public.error_table (field date_col): Query failed. Using default range.", "WARNING"
        )

# --- Tests for main execution flow and execute_partition_steps ---
# These are more complex and require more involved mocking setups.
# Here's a sketch for execute_partition_steps

class TestExecutePartitionSteps(unittest.TestCase):
    def setUp(self):
        self.mock_conn = MagicMock(spec=psycopg2.extensions.connection)
        self.mock_cursor = MagicMock(spec=psycopg2.extensions.cursor)
        self.mock_conn.cursor.return_value.__enter__.return_value = self.mock_cursor
        self.mock_conn.encoding = "utf-8"
        self.table_name = "test_table"
        self.partition_field = "created_date"
        self.schema = "public"
        self.tmp_table_name = f"{self.table_name}_tmp"
        self.partitions_schema = "partitions"

        # Common mock returns
        self.patch_table_exists = patch('migrate.table_exists', return_value=self.schema)
        self.patch_is_partitioned = patch('migrate.is_table_partitioned', return_value=False)
        self.patch_get_schema = patch('migrate.get_table_schema', return_value=[('col1', 'type1', 'NO', None)])
        self.patch_validate_field = patch('migrate.validate_partition_field', return_value=None)
        self.patch_get_pk = patch('migrate.get_primary_key_info', return_value={'name': 'test_pk', 'columns': ['id']})
        self.patch_get_views = patch('migrate.get_dependent_views', return_value=[])
        self.patch_get_fks_referencing = patch('migrate.get_foreign_keys_referencing_table', return_value=[])
        self.patch_get_fks_on = patch('migrate.get_foreign_keys_on_table', return_value=[])
        self.patch_drop_triggers = patch('migrate.drop_triggers', return_value=[])
        self.patch_get_year_range = patch('migrate.get_year_range', return_value=(2022, 2023))
        self.patch_print_message = patch('migrate.print_message') # Catch all prints

        self.mock_table_exists = self.patch_table_exists.start()
        self.mock_is_partitioned = self.patch_is_partitioned.start()
        self.mock_get_schema = self.patch_get_schema.start()
        self.mock_validate_field = self.patch_validate_field.start()
        self.mock_get_pk = self.patch_get_pk.start()
        self.mock_get_views = self.patch_get_views.start()
        self.mock_get_fks_referencing = self.patch_get_fks_referencing.start()
        self.mock_get_fks_on = self.patch_get_fks_on.start()
        self.mock_drop_triggers = self.patch_drop_triggers.start()
        self.mock_get_year_range = self.patch_get_year_range.start()
        self.mock_print_message = self.patch_print_message.start()


    def tearDown(self):
        patch.stopall()



    def test_table_already_partitioned(self):
        self.mock_is_partitioned.return_value = True # Table is already partitioned
        dp.execute_partition_steps(self.mock_conn, self.table_name, self.partition_field, dry_run=True)
        self.mock_print_message.assert_any_call(f"Table {self.schema}.{self.table_name} is already a partitioned table. Skipping.", "INFO")
        self.mock_validate_field.assert_not_called() # Should skip before this

if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)