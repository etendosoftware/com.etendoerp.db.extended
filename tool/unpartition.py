import psycopg2
from psycopg2 import sql
from pathlib import Path
from configparser import ConfigParser
import re
import sys

BBDD_URL = 'bbdd.url'

# ─── Colors and Emojis ───

class Style:
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    RESET = '\033[0m'
    BOLD = '\033[1m'
    SUCCESS = '✅'
    ERROR = '❌'
    INFO = 'ℹ️'
    SKIP = '➡️'
    HEADER = '✨'

def print_message(msg, level="INFO", end="\n"):
    color = Style.RESET
    emoji = ""

    if level == "INFO":
        color = Style.CYAN
        emoji = Style.INFO
    elif level == "SUCCESS":
        color = Style.GREEN + Style.BOLD
        emoji = Style.SUCCESS
    elif level == "ERROR":
        color = Style.RED + Style.BOLD
        emoji = Style.ERROR
    elif level == "SKIP":
        color = Style.YELLOW
        emoji = Style.SKIP
    elif level == "HEADER":
        color = Style.MAGENTA + Style.BOLD
        emoji = Style.HEADER

    print(f"{color}{emoji} {msg}{Style.RESET}", end=end)

def print_unpartition_summary(summary):
    if not summary:
        print_message("No table was processed.", "SKIP")
        return

    border = "═" * 60
    print(f"\n{Style.BOLD}{Style.YELLOW}╔{border}╗")
    print(f"║{'FINAL SUMMARY OF DEPARTICIPATION':^60}║")
    print(f"╚{border}╝{Style.RESET}")

    for item in summary:
        table = item['table']
        status = item['status']
        message = item['message']
        if status == 'SUCCESS':
            emoji = Style.SUCCESS
            color = Style.GREEN
        else:
            emoji = Style.ERROR
            color = Style.RED
        print(f"{color}{emoji} {table:<25} → {status:<8} | {message}{Style.RESET}")

    print_message("Execute './gradlew update.database -Dforce=yes' to complete the operation", "HEADER")

# ─── Read Properties File ───

def read_properties_file(file_path):
    config = ConfigParser()
    p = Path(file_path)
    if not p.exists():
        print_message(f"Properties file {file_path} not found.", "ERROR")
        return {}

    with open(file_path, 'r', encoding='utf-8') as f:
        content = f"[default]\n{f.read()}"
    config.read_string(content)
    return {
        'bbdd.rdbms': config.get('default', 'bbdd.rdbms', fallback=None),
        BBDD_URL: config.get('default', BBDD_URL, fallback=None).replace('\\', ''),
        'bbdd.sid': config.get('default', 'bbdd.sid', fallback=None),
        'bbdd.user': config.get('default', 'bbdd.user', fallback=None),
        'bbdd.password': config.get('default', 'bbdd.password', fallback=None),
    }

def parse_db_params(properties):
    url = properties.get(BBDD_URL)
    match = re.match(r'jdbc:postgresql://([^:]+):(\d+)', url)
    if not match:
        raise ValueError(f"Invalid bbdd.url: {url}")
    host, port = match.groups()
    return {
        'host': host,
        'port': port,
        'dbname': properties.get('bbdd.sid'),
        'user': properties.get('bbdd.user'),
        'password': properties.get('bbdd.password')
    }

# ─── Unpartitioning Helpers ───

def get_schema(conn, table_name):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_schema FROM information_schema.tables
            WHERE table_name = %s AND table_schema NOT IN ('pg_catalog', 'information_schema')
            LIMIT 1
        """, (table_name,))
        res = cur.fetchone()
        return res[0] if res else None

def is_partitioned(conn, schema, table):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT c.relkind FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = %s AND n.nspname = %s
        """, (table, schema))
        r = cur.fetchone()
        return r and r[0] == 'p'

def get_child_partitions(conn, schema, table):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT inhrelid::regclass::text
            FROM pg_inherits
            JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
            JOIN pg_namespace ns ON parent.relnamespace = ns.oid
            WHERE parent.relname = %s AND ns.nspname = %s
        """, (table, schema))
        return [r[0] for r in cur.fetchall()]

def get_constraints(conn, schema, table):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT conname, contype, conkey, pg_get_constraintdef(pg_constraint.oid), conislocal
            FROM pg_constraint
            WHERE conrelid = %s::regclass
        """, (f"{schema}.{table}",))
        return cur.fetchall()

def get_foreign_keys(conn, schema, table):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT conname, confrelid::regclass::text, conkey, confkey
            FROM pg_constraint
            WHERE conrelid = %s::regclass AND contype = 'f'
        """, (f"{schema}.{table}",))
        return cur.fetchall()

def get_column_names_by_indexes(conn, schema, table, indexes):
    if not indexes:
        return []
    with conn.cursor() as cur:
        cur.execute("""
            SELECT attnum, attname
            FROM pg_attribute
            WHERE attrelid = %s::regclass AND attnum > 0 AND NOT attisdropped
        """, (f"{schema}.{table}",))
        mapping = dict(cur.fetchall())
        return [mapping.get(i) for i in indexes if i in mapping]

def add_constraints(conn, schema, table, constraints, foreign_keys):
    with conn.cursor() as cur:
        # Re-read existing constraints for safety
        cur.execute("""
            SELECT conname FROM pg_constraint
            WHERE conrelid = %s::regclass
        """, (f"{schema}.{table}",))
        existing_constraints = {row[0] for row in cur.fetchall()}

        # Restaurar CHECK constraints
        for conname, contype, conkey, consrc in constraints:
            if contype == 'p':
                continue  # saltamos PK original here; la manejamos al final
            if conname in existing_constraints:
                print_message(f"Constraint {conname} already exists, skipping.", "SKIP")
                continue

            try:
                if contype == 'c':
                    if consrc.upper().startswith('CHECK '):
                        consrc = consrc[6:].strip()

                    if needs_parentheses(consrc):
                        consrc = f"(({consrc}))"

                    cur.execute(sql.SQL("""
                        ALTER TABLE {}.{} ADD CONSTRAINT {} CHECK ({})
                    """).format(
                        sql.Identifier(schema),
                        sql.Identifier(table),
                        sql.Identifier(conname),
                        sql.SQL(consrc)
                    ))
                    print_message(f"Added CHECK constraint {conname} on {schema}.{table}", "INFO")
            except Exception as e:
                print_message(f"Failed to add CHECK constraint {conname} on {schema}.{table}: {e}", "ERROR")
                continue

        # Restaurar Foreign Keys (se crean NOT VALID para evitar validación inmediata)
        for conname, ref_table, conkey, refkey in foreign_keys:
            if conname in existing_constraints:
                print_message(f"Foreign key {conname} already exists, skipping.", "SKIP")
                continue

            try:
                source_cols = get_column_names_by_indexes(conn, schema, table, conkey)
                if '.' in ref_table:
                    target_schema, target_table = ref_table.split('.')
                else:
                    target_schema, target_table = schema, ref_table
                target_cols = get_column_names_by_indexes(conn, target_schema, target_table, refkey)
                if not source_cols or not target_cols:
                    print_message(f"Columns not found for foreign key {conname}, skipping.", "SKIP")
                    continue

                # Create FK with NOT VALID to avoid scanning/validation errors now.
                cur.execute(sql.SQL("""
                    ALTER TABLE {}.{} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {}.{} ({}) NOT VALID
                """).format(
                    sql.Identifier(schema), sql.Identifier(table), sql.Identifier(conname),
                    sql.SQL(', ').join(map(sql.Identifier, source_cols)),
                    sql.Identifier(target_schema), sql.Identifier(target_table),
                    sql.SQL(', ').join(map(sql.Identifier, target_cols))
                ))
                print_message(f"Added FOREIGN KEY {conname} on {schema}.{table} (NOT VALID)", "INFO")
            except Exception as e:
                print_message(f"Failed to add foreign key {conname} on {schema}.{table}: {e}", "ERROR")
                continue

        # Crear nueva PK basada en <table_name>_id
        try:
            pk_column = f"{table}_id"
            new_pk_name = f"{table}_pk"
            # Si ya existe una PK, la saltamos
            cur.execute("SELECT 1 FROM pg_constraint WHERE contype = 'p' AND conrelid = %s::regclass", (f"{schema}.{table}",))
            if cur.fetchone():
                print_message(f"Primary key already exists on {schema}.{table}, skipping PK creation.", "SKIP")
            else:
                # Crear la PK; esto puede fallar si hay duplicados
                cur.execute(sql.SQL("""
                    ALTER TABLE {}.{} ADD CONSTRAINT {} PRIMARY KEY ({})
                """).format(
                    sql.Identifier(schema), sql.Identifier(table),
                    sql.Identifier(new_pk_name), sql.Identifier(pk_column)
                ))
                print_message(f"Primary key {new_pk_name} added on column {pk_column}", "INFO")
        except Exception as e:
            print_message(f"Failed to add primary key on {schema}.{table}: {e}", "ERROR")
            # no abortamos toda la ejecución; el usuario debe revisar datos/duplicados

def needs_parentheses(expr):
    # Ignora si ya comienza con paréntesis
    if expr.strip().startswith('('):
        return False
    # Detecta mezcla de operadores lógicos
    has_and = re.search(r'\bAND\b', expr, re.IGNORECASE)
    has_or = re.search(r'\bOR\b', expr, re.IGNORECASE)
    return has_and and has_or

def delete_table_config_entry(conn, table_name):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT ad_table_id FROM ad_table WHERE lower(tablename) = lower(%s)
        """, (table_name,))
        result = cur.fetchone()
        if not result:
            print_message(f"Table {table_name} not found in AD_Table. Skipping deletion from etarc_table_config.", "SKIP")
            return
        ad_table_id = result[0]

        cur.execute("""
            DELETE FROM etarc_table_config WHERE ad_table_id = %s
        """, (ad_table_id,))
        print_message(f"Deleted config for {table_name} from etarc_table_config (ad_table_id={ad_table_id})", "INFO")

def unpartition_table(conn, table_name):
    schema = get_schema(conn, table_name)
    if not schema:
        print_message(f"Table {table_name} not found.", "ERROR")
        return
    if not is_partitioned(conn, schema, table_name):
        print_message(f"{schema}.{table_name} is not partitioned. Skipping.", "SKIP")
        return

    tmp_table = f"{table_name}_tmp_unpart"
    with conn.cursor() as cur:
        constraints = get_constraints(conn, schema, table_name)
        foreign_keys = get_foreign_keys(conn, schema, table_name)

        print_message(f"Unpartitioning {Style.BOLD}{schema}.{table_name}{Style.RESET}...", "INFO")

        # DROP de constraints locales (no heredadas) en la tabla original antes de crear la copia temporal
        sp_counter = 0
        for con in constraints:
            # constraints now include conislocal as last element
            if len(con) == 5:
                conname, contype, conkey, consrc, conislocal = con
            else:
                # fallback for unexpected shape
                conname, contype, conkey, consrc = con
                conislocal = True

            if not conislocal:
                print_message(f"Constraint {conname} is inherited, skipping drop.", "SKIP")
                continue

            sp_counter += 1
            sp_name = f"sp_drop_{sp_counter}"
            try:
                cur.execute(sql.SQL("SAVEPOINT {}" ).format(sql.Identifier(sp_name)))
            except Exception:
                sp_name = None

            try:
                cur.execute(sql.SQL("ALTER TABLE {}.{} DROP CONSTRAINT IF EXISTS {} CASCADE").format(
                    sql.Identifier(schema), sql.Identifier(table_name), sql.Identifier(conname)
                ))
                print_message(f"Dropped constraint {conname} on {schema}.{table_name}", "INFO")
                if sp_name:
                    try:
                        cur.execute(sql.SQL("RELEASE SAVEPOINT {}" ).format(sql.Identifier(sp_name)))
                    except Exception:
                        pass
            except Exception as e:
                # rollback to savepoint to keep transaction usable
                if sp_name:
                    try:
                        cur.execute(sql.SQL("ROLLBACK TO SAVEPOINT {}" ).format(sql.Identifier(sp_name)))
                        cur.execute(sql.SQL("RELEASE SAVEPOINT {}" ).format(sql.Identifier(sp_name)))
                    except Exception:
                        pass
                print_message(f"Failed to drop constraint {conname} on {schema}.{table_name}: {e}", "ERROR")
                continue

        # Crear la tabla temporal y copiar datos
        cur.execute(sql.SQL("""
            CREATE TABLE {}.{} (LIKE {}.{} INCLUDING DEFAULTS INCLUDING IDENTITY)
        """).format(sql.Identifier(schema), sql.Identifier(tmp_table),
                    sql.Identifier(schema), sql.Identifier(table_name)))
        cur.execute(sql.SQL("""
            INSERT INTO {}.{} SELECT * FROM {}.{}
        """).format(sql.Identifier(schema), sql.Identifier(tmp_table),
                    sql.Identifier(schema), sql.Identifier(table_name)))

        # Borrar particiones hijas
        for part in get_child_partitions(conn, schema, table_name):
            cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.SQL(part)))
            print_message(f"Partition dropped: {part}", "INFO")

        # Borrar tabla original y renombrar la temporal
        cur.execute(sql.SQL("DROP TABLE {}.{} CASCADE").format(sql.Identifier(schema), sql.Identifier(table_name)))
        cur.execute(sql.SQL("ALTER TABLE {}.{} RENAME TO {}")
                    .format(sql.Identifier(schema), sql.Identifier(tmp_table), sql.Identifier(table_name)))

        # NOTA: no se recrean constraints aquí — la tarea gradlew se encargará de ello
        print_message(f"Table {schema}.{table_name} was successfully unpartitioned (constraints removed).", "SUCCESS")
        delete_table_config_entry(conn, table_name)
    conn.commit()

# ─── New: fetch tables from etarc_table_config ───

def fetch_tables_from_config(conn):
    """
    Devuelve la lista de tablas (lower(tablename)) que están en etarc_table_config.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT lower(tablename)
            FROM ad_table
            WHERE ad_table_id IN (SELECT ad_table_id FROM etarc_table_config)
        """)
        rows = cur.fetchall()
        tables = [r[0] for r in rows]
        if tables:
            print_message(f"Found {len(tables)} table(s) to unpartition from etarc_table_config.", "INFO")
        else:
            print_message("No tables found in etarc_table_config.", "SKIP")
        return tables

# ─── Main Entry ───

def main():
    success = True
    summary = []

    props_path = Path("config/Openbravo.properties")
    if not props_path.exists():
        print_message(f"File not found: {props_path}", "ERROR")
        return False

    try:
        props = read_properties_file(str(props_path))
        if not props.get("bbdd.url") or not props.get("bbdd.user"):
            print_message("Missing connection parameters in Openbravo.properties", "ERROR")
            return False

        # Conexión
        conn = psycopg2.connect(**parse_db_params(props))
        print_message("Connection established successfully", "SUCCESS")

        # Determinar tablas a procesar:
        if len(sys.argv) >= 2 and sys.argv[1].strip():
            # Modo explícito: "t1,t2,..."
            tables = [x.strip().lower() for x in sys.argv[1].split(",") if x.strip()]
            print_message(f"Using tables from command-line: {', '.join(tables)}", "INFO")
        else:
            # Modo automático: leer de etarc_table_config
            print_message("No tables provided. Fetching from etarc_table_config…", "INFO")
            tables = fetch_tables_from_config(conn)
            if not tables:
                conn.close()
                print_unpartition_summary(summary)
                return False

        # 🔹 Confirmación antes de ejecutar
        print_message("The following tables will be unpartitioned:", "HEADER")
        for t in tables:
            print(f"  - {t}")
        confirm = input(f"\n{Style.BOLD}{Style.YELLOW}Type 'yes' to continue: {Style.RESET}").strip().lower()
        if confirm != "yes":
            print_message("Operation cancelled by user.", "SKIP")
            conn.close()
            return False

        # Ejecutar desparticionado
        for t in tables:
            try:
                unpartition_table(conn, t)
                summary.append({'table': t, 'status': 'SUCCESS', 'message': 'Table correctly despartitioned.'})
            except Exception as e_table:
                print_message(f"Failed to unpartition table {t}: {e_table}", "ERROR")
                summary.append({'table': t, 'status': 'FAILURE', 'message': str(e_table)})
                success = False
                # rollback the connection to continue with next tables
                conn.rollback()

        conn.close()
    except Exception as e:
        print_message(f"Unexpected error: {e}", "ERROR")
        summary.append({'table': 'GLOBAL_EXECUTION', 'status': 'FAILURE', 'message': str(e)})
        success = False

    print_unpartition_summary(summary)
    return success

if __name__ == "__main__":
    result = main()
    sys.exit(0 if result else 1)