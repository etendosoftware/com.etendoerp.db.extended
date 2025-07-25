# db_utils.py
def print_message(message, level="INFO", end="\n"):
    # Importamos en tiempo de ejecución, cuando migrate.py ya está cargado
    from migrate import print_message as _print_message
    _print_message(message, level=level, end=end)

from functools import wraps
import psycopg2

def handle_db_errors(default=None):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except psycopg2.Error as e:
                print_message(f"Database error en {func.__name__}: {e}", "ERROR")
                return default
            except Exception as e:
                print_message(f"Error en {func.__name__}: {e}", "ERROR")
                return default
        return wrapper
    return decorator

def fetchall(conn, query, params=None, log_msg=None, level="DEBUG"):
    if log_msg:
        print_message(log_msg, level)
    with conn.cursor() as cur:
        cur.execute(query, params or ())
        return cur.fetchall()

def fetchone(conn, query, params=None, log_msg=None, level="DEBUG"):
    if log_msg:
        print_message(log_msg, level)
    with conn.cursor() as cur:
        cur.execute(query, params or ())
        return cur.fetchone()
