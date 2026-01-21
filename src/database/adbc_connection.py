import os
from dotenv import load_dotenv
from contextlib import contextmanager
import adbc_driver_postgresql.dbapi as adbc


class AdbcDatabase:
    """Database connection manager with lazy configuration"""

    def __init__(self):
        self._config = None

    @property
    def config(self):
        """Lazy-loaded configuration property"""
        if self._config is None:
            load_dotenv()
            self._config = {
                'dbname': os.getenv('POSTGRES_DB'),
                'user': os.getenv('POSTGRES_USER'),
                'password': os.getenv('POSTGRES_PASSWORD'),
                'host': os.getenv('DB_HOST'),
                'port': os.getenv('DB_PORT')
            }
        return self._config


    @contextmanager
    def connection(self):
        """Get a database connection"""
        if self._config is None:
            self.config
        conf = self._config
        URI = f"postgresql://{conf['user']}:{conf['password']}@{conf['host']}:{conf['port']}/{conf['dbname']}"
        conn = adbc.connect(uri=URI)
        try:
            yield conn
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        
    @contextmanager
    def cursor(self):
        """Get a database cursor"""
        with self.connection() as conn:
            cursor = conn.cursor()
            try:
                yield cursor
            finally:
                cursor.close()

adbc_connect = AdbcDatabase()