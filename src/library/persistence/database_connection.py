import os
import psycopg2
from contextlib import contextmanager
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy import Engine
import pandas as pd


class Database:
    """Database connection manager with lazy configuration"""

    def __init__(self):
        self._config = None
        self._engine: Engine = None


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
            dbname = self._config['dbname']
            user = self._config['user']
            password = self._config['password']
            host = self._config['host']
            port = self._config['port']
            connection_string = f'postgresql://{user}:{password}@{host}:{port}/{dbname}'
            self._engine = create_engine(connection_string)
        return self._config


    @contextmanager
    def connection(self):
        """Get a database connection"""
        conn = psycopg2.connect(**self.config)
        try:
            yield conn
            conn.commit()
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


    def df_to_sql(self, table: str, df: pd.DataFrame):
        with self._engine.connect() as connection:
            df = df.to_sql(
                name=table,
                con=connection,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=100
            )
                
    @contextmanager
    def sqla_connection(self):
        with self._engine.begin() as conn:
                yield conn
    
            
    def sql_to_df(self, query: str, params: dict = {}):
        with self._engine.connect() as connection:
            return pd.read_sql_query(sql=query, con=connection, params=params)


# Create a singleton instance
db = Database()
