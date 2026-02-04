import logging
import os
import psycopg2
from contextlib import contextmanager
from sqlalchemy import create_engine, Engine
import pandas as pd


class Database:
    """Database connection manager with lazy configuration"""

    def __init__(self):
        self._config = None
        self._engine: Engine = None
        
    def get_valid_property(self, property_name):
        prop_value = os.getenv(property_name)
        if prop_value:
            return prop_value
        else:
            raise Exception(f"property {property_name} can not be empty")

    @property
    def config(self):
        """Lazy-loaded configuration property"""
        if self._config is None:
            dbname = self.get_valid_property('POSTGRES_DB')
            user = self.get_valid_property('POSTGRES_USER')
            password = self.get_valid_property('POSTGRES_PASSWORD')
            host = self.get_valid_property('DB_HOST')
            port = self.get_valid_property('DB_PORT')
            
            connection_url = f'postgresql://{user}:{password}@{host}:{port}/{dbname}'
            self._config = {
                'dbname': dbname,
                'user': user,
                'password': password,
                'host': host,
                'port': port,
            }
            self.connection_url = connection_url
            logging.info(f"database url {connection_url}")
            self._engine = create_engine(connection_url)
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
    