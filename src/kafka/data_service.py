from importlib.resources import files
import logging
from typing import Any
import pandas as pd
from sqlalchemy import Tuple
import database.database_connection as dc


def init_data():
    with dc.db.cursor() as cursor:
        cursor.execute("SELECT * FROM flights")
        rows: list[Tuple[Any, ...]] = cursor.fetchall()
        if rows:
            return
    csv_path = files("kafka").joinpath("data/flights.csv")
    df: pd.DataFrame = pd.read_csv(csv_path)
    dc.db.df_to_sql(table='flights', df=df)
    logging.info("uploaded data from csv to table 'flights'")


def upload_from_database_to_kafka():
    with dc.db.cursor() as cursor:
        cursor.execute("SELECT * FROM flights")
        rows: list[Tuple[Any, ...]] = cursor.fetchall()
        if rows:
            logging.info(f"table has {len(rows)} flights")

def sink_from_kafka_to_database():
    pass
