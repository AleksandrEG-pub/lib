from importlib.resources import files
import logging
from typing import Any
import pandas as pd
from sqlalchemy import Tuple
import database.database_connection as dc
import kafka_pipeline.kafka_service as ks


def init_data():
    logging.info("initializing data from csv")
    with dc.db.cursor() as cursor:
        cursor.execute("SELECT * FROM flights")
        rows: list[Tuple[Any, ...]] = cursor.fetchall()
        if rows:
            logging.info("data already exist. no csv upload.")
            return
    csv_path = files("kafka").joinpath("data/flights.csv")
    df: pd.DataFrame = pd.read_csv(csv_path)
    dc.db.df_to_sql(table='flights', df=df)
    logging.info("uploaded data from csv to table 'flights'")


def upload_from_database_to_kafka():
    logging.info("uploading data from postgres to kafka")
    with dc.db.cursor() as cursor:
        cursor.execute("SELECT * FROM flights")
        rows: list[Tuple[Any, ...]] = cursor.fetchall()
        if rows:
            logging.info(f"table has {len(rows)} flights")
            for row in rows:
                ks.kafka_service.send_to_server(row)
    logging.info("uploaded data from postgres to kafka")
            

def sink_from_kafka_to_database():
    pass
