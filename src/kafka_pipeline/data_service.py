from importlib.resources import files
import json
import logging
import os
from typing import Any
import pandas as pd
from sqlalchemy import Tuple
from pyspark.sql import SparkSession
import database.database_connection as dc
import kafka_pipeline.kafka_service as ks
from kafka_pipeline.spark_service import spark_service
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as sf
import pyspark.sql.types as st


def init_data():
    logging.info("initializing data from csv")
    with dc.db.cursor() as cursor:
        cursor.execute("SELECT * FROM flights")
        rows: list[Tuple[Any, ...]] = cursor.fetchall()
        if rows:
            logging.info("data already exist. no csv upload.")
            return
    csv_path = files("kafka_pipeline").joinpath("data/flights.csv")
    df: pd.DataFrame = pd.read_csv(csv_path)
    dc.db.df_to_sql(table='flights', df=df)
    logging.info("uploaded data from csv to table 'flights'")


def upload_from_database_to_kafka():
    logging.info("uploading data from postgres to kafka")
    with dc.db.cursor() as cursor:
        cursor.execute("SELECT * FROM flights")
        columns = [c[0] for c in cursor.description]
        for row in cursor.fetchall():
            message = {
                "schema": "flight.v1",
                "data": dict(zip(columns, row))
            }
            json_message = json.dumps(message, default=str)
            ks.kafka_service.send_to_server(json_message.encode("utf-8"))
    logging.info("uploaded data from postgres to kafka")


def sink_from_kafka_to_database():
    spark: SparkSession = spark_service.spark
    df: DataFrame = (spark.readStream
                     .format("kafka")
                     .option("kafka.bootstrap.servers", os.getenv('BOOTSTRAP_SERVER'))
                     .option("subscribe", "it-one")
                     .option("startingOffsets", "earliest")
                     .load()
                     .select(sf.col('value'))
                     .withColumn("value", sf.col("value").cast(st.StringType()))
                     .withColumn('value', sf.from_json(sf.col("value"), schemes.kafka_message_type))
                     .select("value.*", "*")
                     .select("data")
                     .select("data.*", "*")
                     .select(*(schemes.flights_upload_properties))
                     )
    def write_to_sql(batch_df, batch_id):
        try:
            batch_df.write.jdbc(
                url=f"jdbc:postgresql://{dc.db.config['host']}:{dc.db.config['port']}/{dc.db.config['dbname']}",
                table='flights_upload',
                mode='append',
                properties={
                    'user': os.getenv('POSTGRES_USER'),
                    'password': os.getenv('POSTGRES_PASSWORD'),
                    'batchsize': '25'
                }
            )
            print(f"Batch {batch_id} written successfully")
        except Exception as e:
            print(f"Error in batch {batch_id}: {e}")
            raise
    df.writeStream.foreachBatch(write_to_sql).start().awaitTermination()


class KafkaScheme():
    def __init__(self):
        data_type = st.StructType([
            st.StructField("flight_id", st.LongType()),
            st.StructField("airline", st.StringType()),
            st.StructField("flight_number", st.StringType()),
            st.StructField("origin", st.StringType()),
            st.StructField("destination", st.StringType()),
            st.StructField("departure_time", st.TimestampType()),
            st.StructField("arrival_time", st.TimestampType()),
            st.StructField("duration_minutes", st.IntegerType()),
            st.StructField("aircraft_type", st.StringType()),
            st.StructField("status", st.StringType()),
            st.StructField("economy_seats", st.IntegerType()),
            st.StructField("business_seats", st.IntegerType()),
            st.StructField("first_class_seats", st.IntegerType()),
            st.StructField("booked_economy", st.IntegerType()),
            st.StructField("booked_business", st.IntegerType()),
            st.StructField("booked_first_class", st.IntegerType()),
        ])
        self.kafka_message_type = st.StructType([
            st.StructField("schema", st.StringType()),
            st.StructField("data", data_type)
        ])

        self.flights_upload_properties = [
            'flight_id',
            'airline',
            'flight_number',
            'origin',
            'destination',
            'departure_time',
            'arrival_time',
            'duration_minutes',
            'aircraft_type',
            'status',
        ]


schemes = KafkaScheme()
