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
from pyspark.sql.functions import struct as s_struct


class Schemes():
    '''
    Contains schemas for integrations with sql, spark and kafka 
    - kafka_message_type - structure of kafka message in spark types
    -flights_upload_properties - which columns contain 'flight-upload' table
    '''
    def __init__(self):
        flights_data_type = st.StructType([
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
            st.StructField("data", flights_data_type)
        ])
        self.json_parse_type = st.StructType([
            st.StructField("parsed", st.StringType()),
            st.StructField("error", st.StringType())
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
schemes = Schemes()


def init_data():
    '''
    upload data to 'flight' table from ./data/flights.csv file
    '''
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
    '''
    Upload data from 'flights' table to kafka 'it-one' topic using database cursor.
    Each row converted to json.
    
    Use on big data with caution, because it collects send task's futures, which can
    take significant memory if tables are big 
    '''
    logging.info("uploading data from postgres to kafka")
    send_futures = []
    with dc.db.cursor() as cursor:
        cursor.execute("SELECT * FROM flights")
        columns = [c[0] for c in cursor.description]
        for row in cursor.fetchall():
            message = {
                "schema": "flight.v1",
                "data": dict(zip(columns, row))
            }
            json_message = json.dumps(message, default=str)
            future = ks.kafka_service.send_to_server(
                json_message.encode("utf-8"))
            send_futures.append(future)
    timeout_seconds = 30
    for future in send_futures:
        try:
            future.get(timeout=timeout_seconds)
        except Exception as e:
            logging.error(f"Failed to send message: {e}")
    logging.info("uploaded data from postgres to kafka")


@sf.udf(returnType=schemes.json_parse_type)
def _parse_json(kafka_message):
    '''
    Decodes kafka message
    Check if message has json structure
    Check if message has correct schema version
    '''
    json_binary = kafka_message['value']
    message_payload_str = json_binary.decode("utf-8")
    try:
        message_payload_json = json.loads(message_payload_str)
        immitate_wrong_version = str(message_payload_json['data']['flight_number']).find("A") > 0
        if message_payload_json.get('schema') == 'flight.v1' and immitate_wrong_version:
            return (message_payload_str, None)
        else:
            return (None, f"unexpected version: {message_payload_json['schema']}. Expected: [flight.v1]")
    except Exception as e:
        return (None, f"error processing: {str(e)}")


def _write_to_sql(batch_df, batch_id):
    '''
    Writes spark microbatches to postgres table 'flights_upload'
    '''
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
        logging.info(f"Batch {batch_id} written successfully")
    except Exception as e:
        logging.info(f"Error in batch {batch_id}: {e}")
        raise


def sink_from_kafka_to_database():
    logging.info("sink data from topic 'it-one' to 'flights_upload' table")
    spark: SparkSession = spark_service.spark
    df: DataFrame = (spark.readStream
                     .format("kafka")
                     .option("kafka.bootstrap.servers", os.getenv('BOOTSTRAP_SERVER'))
                     .option("subscribe", "it-one")
                     .option("startingOffsets", "earliest")
                     .load()
                     .withColumn('original_message', s_struct('*'))
                     .select('original_message')
                     .withColumn("json_parse_result", _parse_json(s_struct('original_message.*')))
                     )
    df_messages = (df.filter(sf.col('json_parse_result.parsed').isNotNull())
                   .withColumn('kafka_message',
                               sf.from_json(sf.col('json_parse_result.parsed'), schema=schemes.kafka_message_type))
                   .select("kafka_message.data.*").alias('flight_data')
                   .select(*(schemes.flights_upload_properties)))
    df_messages.writeStream.foreachBatch(_write_to_sql).start()
    logging.info("finished upload to 'flights_upload' table")

    df_error = (df.filter(sf.col('json_parse_result.error').isNotNull())
                .withColumn('reason_in_dlq', sf.col('json_parse_result.error'))
                .select(sf.to_json(sf.struct("original_message", "reason_in_dlq")).alias('value'))
                )
    (df_error.writeStream
     .format("kafka")
     .option("kafka.bootstrap.servers", os.getenv('BOOTSTRAP_SERVER'))
     .option("topic", "it-one.dlq")
     .option("checkpointLocation", "/tmp/checkpoint/it-one-dlq")
     .start()
     .awaitTermination(timeout=30)
     )
    logging.info("finished sending messages to dlq")
