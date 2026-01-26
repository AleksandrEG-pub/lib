from importlib.resources import files
import logging
import os
import csv
import io
from typing import Any
import pandas as pd
from sqlalchemy import Tuple
from pyspark.sql import SparkSession
import database.database_connection as dc
import kafka_pipeline.kafka_service as ks
from kafka_pipeline.spark_service import spark_service
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, split, to_timestamp_ltz, lit, trim, regexp_replace, from_csv
from pyspark.sql.types import StringType, DateType, IntegerType, TimestampType, StructField, LongType, StructType


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
        rows: list[Tuple[Any, ...]] = cursor.fetchall()
        if rows:
            logging.info(f"table has {len(rows)} flights")
            for row in rows:
                logging.info(row)
                output = io.StringIO()
                writer = csv.writer(output)
                writer.writerow(row)
                csv_string = output.getvalue().strip()  # you already do this
                csv_string = csv_string.strip('"')      # extra safety
                ks.kafka_service.send_to_server(csv_string.encode('utf-8'))
    logging.info("uploaded data from postgres to kafka")


def sink_from_kafka_to_database():
    spark: SparkSession = spark_service.spark
    df: DataFrame = (spark.readStream
                     .format("kafka")
                     .option("kafka.bootstrap.servers", os.getenv('BOOTSTRAP_SERVER'))
                     .option("subscribe", "it-one")
                     .option("startingOffsets", "earliest")
                     .load()
                     )
    values = df.selectExpr("CAST(value AS STRING)")
    df = df.withColumn("key", col("key").cast(StringType()))
    df = df.withColumn("value", col("value").cast(StringType()))
    df = df.select('value')
    df = df.withColumn(
        "value",
        regexp_replace(col("value"), r'^[\[\(]|[\]\)]$', '')
    )
    schema = StructType([
        StructField("flight_id", LongType()),
        StructField("airline", StringType()),
        StructField("flight_number", StringType()),
        StructField("origin", StringType()),
        StructField("destination", StringType()),
        StructField("departure_time", StringType()),   # parse later
        StructField("arrival_time", StringType()),     # parse later
        StructField("duration_minutes", IntegerType()),
        StructField("aircraft_type", StringType()),
        StructField("status", StringType()),
        StructField("economy_seats", IntegerType()),
        StructField("business_seats", IntegerType()),
        StructField("first_class_seats", IntegerType()),
        StructField("booked_economy", IntegerType()),
        StructField("booked_business", IntegerType()),
        StructField("booked_first_class", IntegerType()),
    ])
    props = [
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
        'economy_seats',
        'business_seats',
        'first_class_seats',
        'booked_economy',
        'booked_business',
        'booked_first_class',
    ]
    schema_ddl = ",".join(
        f"{f.name} {f.dataType.simpleString().upper()}"
        for f in schema.fields
    )
    csv_options = {
        "sep": ",",
        "quote": '"',
        "ignoreLeadingWhiteSpace": "true",
        "ignoreTrailingWhiteSpace": "true"
    }
    df = df.select(
        from_csv(col("value"), schema_ddl, csv_options).alias("row")
    ).select("row.*")
    for i, column in enumerate(props):
        df = df.withColumn(column, trim(col(column)))
        df = df.withColumn(column, trim(regexp_replace(col(column), r'\]\["', '')))
        # col_type = column_to_type[1]
        # df = df.withColumn(col_name, col(col_name).try_cast(col_type))
    df = df.withColumn('flight_id', col('flight_id').cast(LongType()))
    df = df.withColumn('duration_minutes', col('duration_minutes').cast(IntegerType()))
    for time_col_name in ['departure_time', 'arrival_time']:
        df = df.withColumn(time_col_name,
                           to_timestamp_ltz(
                               trim(regexp_replace(
                                   col(time_col_name), r'"', '')),
                               lit("yyyy-MM-dd HH:mm:ssXXX")
                           ))
    to_remove = [
        'value',
        'economy_seats',
        'business_seats',
        'first_class_seats',
        'booked_economy',
        'booked_business',
        'booked_first_class',
    ]

    df = df.drop(*to_remove)
    df = df.na.drop()

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

# airline            VARCHAR
# flight_number      VARCHAR
# origin             VARCHAR
# destination        VARCHAR
# departure_time     timestamp
# arrival_time       timestamp
# duration_minutes   INTEGER
# aircraft_type      VARCHAR
# status             VARCHAR
# economy_seats      INTEGER
# business_seats     INTEGER
# first_class_seats  INTEGER
# booked_economy     INTEGER
# booked_business    INTEGER
# booked_first_class INTEGER
