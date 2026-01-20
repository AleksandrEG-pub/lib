import logging
import os
import pyarrow as pa
import pyarrow.csv as pa_csv
from importlib.resources import files, as_file
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from s3_upload.s3_manager import s3manager
from database.database_connection import db
import spark_upload.spark_sevice as spark_s

def _load_products_from_csv() -> pa.Table:
    resource = files("spark_upload").joinpath("data/products.csv")
    with as_file(resource) as path:
        return pa_csv.read_csv(path)

def init_product_parquet_file() -> str:
    products_pa_table = _load_products_from_csv()
    logging.info(f"loaded products.csv, size: [{len(products_pa_table)}]")
    file = 'products.parquet'
    s3manager.write_as_parquet(s3manager.spark_bucket, file, products_pa_table)
    logging.info("products saved to 'products.parquet'")
    return file

def upload_products_from_parquet_to_postgres(file_name: str):
    spark: SparkSession = spark_s.get_spark_session()
    data_df = spark.read.parquet(f"s3a://{s3manager.spark_bucket}/{file_name}")
    logging.info("read products from 'products.parquet'")
    data_df = data_df.withColumn('created_at', current_timestamp())
    data_df = data_df.withColumnRenamed('Product', 'name')
    data_df = data_df.withColumnRenamed('Price', 'price')
    data_df.write.jdbc(
        url=f"jdbc:postgresql://{db.config['host']}:{db.config['port']}/{db.config['dbname']}",
        table='products',
        mode='append',
        properties= {
            'user': os.getenv('POSTGRES_USER'),
            'password': os.getenv('POSTGRES_PASSWORD'),
        }
    )
    logging.info("products written to table 'products' in postgres")
    data_df.show()
