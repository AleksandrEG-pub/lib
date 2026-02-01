import os
import logging
from datetime import datetime, timezone
from importlib.resources import as_file, files

import pyarrow as pa
import pyarrow.csv as pa_csv
from s3_upload.s3_manager import s3manager
from spark_upload import spark_sevice
import pyspark.sql.types as st
import pyspark.sql.functions as sf
from pyspark.sql import SparkSession


bucket_name = 'delivery'
processed_directory = 'processed'

def _read_csv():
    delivery_files = [
        "delivery_1.csv",
        "delivery_2.csv",
        "delivery_3.csv",
        "delivery_4.csv",
        "delivery_5.csv",
    ]
    for file in delivery_files:
        resource = files("airflow_pipeline").joinpath(f"data/{file}")
        with as_file(resource) as path:
            yield (file, pa_csv.read_csv(path))
    
    
def init_data_from_csv():
    logging.info('starting data initialization from csv')
    s3manager.init_bucket(bucket_name)
    for file in _read_csv():
        file_name: str = file[0]
        file_name = file_name.replace('.csv', '.parquet')
        pa_table: pa.Table = file[1]
        s3manager.write_parquet(bucket_name, file_name, pa_table)
    logging.info('finished data initialization from csv')
        

def upload_from_s3_to_postgres():
    spark_session: SparkSession = spark_sevice.get_spark_session()
    file_name = s3manager.get_first_root_file(bucket_name)
    s3_file_name = f"s3a://{bucket_name}/{file_name}"
    logging.info(f"uploading file {s3_file_name} to postgres")
    
    delivery_type = st.StructType([
        st.StructField('delivery_id', st.StringType()),
        st.StructField('item_type', st.StringType()),
        st.StructField('quantity', st.IntegerType()),
        st.StructField('price_per_unit', st.DecimalType()),
        st.StructField('manufacture_datetime', st.TimestampType()),
    ])
    delivery_file_df = spark_session.read.parquet(s3_file_name)
    delivery_file_df.printSchema()
    delivery_file_df.show()
    # delivery_file_df.withColumn('source_file', sf.lit(file_name))
    # delivery_file_df.withColumn('upload_timestamp', sf.current_timestamp())
    
    # create a StructType for schema
    # delivery_file_df.jdbc(
            # url=f"jdbc:postgresql://{db.config['host']}:{db.config['port']}/{db.config['dbname']}",
            # table='products',
            # mode='append',
            # properties= {
                # 'user': os.getenv('POSTGRES_USER'),
                # 'password': os.getenv('POSTGRES_PASSWORD'),
            # }
        # )
    logging.info(f"delivery file {file_name} written to table 'deliveries' in postgres")

    # prepend file name with timestamp of precessing    
    # timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    # new_name = f"{timestamp}_{file_name}"
    # s3manager.move_file(bucket_name=bucket_name, src_path=new_name, target_dir=processed_directory)


def _find_latest_file(files: list[str]) -> str | None:
    if not files:
        return None
    return max(
        files,
        key=lambda p: os.path.basename(p).split("_", 1)[0]
    )
    
def _get_last_uploaded() -> str:
    files = s3manager.list_files(bucket_name, processed_directory)
    return _find_latest_file(files)
    
    
def check_validity_of_file_upload() -> bool:
    file_name = _get_last_uploaded()
    if file_name is None:
        logging.error(f"no files in {processed_directory} directory")
        return False
    file_lines_count = s3manager.count_file_lines(bucket_name, file_name)
    # db select * from deliveries where source_file = '{file_name}'
    # db_count == file_lines_count    
    return True
