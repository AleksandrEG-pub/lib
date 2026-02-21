from decimal import Decimal
import logging
from datetime import datetime
import os
import pyarrow as pa
import pyarrow.fs as pa_fs
import pyarrow.csv as csv
import pyarrow.compute as pc
import adbc_driver_postgresql.dbapi as adbc


def _get_source_s3_data_generator():
    s3_fs = pa_fs.S3FileSystem(
        endpoint_override=os.getenv('S3_ENDPOINT'),
        access_key=os.getenv('ACCESS_KEY_ID'),
        secret_key=os.getenv('SECRET_ACCESS_KEY'),
        region=os.getenv('S3_REGION'),
        scheme='http',
        allow_bucket_creation=True,
        connect_timeout=60,
        background_writes=True
    )

    bucket = os.getenv('S3_BUCKET')
    file_path = os.getenv('S3_SOURCE_FILE')
    s3_path = f"{bucket}/{file_path}"
    with s3_fs.open_input_stream(s3_path) as file_input_stream:
        logging.info("getting data from csv source")
        reader: csv.CSVStreamingReader =  csv.open_csv(
            input_file=file_input_stream,
            parse_options=csv.ParseOptions(delimiter=','),
            read_options=csv.ReadOptions(autogenerate_column_names=False),
            convert_options=csv.ConvertOptions(column_types={
                "id": pa.int64(),
                "name": pa.string(),
                "age": pa.string(),
                "city": pa.string(),
                "salary": pa.string()
            })
        )
        for batch in reader:
            yield batch
    

def _add_loaded_at(batch: pa.RecordBatch):
    value = datetime.now()
    timestamps = pa.array([value] * len(batch))
    batch = batch.append_column('loaded_at', timestamps)
    logging.info('added column "loaded_at"')
    return batch


def _add_record_source(batch: pa.RecordBatch):
    source = os.getenv('S3_SOURCE_FILE')
    sources = pa.array([source] * len(batch))
    batch = batch.append_column('record_source', sources)
    logging.info('added column "record_source"')
    return batch


def _upload_to_sql(data: pa.RecordBatch):
    db_name=os.getenv('POSTGRES_DB')
    user=os.getenv('POSTGRES_USER')
    password=os.getenv('POSTGRES_PASSWORD')
    host=os.getenv('DB_HOST')
    port=os.getenv('DB_PORT')
    with adbc.connect(f"postgresql://{user}:{password}@{host}:{port}/{db_name}") as connection:
        with connection.cursor() as cursor:
            try:
                table = 'raw_employees'
                cursor.adbc_ingest(table, data, mode="append")
                logging.info(f"uploaded data to table: [{table}]")
            except Exception as e:
                logging.error(f"failed oto write data to sql table [{table}] {e}")
                raise e
        connection.commit()

def upload_data():
    # upload files from s3 csv to postgres
    for batch in _get_source_s3_data_generator():
        logging.info('source data batch:')
        logging.info(batch)
        batch = _add_loaded_at(batch)
        batch = _add_record_source(batch)
        batch = batch.rename_columns({'id': 'employee_id'})
        logging.info(batch)
        _upload_to_sql(batch)
      
