from contextlib import contextmanager
import os
import logging
import psycopg2 
import pyarrow as pa
import pyarrow.fs as pa_fs
import pyarrow.csv as csv


@contextmanager
def _get_cursor():
    with psycopg2.connect(
        database=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD'),
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT')
    ) as connection:
        with connection.cursor() as cursor:
            yield cursor
        
def _get_s3_fs():
    return pa_fs.S3FileSystem(
        endpoint_override=os.getenv('S3_ENDPOINT'),
        access_key=os.getenv('ACCESS_KEY_ID'),
        secret_key=os.getenv('SECRET_ACCESS_KEY'),
        region=os.getenv('S3_REGION'),
        scheme='http',
        allow_bucket_creation=True,
        connect_timeout=60,
        background_writes=True
    )

def _get_s3_file_row_count(bucket, file_path):
    s3_path = f"{bucket}/{file_path}"
    with _get_s3_fs().open_input_stream(s3_path) as file_input_stream:
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
        num_rows = 0
        for batch in reader:
            num_rows += batch.num_rows
        return num_rows


def _get_row_count_table(table_name):
    with _get_cursor() as cursor:
        query = f"select count(*) from {table_name}"
        cursor.execute(query)
        row = cursor.fetchone()
        if row:
            employee_record_count_sql = int(row[0])
            return employee_record_count_sql


def _record_fullness_check(source_record_count, target_record_count):
    with _get_cursor() as cursor:
        query = """
        insert into data_quality_checks(check_name, status, value)
        values (%s, %s, %s)
        """
        # value - row count relation in %
        value = target_record_count / source_record_count * 100
        # status - is value -+5%: OK / NOK
        status = 'OK' if abs(100 - value) < 5 else 'NOK' 
        cursor.execute(query, ('fullness', status, value))
    

def fullness():
    employee_record_count_sql = _get_row_count_table('stage_employees')
    bucket = os.getenv('S3_BUCKET')
    file_path = os.getenv('S3_SOURCE_FILE')
    employee_record_count_s3_source = _get_s3_file_row_count(bucket, file_path)
    _record_fullness_check(employee_record_count_s3_source, employee_record_count_sql)
    logging.info(f"records in target {file_path}: [{employee_record_count_sql}], in source: [{employee_record_count_s3_source}]")

