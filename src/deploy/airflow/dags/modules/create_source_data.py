import logging
import os
import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.fs as fs


def _get_file_system():
    return fs.S3FileSystem(
        endpoint_override=os.getenv('S3_ENDPOINT'),
        access_key=os.getenv('ACCESS_KEY_ID'),
        secret_key=os.getenv('SECRET_ACCESS_KEY'),
        region=os.getenv('S3_REGION'),
        scheme='http',
        allow_bucket_creation=True,
        connect_timeout=60,
        background_writes=True
    )
    

def _get_source_data() -> pa.Table:
    data = [
        {"id": 1, "name": "John Doe", "age": 30, "city": "New York", "salary": 75000.50},
        {"id": 2, "name": "Jane Smith", "age": 25, "city": "Los Angeles", "salary": 82000.75},
        {"id": 3, "name": "Bob Johnson", "age": 35, "city": "Chicago", "salary": 95000.00},
        {"id": 4, "name": "Alice Brown", "age": 28, "city": "Houston", "salary": 68000.25},
    ]
    table = pa.Table.from_pylist(data)
    return table


def _create_bucket_if_not_exist(bucket_name):
    file_system = _get_file_system()
    try:
        bucket_file_info = file_system.get_file_info(bucket_name)
        if bucket_file_info.type == fs.FileType.NotFound:
            logging.info("bucket does not exist")
            file_system.create_dir(bucket_name)
            logging.info("bucket created")
        else:
            logging.info("bucket exist")
    except Exception as e:
        logging.error("failed to check bucket {e}")
        raise e


def _write_file_to_s3(table: pa.Table, path: str):
    file_system = _get_file_system()
    try:
        with file_system.open_output_stream(path) as stream:
            pa_csv.write_csv(table, stream)
            print(f"Successfully wrote to {os.getenv('S3_ENDPOINT')}/{path}")
    except Exception as e:
        print(f"Error writing to S3: {e}")
        raise e


def create_source_data():
    bucket = os.getenv('S3_BUCKET')
    _create_bucket_if_not_exist(bucket_name=bucket)
    key = os.getenv('S3_SOURCE_FILE')
    s3_path = f"{bucket}/{key}"
    table: pa.Table = _get_source_data()
    _write_file_to_s3(table, s3_path)
    
