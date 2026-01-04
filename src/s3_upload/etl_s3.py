import os
import pyarrow as pa
import pyarrow.parquet as pq
import database.database_setup as ds
from database.adbc_connection import adbc_connect
import s3fs

script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)
init_tables = f"{script_dir}/sql/init-tables.sql"
init_data = f"{script_dir}/sql/init-data.sql"


def init_database():
    ds.execute_scripts(init_tables)
    ds.execute_scripts(init_data)
    
    
def load_products():
    with adbc_connect.cursor() as cursor:
        cursor.execute("SELECT * FROM products")
        arrow_table: "pa.Table" = cursor.fetch_arrow_table()
        return arrow_table


def s3_upload_parquet(pa_table: pa.Table):
    S3_ENDPOINT = "http://localhost:10456"
    BUCKET_NAME = "it_one_bucket"
    BUCKET_NAME = f"/buckets/{BUCKET_NAME}"
    fs = s3fs.S3FileSystem(
        anon=True,
        client_kwargs={'endpoint_url': S3_ENDPOINT}
    )
    file = f'{BUCKET_NAME}/products.parquet'
    
    try:
        if not fs.exists(BUCKET_NAME):
            fs.mkdir(BUCKET_NAME)
            print(f"Bucket '{BUCKET_NAME}' created")
    except Exception as e:
        print(f"Error checking/creating bucket: {e}")
    
    with fs.open(file, 'wb') as f:
        # f.write('New log entry\n')
        # pq.write_table(pa_table, file, filesystem=fs, compression=None)  
        # pq.write_table(pa_table, f, compression=None)  
        pq.write_table(pa_table, f)
    # with fs.open(file, 'r') as f:
        # content = f.read()
    with fs.open(file, "rb") as f:
        table = pq.read_table(f)
        print(table)
        
        
def s3_upload_iceberg(data):
    pass

