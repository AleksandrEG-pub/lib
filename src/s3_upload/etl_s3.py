import os
import pyarrow as pa
import pyarrow.parquet as pq
import database.database_setup as ds
from database.adbc_connection import adbc_connect
from s3_upload.s3 import s3manager
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
    file = 'products.parquet'
    BUCKET_NAME = "it_one_bucket",
    s3manager.create_bucket(BUCKET_NAME)
    s3manager.write_as_parquet(BUCKET_NAME, file, pa_table)
    result_pa_table = s3manager.read_parquet(BUCKET_NAME, file)
    print(result_pa_table)
        
        
def s3_upload_iceberg(data):
    pass

