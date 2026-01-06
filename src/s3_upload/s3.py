import s3fs
from dotenv import load_dotenv
import pyarrow as pa
import pyarrow.parquet as pq


class S3Manager:

    def __init__(self):
        self.s3_endpoint = "http://localhost:10456"
        self.bucket = "it-one-bucket"
        self.fs: s3fs.S3FileSystem = s3fs.S3FileSystem(
            anon=True,
            client_kwargs={'endpoint_url': self.s3_endpoint,
                           'region_name': 'us-east-1'}
        )
        
    def init_bucket(self):
        bucket_name = self.bucket
        try:
            fs: s3fs.S3FileSystem = self.fs
            if not fs.exists(bucket_name):
                fs.mkdir(bucket_name)
                print(f"Bucket '{bucket_name}' created")
        except Exception as e:
            print(f"Error checking/creating bucket: {e}") 
            raise e


    def write_as_parquet(self, bucket_name: str, file_name: str, content: pa.Table):
        file_path = f"{bucket_name}/{file_name}"
        with self.fs.open(file_path, 'wb') as f:
            pq.write_table(content, f)

    def read_parquet(self, bucket_name: str, file_name: str) -> pa.Table:
        file_path = f"{bucket_name}/{file_name}"
        with self.fs.open(file_path, "rb") as f:
            table: pa.Table = pq.read_table(f)
            table = self._convert_numeric_to_decimal(table)
            return table



s3manager = S3Manager()
