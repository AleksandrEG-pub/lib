import os
import s3fs
import pyarrow as pa
import pyarrow.parquet as pq


class S3Manager:

    def __init__(self):
        self.endpoint = os.getenv('S3_ENDPOINT')
        self.region = os.getenv('S3_REGION')
        self.s3_bucket = os.getenv('S3_BUCKET')
        self.spark_bucket = os.getenv('SPARK_BUCKET')
        self.catalog = os.getenv('S3_CATALOG')
        self.namespace = os.getenv('S3_NAMESPACE')
        self.table = os.getenv('S3_TABLE')
        self.fs: s3fs.S3FileSystem = s3fs.S3FileSystem(
            anon=True,
            client_kwargs={'endpoint_url': self.endpoint,
                           'region_name': self.region}
        )

    def init_bucket(self, bucket_name: str):
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
