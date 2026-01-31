import logging
import os
import s3fs
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pa_csv


class S3Manager:

    def __init__(self):
        self.endpoint = os.getenv('S3_ENDPOINT')
        self.region = os.getenv('S3_REGION')
        key=os.getenv("AWS_ACCESS_KEY_ID"),
        secret=os.getenv("AWS_SECRET_ACCESS_KEY"),
        self.fs: s3fs.S3FileSystem = s3fs.S3FileSystem(
            anon=True,
            key=key,
            secret=secret,
            client_kwargs={'endpoint_url': self.endpoint,
                           'region_name': self.region,}
        )

    def init_bucket(self, bucket_name: str):
        try:
            fs: s3fs.S3FileSystem = self.fs
            if not fs.exists(bucket_name):
                fs.mkdir(bucket_name)
                logging.info(f"Bucket '{bucket_name}' created")
        except Exception as e:
            logging.error(f"Error checking/creating bucket: {e}")
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

    def write_as_csv(self, bucket_name: str, file_name: str, content: pa.Table):
        file_path = f"{bucket_name}/{file_name}"
        with self.fs.open(file_path, 'wb') as f:
            pa_csv.write_csv(content, f)
            logging.info(f"written file {file_path} to bucket {bucket_name}")

    def list_files(self, bucket_name: str, processed_directory: str):
        """
        Returns a list of file paths inside bucket/processed_directory
        """
        path = f"{bucket_name}/{processed_directory}".rstrip("/")
        if not self.fs.exists(path):
            return []
        return [
            p for p in self.fs.ls(path) if not p.endswith("/")
        ]

    def count_file_lines(self, bucket_name: str, file_name: str) -> int:
        """
        Counts number of records in a CSV file (excluding header)
        """
        file_path = f"{bucket_name}/{file_name}"
        with self.fs.open(file_path, "rb") as f:
            table = pa_csv.read_csv(f)
        return table.num_rows
    

    def move_file(self, bucket_name: str, src_path: str, target_dir: str):
        src = f"{bucket_name}/{src_path}"
        dst = f"{bucket_name}/{target_dir.rstrip('/')}/{os.path.basename(src_path)}"

        self.fs.copy(src, dst)
        self.fs.rm(src)

        
s3manager: S3Manager = S3Manager()
