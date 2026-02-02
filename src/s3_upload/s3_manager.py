import io
import logging
import os
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError


class S3Manager:
    def _get_s3_client(self):
        return boto3.client('s3',
            endpoint_url=os.getenv('S3_ENDPOINT'),  # SeaweedFS S3 gateway
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),   # From SeaweedFS setup
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),  # From SeaweedFS setup
            config=Config(
                signature_version='s3v4',
                s3={'addressing_style': 'path'}  # or 'virtual' depending on setup
            )
        )
    
    def init_bucket_boto(self, bucket_name: str):
        s3_client = self._get_s3_client()
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            logging.info(f"bucket {bucket_name} exist")
            return
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code in ("404", "NoSuchBucket"):
                logging.info(f"bucket {bucket_name} not exist")
                s3_client.create_bucket(Bucket=bucket_name)
                logging.info(f"Bucket '{bucket_name}' created")
            else:
                raise

    def write_as_parquet(self, bucket_name: str, file_name: str, content: pa.Table):
        buf = io.BytesIO()
        pq.write_table(content, buf)
        buf.seek(0)
        self._get_s3_client().upload_fileobj(
            buf,
            bucket_name,
            file_name
        )
        logging.info(f"uploaded file {file_name} to bucket {bucket_name}")


    def read_parquet(self, bucket_name: str, file_name: str) -> pa.Table:
        file_path = f"{bucket_name}/{file_name}"
        with self.fs.open(file_path, "rb") as f:
            table: pa.Table = pq.read_table(f)
            table = self._convert_numeric_to_decimal(table)
            return table


s3manager: S3Manager = S3Manager()
