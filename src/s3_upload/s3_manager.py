import logging
import os
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pa_csv
import pyarrow.fs as fs
import boto3
from botocore.client import Config


class S3Manager:

    # ---------- infra ----------

    def _s3_client(self):
        return boto3.client(
            "s3",
            endpoint_url="http://seaweedfs:8333",
            aws_access_key_id="admin",
            aws_secret_access_key="key",
            config=Config(connect_timeout=30, read_timeout=30, retries={"max_attempts": 1})
        )

    def _fs(self) -> fs.S3FileSystem:
        return fs.S3FileSystem(
            endpoint_override="http://seaweedfs:8333",
            access_key="admin",
            secret_key="key",
            scheme="http"
        )

    # ---------- bucket ----------

    def init_bucket(self, bucket_name: str):
        s3 = self._s3_client()
        existing = {b["Name"] for b in s3.list_buckets()["Buckets"]}
        if bucket_name not in existing:
            logging.info(f"creating bucket {bucket_name}")
            s3.create_bucket(Bucket=bucket_name)

    # ---------- parquet ----------

    def write_parquet(self, bucket: str, file_name: str, table: pa.Table):
        fsys = self._fs()
        path = f"{bucket}/{file_name}"
        with fsys.open_output_stream(path) as out:
            pq.write_table(table, out)
            

    def read_parquet(self, bucket: str, key: str) -> pa.Table:
        fsys = self._fs()
        path = f"{bucket}/{key}"
        with fsys.open_input_stream(path) as inp:
            table = pq.read_table(inp)
        return self._convert_numeric_to_decimal(table)

    # ---------- csv ----------

    def write_csv(self, bucket: str, key: str, table: pa.Table):
        fsys = self._fs()
        path = f"{bucket}/{key}"
        with fsys.open_output_stream(path) as out:
            pa_csv.write_csv(table, out)

    def read_csv(self, bucket: str, key: str) -> pa.Table:
        fsys = self._fs()
        path = f"{bucket}/{key}"
        with fsys.open_input_stream(path) as inp:
            return pa_csv.read_csv(inp)

    # ---------- listing ----------

    def list_files(self, bucket: str, prefix: str = "") -> list[str]:
        fsys = self._fs()
        base = f"{bucket}/{prefix}".rstrip("/") + "/"
        selector = fs.FileSelector(base, recursive=False)
        infos = fsys.get_file_info(selector)
        return [
            info.path.replace(f"{bucket}/", "")
            for info in infos
            if info.type == fs.FileType.File
        ]

    def get_first_root_file(self, bucket: str) -> str | None:
        files = self.list_files(bucket)
        return files[0] if files else None

    # ---------- utils ----------

    def count_file_lines(self, bucket: str, key: str) -> int:
        table = self.read_csv(bucket, key)
        return table.num_rows

    def move_file(self, bucket: str, src: str, target_dir: str):
        fsys = self._fs()
        dst = f"{bucket}/{target_dir.rstrip('/')}/{os.path.basename(src)}"
        fsys.move(f"{bucket}/{src}", dst)



s3manager: S3Manager = S3Manager()
