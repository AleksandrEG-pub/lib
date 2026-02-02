from datetime import datetime, timezone
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

    def get_first_file(self, bucket, prefix=None):
        kwargs = {"Bucket": bucket}
        if prefix:
            kwargs["Prefix"] = prefix
        resp = self._get_s3_client().list_objects_v2(**kwargs)
        if "Contents" not in resp:
            logging.info('bucket is empty')
            return None  # bucket / prefix empty
        first = min(obj["Key"] for obj in resp["Contents"])
        logging.info(f"found first file: {first}")
        return first
    
    def move_file(self, bucket_name: str, src_path: str, target_dir: str) -> bool:
        """
        Move a file within the same bucket from src_path to target directory
        Adds timestamp prefix to avoid filename collisions
        
        Args:
            bucket_name: Name of the S3 bucket
            src_path: Source file path in bucket (e.g., 'folder/file.txt')
            target_dir: Target directory path (e.g., 'processed/')
                       Must end with '/'
        
        Returns:
            bool: True if move successful, False otherwise
        
        Examples:
            move_file('my-bucket', 'uploads/file.txt', 'processed/')
            # Moves to 'processed/20250115_143045_file.txt'
            
            move_file('my-bucket', 'data/report.pdf', 'archived/')
            # Moves to 'archived/20250115_143045_report.pdf'
        """
        try:
            # 1. Validate inputs
            if not target_dir.endswith('/'):
                raise ValueError("target_dir must end with '/' (e.g., 'processed/')")
            
            # 2. Normalize paths
            src_path = src_path.strip('/')
            target_dir = target_dir.rstrip('/') + '/'  # Ensure single trailing slash
            
            # 3. Extract filename and generate new name with timestamp
            src_filename = os.path.basename(src_path)
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            
            # Add timestamp prefix (not suffix) to maintain file extension
            new_name = f"{timestamp}_{src_filename}"
            target_key = f"{target_dir}{new_name}"
            
            print(f"Moving: {src_path} -> {target_key}")
            
            # 4. Check if source file exists
            try:
                self._get_s3_client().head_object(Bucket=bucket_name, Key=src_path)
            except ClientError as e:
                if e.response['Error']['Code'] == '404':
                    print(f"Source file {src_path} not found in bucket {bucket_name}")
                    return False
                raise
            
            # 5. Check if target already exists (unlikely with timestamp but safe)
            try:
                self._get_s3_client().head_object(Bucket=bucket_name, Key=target_key)
                print(f"Warning: Target file {target_key} already exists, overwriting")
            except ClientError:
                pass  # Target doesn't exist, which is expected
            
            # 6. Copy object to new location
            copy_source = {
                'Bucket': bucket_name,
                'Key': src_path
            }
            
            self._get_s3_client().copy_object(
                Bucket=bucket_name,
                CopySource=copy_source,
                Key=target_key
            )
            print(f"Copied to: {target_key}")
            
            # 7. Delete original file
            self._get_s3_client().delete_object(
                Bucket=bucket_name,
                Key=src_path
            )
            print(f"Deleted original: {src_path}")
            
            print(f"Successfully moved {src_path} to {target_key}")
            return True
            
        except ValueError as e:
            print(f"Validation error: {str(e)}")
            return False
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_msg = e.response['Error']['Message']
            print(f"S3 Error ({error_code}): {error_msg}")
            return False
        except Exception as e:
            print(f"Unexpected error: {str(e)}")
            return False


s3manager: S3Manager = S3Manager()
