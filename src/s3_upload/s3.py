import s3fs
from dotenv import load_dotenv
import pyarrow.parquet as pq


class S3Manager:
    """Database connection manager with lazy configuration"""

    def __init__(self):
        self.s3_endpoint = "http://localhost:10456"
        self.fs = s3fs.S3FileSystem(
            anon=True,
            client_kwargs={'endpoint_url': self.s3_endpoint}
        )

        
    def __get_bucket_path(self, bucket_name) -> str:
        return f"/buckets/{bucket_name}"


    def create_bucket(self, bucket_name):
        BUCKET_PATH = self.__get_bucket_path(bucket_name)
        try:
            if not self.fs.exists(BUCKET_PATH):
                self.fs.mkdir(BUCKET_PATH)
                print(f"Bucket '{BUCKET_PATH}' created")
        except Exception as e:
            print(f"Error checking/creating bucket: {e}")
            raise e
        
        
    def write_as_parquet(self, bucket_name: str, file_name: str, content):
        BUCKET_PATH = self.__get_bucket_path(bucket_name)
        file_path = f"{BUCKET_PATH}/{file_name}" 
        with self.fs.open(file_path, 'wb') as f:
            pq.write_table(content, f)
        
        
    def read_parquet(self, bucket_name: str, file_name: str):
        BUCKET_PATH = self.__get_bucket_path(bucket_name)
        file_path = f"{BUCKET_PATH}/{file_name}" 
        with self.fs.open(file_path, "rb") as f:
            table = pq.read_table(f)
            return table
        

s3manager = S3Manager()
