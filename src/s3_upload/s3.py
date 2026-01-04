import os
import s3fs
from dotenv import load_dotenv

# Configuration
S3_ENDPOINT = "http://localhost:8333"  # Default SeaweedFS S3
ACCESS_KEY = "some_access_key"         # Optional
SECRET_KEY = "some_secret_key"         # Optional
BUCKET_NAME = "your-bucket-name"


class S3Manager:
    """Database connection manager with lazy configuration"""

    def __init__(self):
        self._fs = None

    @property
    def config(self):
        """Lazy-loaded configuration property"""
        # if self._config is None:
            # load_dotenv()
            # self._config = {
                # 'dbname': os.getenv('POSTGRES_DB'),
                # 'user': os.getenv('POSTGRES_USER'),
                # 'password': os.getenv('POSTGRES_PASSWORD'),
                # 'host': os.getenv('DB_HOST'),
                # 'port': os.getenv('DB_PORT')
            # }
        fs = s3fs.S3FileSystem(
            anon=True,
            endpoint_url="http://seaweedfs:10456"
        )
        return self._config

# # List files in bucket
# files = fs.ls(f'{BUCKET_NAME}/')
# for f in files:
#     print(f)

# # List with pattern
# csv_files = fs.glob(f'{BUCKET_NAME}/*.csv')

# # Check if file exists
# if fs.exists(f'{BUCKET_NAME}/file.txt'):
#     print("File exists!")

# # Get file info
# info = fs.info(f'{BUCKET_NAME}/file.txt')
# print(f"Size: {info['size']} bytes")


# # Read text file    
# with fs.open(f'{BUCKET_NAME}/file.txt', 'r') as f:
#     content = f.read()
#     print(content)

# # Read binary file
# with fs.open(f'{BUCKET_NAME}/image.png', 'rb') as f:
#     image_data = f.read()

# # Read line by line
# with fs.open(f'{BUCKET_NAME}/log.txt', 'r') as f:
#     for line in f:
#         print(line.strip())
        
        
        
# # Write text file
# with fs.open(f'{BUCKET_NAME}/output.txt', 'w') as f:
#     f.write('Hello SeaweedFS S3!')

# # Write binary file
# with fs.open(f'{BUCKET_NAME}/data.bin', 'wb') as f:
#     f.write(b'\x00\x01\x02\x03')

# # Append to file
# with fs.open(f'{BUCKET_NAME}/log.txt', 'a') as f:
#     f.write('New log entry\n')
    
    

# # Copy files
# fs.copy(f'{BUCKET_NAME}/source.txt', f'{BUCKET_NAME}/dest.txt')

# # Move/Rename files
# fs.move(f'{BUCKET_NAME}/old.txt', f'{BUCKET_NAME}/new.txt')

# # Delete files
# fs.rm(f'{BUCKET_NAME}/file.txt')

# # Delete multiple files
# fs.rm([f'{BUCKET_NAME}/file1.txt', f'{BUCKET_NAME}/file2.txt'])

# # Delete directory recursively
# fs.rm(f'{BUCKET_NAME}/folder/', recursive=True)