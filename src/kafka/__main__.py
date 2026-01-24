import logging
import os
from dotenv import load_dotenv
from pathlib import Path
import kafka.data_service as data_service
import kafka.sql_service as sql_service

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")
env_path = Path(__file__).resolve().parents[2] / "env"
for env_file in env_path.iterdir():
    if env_file.name.count('docker') == 0:
        logging.info(f"lading env file {env_file}")
        load_dotenv(env_file)

def main():
    sql_service.init_sql()
    data_service.init_data()
    data_service.upload_from_database_to_kafka()
    data_service.sink_from_kafka_to_database()

if __name__ == '__main__':
    main()