import logging
from pathlib import Path
from dotenv import load_dotenv

from airflow_pipeline import sql_service
from airflow_pipeline import delivery_service 


logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
env_path = Path(__file__).resolve().parents[2] / "env"
required_envs = [
    'database_docker.env',
    'spark.env',
    's3_docker.env',
    'airflow.env',
    'tg.env',
]

for env_file in env_path.iterdir():
    if env_file.name in required_envs:
        logging.info(f"lading env file {env_file}")
        load_dotenv(env_file)

from airflow_pipeline.http_server import server
def main():
    logging.info('starting airflow pipeline')
    sql_service.init_sql()
    delivery_service.init_data_from_csv()
    server.start_server()

if __name__ == '__main__':
    main()
