import logging
from pathlib import Path
from dotenv import load_dotenv

from airflow_pipeline import sql_service
from airflow_pipeline import delivery_service
from s3_upload import s3_manager


logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
env_path = Path(__file__).resolve().parents[2] / "env"
required_envs = [
    'database_docker.env',
    'spark.env',
    's3_docker.env',
    'airflow.env',
]

for env_file in env_path.iterdir():
    if env_file.name in required_envs:
        logging.info(f"lading env file {env_file}")
        load_dotenv(env_file)


def main():
    # init sql
    logging.info('starting airflow pipeline')
    sql_service.init_sql()
    # init data, csv -> s3
    delivery_service.init_data_from_csv()

    # start http server:
    # - upload
    # - check

    # upload: once per 1 hour load files from s3 to postgres
    # data quality: check loaded amount


if __name__ == '__main__':
    main()
