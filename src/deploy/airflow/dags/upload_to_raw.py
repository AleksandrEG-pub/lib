from datetime import datetime
import logging
from airflow.sdk import DAG, task

from modules import env_manager
from modules import upload_source_data

with DAG(dag_id="upload_employee_from_source_to_raw",
         start_date=datetime(2025, 1, 1),
         schedule="0 * * * *",
         catchup=False
         ) as dag:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    @task()
    def upload():
        env_manager.init_env(['database.env', 's3.env'])
        upload_source_data.upload_data()
        
    upload()
