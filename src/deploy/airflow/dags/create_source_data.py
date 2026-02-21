from datetime import datetime
from airflow.sdk import DAG, task

from modules import env_manager
from modules.create_source_data import create_source_data

with DAG(dag_id="source_data",
         start_date=datetime(2025, 1, 1),
         schedule=None,
         catchup=False
         ) as dag:
   
    @task()
    def create_source():
        env_manager.init_env(['s3.env'])
        create_source_data()
          
    create_source()
