from datetime import datetime
from airflow.sdk import DAG, task
from modules import init_structure
from modules import env_manager

with DAG(dag_id="init_structure",
         start_date=datetime(2025, 1, 1),
         schedule=None,
         catchup=False
         ) as dag:
    
    @task()
    def init_tables():
        env_manager.init_env(['database.env'])
        init_structure.init_tables()
        
    init_tables()
