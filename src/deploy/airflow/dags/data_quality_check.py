from datetime import datetime
from pathlib import Path
from airflow.sdk import DAG, task
from modules import env_manager
from modules import quality_check
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(dag_id="quality_ckeck_data_upload",
         start_date=datetime(2025, 1, 1),
         schedule="55 * * * *",
         catchup=False
         ) as dag:
    
    freshness_query_file = Path(__file__).parent / "modules" / "sql" / "freshness-check.sql"
    freshness_query = freshness_query_file.read_text()
    freshness_task = SQLExecuteQueryOperator(
        task_id='freshness',
        conn_id='it_one_postgres_connection',
        sql=freshness_query,
    )
    
    @task()
    def fullness():
        env_manager.init_env(['database.env', 's3.env'])
        quality_check.fullness()
    
    fullness_task = fullness()
