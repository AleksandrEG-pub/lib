from datetime import datetime
from pathlib import Path
from airflow.sdk import DAG, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


with DAG(dag_id="upload_employee_from_raw_to_stage",
         start_date=datetime(2025, 1, 1),
         schedule='30 * * * *',
         catchup=False
         ) as dag:
    script = Path(__file__).parent / "modules" / "sql" / "raw-stage-employees.sql"
    query = script.read_text()
    
    insert_task = SQLExecuteQueryOperator(
        task_id="my_postgres_query",
        conn_id="it_one_postgres_connection", 
        sql=query,
    )
    insert_task
    