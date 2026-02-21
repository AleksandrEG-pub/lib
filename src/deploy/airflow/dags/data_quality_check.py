

## airflow dags:
# freshness: last record written less than 24 hours ago
# fullness: source is -+5% from target by record count
# save results in separate table 'data_quality_checks' with columns: check_name, status, value, timestamp

from datetime import datetime
from airflow.sdk import DAG, task
from airflow.providers.standard.operators.bash import BashOperator

with DAG(dag_id="quality_ckeck_data_upload",
         start_date=datetime(2025, 1, 1),
         schedule="*/1 * * * *",
         catchup=False
         ) as dag:
    
    @task()
    def freshness():
        print("echo freshness")
    
    @task()
    def fullness():
        print("echo fullness")

    freshness() >> fullness()
