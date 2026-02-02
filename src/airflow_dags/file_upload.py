from datetime import datetime, timedelta

from airflow.sdk import DAG, task
from airflow.providers.http.operators.http import HttpOperator

with DAG(dag_id="airflow-pipeline", start_date=datetime(2025, 1, 1), schedule="*/1 * * * *") as dag:
    upload_from_s3_to_postgres = HttpOperator(
        task_id="upload_from_s3_to_postgres",
        http_conn_id="backend",
        endpoint="/test",
        method="GET",
        # timeout=60,                 # must be > expected runtime
        retries=2,
        retry_delay=timedelta(seconds=30),
    )
    upload_check = HttpOperator(
        task_id="upload_check",
        http_conn_id="backend",
        endpoint="/test2",
        method="GET",
    )

    upload_from_s3_to_postgres >> upload_check
