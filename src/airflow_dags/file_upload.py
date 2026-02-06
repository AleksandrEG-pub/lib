from datetime import datetime, timedelta
import json

from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.utils.trigger_rule import TriggerRule

with DAG(
    dag_id="airflow-pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="* 0 * * *",
    catchup=False,
) as dag:

    # -------- main pipeline --------
    upload_from_s3_to_postgres = HttpOperator(
        task_id="upload_from_s3_to_postgres",
        http_conn_id="backend",
        endpoint="/upload",
        method="POST",
        retries=2,
        retry_delay=timedelta(seconds=30),
    )

    upload_check = HttpOperator(
        task_id="upload_check",
        http_conn_id="backend",
        endpoint="/validate-last-upload",
        method="GET",
    )

    # -------- notifications --------
    notify_success = HttpOperator(
        task_id="notify_success",
        http_conn_id="backend",
        endpoint="/notification",
        method="POST",
        headers={"Content-Type": "application/json"},
        data=json.dumps({
            "dag_id": "{{ dag.dag_id }}",
            "dag_execution_time": "{{ ts }}",
            "result": "OK",
            "details": "All tasks completed successfully",
        }),
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    notify_failure = HttpOperator(
        task_id="notify_failure",
        http_conn_id="backend",
        endpoint="/notification",
        method="POST",
        headers={"Content-Type": "application/json"},
        data=json.dumps({
            "dag_id": "{{ dag.dag_id }}",
            "dag_execution_time": "{{ ts }}",
            "result": "NOK",
            "details": (
                "task={{ ti.task_id }}\n"
                "try={{ ti.try_number }}\n"
                "state={{ ti.state }}"
            ),
        }),
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # -------- flow --------
    upload_from_s3_to_postgres >> upload_check
    [upload_from_s3_to_postgres, upload_check] >> notify_success
    [upload_from_s3_to_postgres, upload_check] >> notify_failure
