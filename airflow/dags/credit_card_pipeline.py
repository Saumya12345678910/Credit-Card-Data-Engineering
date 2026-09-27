from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}
with DAG(
    dag_id="credit_card_data_pipeline",
    default_args=default_args,
    description="Orchestrates Bronze, Silver and Gold processing in Databricks",
    start_date=datetime(2026, 9, 27),
    schedule=None,
    catchup=False,
    tags=["databricks", "credit-card", "data-engineering"],
) as dag:
    bronze_task = DatabricksRunNowOperator(
        task_id="bronze_task",
        databricks_conn_id="databricks_default",
        job_id=270566043657775,
    )
    silver_task = DatabricksRunNowOperator(
        task_id="silver_task",
        databricks_conn_id="databricks_default",
        job_id=263254663004684,
    )

    gold_task = DatabricksRunNowOperator(
        task_id="gold_task",
        databricks_conn_id="databricks_default",
        job_id=462239758695391,
    )
    validation_task = DatabricksRunNowOperator(
        task_id = "validation_task",
        databricks_conn_id = "databricks_default",
        job_id = 640869713300491,
    )
    bronze_task >> silver_task >> gold_task >> validation_task
