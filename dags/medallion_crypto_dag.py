import sys
import os
import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowException  # <-- required

# Ensure medallion package is importable
dag_dir = os.path.dirname(os.path.abspath(__file__))
if dag_dir not in sys.path:
    sys.path.append(dag_dir)

try:
    from medallion.bronze_extract import extract_and_load_bronze
    from medallion.silver_transform import transform_and_load_silver
    from medallion.gold_curation import curate_gold_metrics
except ImportError as e:
    logging.error(f"FATAL: Medallion modules missing at {dag_dir}. Error: {e}")
    extract_and_load_bronze = transform_and_load_silver = curate_gold_metrics = None

log = logging.getLogger("airflow.dag")


@dag(
    dag_id="medallion_crypto_pipeline_v2",
    start_date=datetime(2024, 1, 1),   # must be in the past
    schedule="@daily",                  # modern syntax
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data_engineer",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "depends_on_past": False,
        "email_on_failure": False,
    },
    tags=["production", "crypto", "medallion"],
    doc_md="""
    ### Crypto Medallion Pipeline
    - **Bronze**: Raw API ingestion
    - **Silver**: Schema normalization & cleansing
    - **Gold**: Business metrics (volatility, daily change)
    """
)
def medallion_pipeline():

    @task(retries=5, retry_delay=timedelta(minutes=10))
    def bronze_task(ds=None):
        if not extract_and_load_bronze:
            raise AirflowException("Import Error: bronze_extract logic missing.")
        return extract_and_load_bronze(ds)

    @task
    def silver_task(ds=None):
        if not transform_and_load_silver:
            raise AirflowException("Import Error: silver_transform logic missing.")
        return transform_and_load_silver(ds)

    @task
    def gold_task(ds=None):
        if not curate_gold_metrics:
            raise AirflowException("Import Error: gold_curation logic missing.")
        return curate_gold_metrics(ds)

    pipeline_complete = EmptyOperator(
        task_id="pipeline_complete",
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    bronze_data = bronze_task()
    silver_data = silver_task()
    gold_data = gold_task()

    bronze_data >> silver_data >> gold_data >> pipeline_complete


medallion_crypto_dag = medallion_pipeline()
