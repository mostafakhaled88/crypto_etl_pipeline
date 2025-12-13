from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import logging

# Use standard logger for better integration with Airflow logs
log = logging.getLogger(__name__)

# --- Assuming the scripts below are available in the PYTHON PATH ---
# (They should be accessible if placed in the mounted /opt/airflow/scripts 
# and properly packaged or configured)
# NOTE: Removed manual sys.path manipulation.
from medallion.bronze_extract import extract_and_load_bronze
from medallion.silver_transform import transform_and_load_silver
from dags.medallion.gold_curation import curation_gold_metrics


@dag(
    dag_id="medallion_crypto_pipeline",
    start_date=datetime(2025, 12, 10),
    schedule_interval="@daily",
    default_args={
        "owner": "data_engineer",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": False, # Set to True and provide email for alerts
        "email": ["your_alert_address@example.com"],
    },
    catchup=False,
    tags=["medallion", "crypto", "postgres", "etl"],
    doc_md="""
# 💎 Medallion Crypto Data Pipeline
This DAG implements the three-stage Medallion Architecture (Bronze -> Silver -> Gold)
for daily crypto data processing.

**Target Execution Date (ds):** `{{ ds }}`
**Schedule:** Daily
"""
)
def medallion_pipeline():

    @task(task_id="bronze_extraction")
    def run_bronze_extract(ds: str) -> None:
        """
        Extract raw data from source (API) and load into the Bronze table.
        Uses the `ds` (execution date) provided by Airflow context.
        """
        log.info(f"Starting Bronze extraction for date: {ds}")
        
        # This function should use the Airflow Postgres Hook 
        # (e.g., conn_id='postgres_default')
        extract_and_load_bronze(ds)
        
        log.info(f"Bronze extraction completed for date: {ds}")

    @task(task_id="silver_transformation")
    def run_silver_transform(ds: str) -> None:
        """
        Transform Bronze data (clean, deduplicate) and load into the Silver table.
        """
        log.info(f"Starting Silver transformation for date: {ds}")
        
        # This function should read from Bronze and write to Silver using the Hook
        transform_and_load_silver(ds)
        
        log.info(f"Silver transformation completed for date: {ds}")

    @task(task_id="gold_curation")
    def run_gold_curate(ds: str) -> None:
        """
        Curate Silver data (aggregate, calculate KPIs) into Gold metrics tables.
        """
        log.info(f"Starting Gold curation for date: {ds}")
        
        # This function should read from Silver and write final metrics to Gold
        curation_gold_metrics(ds)
        
        log.info(f"Gold curation completed for date: {ds}")

    # Define a final success point for clear monitoring
    pipeline_complete = EmptyOperator(task_id='pipeline_complete')
    
    # --- DAG Flow: Explicit Dependencies ---
    
    
    # 1. Instantiate the tasks by calling the decorated functions
    # (These functions were defined earlier in the DAG: run_bronze_extract, etc.)
    bronze_task = run_bronze_extract(ds="{{ ds }}")
    silver_task = run_silver_transform(ds="{{ ds }}")
    gold_task = run_gold_curate(ds="{{ ds }}")

    # 2. Define the flow using bitshift operators (>>). 
    # This enforces the sequence: Bronze -> Silver -> Gold -> Complete.
    bronze_task >> silver_task >> gold_task >> pipeline_complete
# Instantiate DAG
medallion_pipeline()