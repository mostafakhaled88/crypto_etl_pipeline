# medallion/gold_curation.py

from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
import json
import os

log = logging.getLogger(__name__)

# ---------------- Configuration ----------------
CONFIG_PATH = "/opt/airflow/config/api_config.json"

def load_config():
    """Reads the external configuration file."""
    try:
        with open(CONFIG_PATH, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        log.error(f"Configuration file not found at: {CONFIG_PATH}")
        raise
    except json.JSONDecodeError as e:
        log.error(f"Error decoding JSON configuration file: {e}")
        raise
# -------------------------------------------------

def curate_gold_metrics(execution_date: str):
    """
    Aggregates cleaned data from the Silver layer into daily metrics 
    and loads the results into the Gold layer.
    """
    log.info(f"[{execution_date}] Starting Gold Curation...")

    # Load configuration for table names
    config = load_config()
    data_cfg = config['data_warehouse']
    silver_table = data_cfg.get('silver_table', 'silver_clean_prices')
    gold_table = data_cfg.get('gold_table', 'gold_daily_metrics')

    # Ensure execution_date is a YYYY-MM-DD string
    metric_date = execution_date

    hook = PostgresHook(postgres_conn_id='postgres_default')

    with hook.get_conn() as conn:
        with conn.cursor() as cur:

            # 1️⃣ Create Gold table if not exists
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {gold_table} (
                    metric_date DATE NOT NULL,
                    coin_symbol TEXT NOT NULL,
                    avg_price_usd NUMERIC,
                    max_price_usd NUMERIC,
                    PRIMARY KEY(metric_date, coin_symbol)
                );
            """)
            conn.commit()
            log.info(f"Ensured Gold table exists: {gold_table}")

            # 2️⃣ Aggregate from Silver and upsert into Gold
            log.info(f"Aggregating Silver data from {silver_table} for {metric_date}")

            sql_query = f"""
                INSERT INTO {gold_table} (metric_date, coin_symbol, avg_price_usd, max_price_usd)
                SELECT
                    DATE(%s) as metric_date,
                    coin_symbol,
                    AVG(price_usd) as avg_price_usd,
                    MAX(price_usd) as max_price_usd
                FROM {silver_table}
                WHERE DATE(source_timestamp) = DATE(%s)
                GROUP BY coin_symbol
                ON CONFLICT (metric_date, coin_symbol) DO UPDATE
                SET avg_price_usd = EXCLUDED.avg_price_usd,
                    max_price_usd = EXCLUDED.max_price_usd;
            """

            cur.execute(sql_query, (metric_date, metric_date))
            row_count = cur.rowcount
            conn.commit()

    log.info(f"[{metric_date}] Successfully curated and loaded/updated {row_count} rows into Gold layer: {gold_table}")
    return row_count
