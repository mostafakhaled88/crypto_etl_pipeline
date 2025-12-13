# scripts/silver_transform.py

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
from datetime import datetime
import json
import os

log = logging.getLogger(__name__)

# Configuration file path
# Before (Relative Path Calculation)
# CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'config', 'api_config.json')

# After (Absolute Path, consistent with docker-compose.yml mount)
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

def transform_and_load_silver(run_date: str):
    """
    Reads the Bronze data for the run_date,
    flattens, cleans, and loads into the Silver layer
    with idempotent upserts using ON CONFLICT.
    """
    log.info(f"Starting Silver Transformation for date: {run_date}")

    config = load_config()
    data_cfg = config['data_warehouse']
    
    bronze_table = data_cfg.get('bronze_table', 'bronze_raw_prices')
    silver_table = data_cfg.get('silver_table', 'silver_clean_prices')

    hook = PostgresHook(postgres_conn_id='postgres_default')

    # 1️⃣ Read Bronze data for the execution date
    sql_query = f"""
        SELECT raw_json_payload, extracted_at
        FROM {bronze_table}
        WHERE execution_date = %s;
    """
    
    records = hook.get_records(sql_query, parameters=(run_date,))
    
    if not records:
        log.warning(f"No Bronze data found for date: {run_date}. Skipping Silver transformation.")
        return

    raw_payload, extracted_at = records[0]

    # 2️⃣ Transform: flatten JSON into rows
    transformed_records = [
        {
            'coin_symbol': coin,
            'price_usd': prices.get('usd'),
            'source_timestamp': extracted_at,
            'processed_timestamp': datetime.now()
        }
        for coin, prices in raw_payload.items()
    ]

    df = pd.DataFrame(transformed_records)
    original_len = len(df)
    log.info(f"Flattened {original_len} rows from JSON payload.")

    # 3️⃣ Clean: remove invalid or missing price_usd
    df['price_usd'] = pd.to_numeric(df['price_usd'], errors='coerce')
    df.dropna(subset=['price_usd'], inplace=True)

    if len(df) < original_len:
        log.warning(f"Dropped {original_len - len(df)} rows due to missing or invalid price_usd.")

    if df.empty:
        log.error(f"No valid data after cleaning. Aborting Silver load.")
        return

    # 4️⃣ Load: Create table with unique constraint
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {silver_table} (
                    id SERIAL PRIMARY KEY,
                    coin_symbol TEXT NOT NULL,
                    price_usd NUMERIC NOT NULL,
                    source_timestamp TIMESTAMP NOT NULL,
                    processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (coin_symbol, source_timestamp)
                );
            """)
            conn.commit()

            # Upsert each row using ON CONFLICT
            for row in df.itertuples(index=False):
                cur.execute(f"""
                    INSERT INTO {silver_table} (coin_symbol, price_usd, source_timestamp, processed_timestamp)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (coin_symbol, source_timestamp) 
                    DO UPDATE SET
                        price_usd = EXCLUDED.price_usd,
                        processed_timestamp = EXCLUDED.processed_timestamp;
                """, (row.coin_symbol, row.price_usd, row.source_timestamp, row.processed_timestamp))
            
            conn.commit()

    log.info(f"Successfully loaded {len(df)} rows into Silver layer: {silver_table}.")
