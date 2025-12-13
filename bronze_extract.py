import requests
import json
import os
import time
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Initialize logger
log = logging.getLogger(__name__)

# Absolute path to configuration file
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


def extract_and_load_bronze(execution_date: str):
    """
    Extracts crypto prices from Coingecko API and loads raw JSON into Postgres Bronze table.
    The task is idempotent (skips insert if execution_date already exists).
    """
    log.info(f"[{execution_date}] Starting Bronze Extraction...")

    # Load configuration
    config = load_config()
    coingecko_cfg = config['api_source']['coingecko']
    bronze_table = config['data_warehouse']['bronze_table']

    # Build API request
    base_url = coingecko_cfg['base_url']
    endpoint = coingecko_cfg['endpoints']['simple_price']
    url = f"{base_url}{endpoint}"
    coin_ids = ",".join([coin['id'] for coin in coingecko_cfg['coins_to_track']])
    vs_currency = coingecko_cfg['vs_currency']
    params = {"ids": coin_ids, "vs_currencies": vs_currency}

    log.info(f"Extracting coins: {coin_ids} | VS: {vs_currency} from {url}")

    # Rate limiting
    sleep_sec = coingecko_cfg['rate_limit']['sleep_seconds_min']
    if sleep_sec > 0:
        log.info(f"Rate limit policy: Sleeping {sleep_sec}s before request.")
        time.sleep(sleep_sec)

    # Perform API call
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        raw_data = response.json()
    except requests.exceptions.RequestException as e:
        log.error(f"API Extraction failed due to connection/HTTP error: {e}")
        raise
    except json.JSONDecodeError as e:
        log.error(f"API Extraction failed: Invalid JSON response: {e}")
        raise

    log.info(f"Successfully extracted {len(raw_data)} coins.")

    # Connect to Postgres and load data idempotently
    hook = PostgresHook(postgres_conn_id='postgres_default')
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            # Create Bronze table if it doesn't exist
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {bronze_table} (
                    id SERIAL PRIMARY KEY,
                    execution_date DATE NOT NULL UNIQUE,
                    raw_json_payload JSONB,
                    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            conn.commit()

            # Insert data with ON CONFLICT for idempotency
            insert_query = f"""
                INSERT INTO {bronze_table} (execution_date, raw_json_payload)
                VALUES (%s, %s)
                ON CONFLICT (execution_date) DO NOTHING;
            """
            cur.execute(insert_query, (execution_date, json.dumps(raw_data)))
            conn.commit()

            rows_inserted = cur.rowcount
            if rows_inserted > 0:
                log.info(f"[{execution_date}] Data successfully loaded (1 row) into Bronze table: {bronze_table}")
            else:
                log.warning(f"[{execution_date}] No data inserted. Data for this execution date already exists in {bronze_table}. Skipping.")
