import requests
import json
import time
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException

log = logging.getLogger("airflow.task")

CONFIG_PATH = "/opt/airflow/config/api_config.json"


# -------------------------------
# CONFIG LOADER
# -------------------------------
def load_pipeline_config():
    """Loads JSON configuration with validation."""
    try:
        with open(CONFIG_PATH, "r") as f:
            config = json.load(f)

        if "api_source" not in config:
            raise AirflowException("Config missing: api_source block")

        if "data_warehouse" not in config:
            raise AirflowException("Config missing: data_warehouse block")

        return config

    except FileNotFoundError:
        log.error(f"Config file not found at {CONFIG_PATH}")
        raise AirflowException("Pipeline Configuration Missing")


# -------------------------------
# API REQUEST ROUTER
# -------------------------------
def extract_from_api(source_key: str, config: dict):
    """
    Handles API requests.
    Supports provider-specific formats for CoinGecko + CoinCap.
    """
    api_cfg = config["api_source"].get(source_key)

    if not api_cfg:
        raise AirflowException(f"Source '{source_key}' missing in config")

    base_url = api_cfg["base_url"]
    endpoints = api_cfg.get("endpoints", {})
    coins = api_cfg.get("coins_to_track", [])

    if not coins:
        raise AirflowException(f"No coins defined for source '{source_key}'")

    results = {}

    # -------------------------------
    # COINGECKO (Batch Request)
    # -------------------------------
    if source_key == "coingecko":
        endpoint = endpoints.get("simple_price")
        if not endpoint:
            raise AirflowException("Coingecko endpoint 'simple_price' missing")

        url = f"{base_url}{endpoint}"

        params = {
            "ids": ",".join(c["id"] for c in coins),
            "vs_currencies": ",".join(api_cfg.get("vs_currencies", ["usd"]))
        }

        request_batches = [(url, params, None)]

    # -------------------------------
    # COINCAP (One request per asset)
    # -------------------------------
    elif source_key == "coincap":
        endpoint = endpoints.get("assets")
        if not endpoint:
            raise AirflowException("CoinCap endpoint 'assets' missing")

        base_asset_url = f"{base_url}{endpoint}"

        # create one call per coin
        request_batches = [
            (f"{base_asset_url}/{c['id']}", {}, c["id"])
            for c in coins
        ]

    else:
        raise AirflowException(f"Unsupported API source '{source_key}'")



    # -------------------------------
    # RATE LIMITING
    # -------------------------------
    delay = api_cfg.get("rate_limit", {}).get("sleep_seconds_min", 1.0)

    retry_limit = config.get("pipeline_settings", {}).get("retry_attempts", 3)
    backoff = config.get("pipeline_settings", {}).get("retry_delay_seconds", 5)



    # -------------------------------
    # REQUEST EXECUTION LOOP
    # -------------------------------
    with requests.Session() as session:
        for url, params, asset_id in request_batches:

            time.sleep(delay)

            for attempt in range(retry_limit):
                try:
                    log.info(
                        f"[{source_key}] Requesting "
                        f"{asset_id if asset_id else 'batch'} "
                        f"(attempt {attempt+1})"
                    )

                    response = session.get(url, params=params, timeout=20)
                    response.raise_for_status()
                    data = response.json()

                    # CoinCap data wrapper fix
                    if source_key == "coincap":
                        results[asset_id] = data.get("data", {})
                    else:
                        results = data

                    break

                except requests.exceptions.RequestException as e:
                    if attempt == retry_limit - 1:
                        log.error(f"All retries failed for {source_key}: {e}")
                        raise

                    log.warning(
                        f"Retry {attempt+1}/{retry_limit} "
                        f"for {source_key} after error: {e}"
                    )
                    time.sleep(backoff)

    return results



# -------------------------------
# BRONZE LAYER LOADER
# -------------------------------
def extract_and_load_bronze(ds: str):
    """
    Extracts raw API payloads and loads them to the Bronze table.
    Idempotent UPSERT using (logical_date, source_name).
    """
    config = load_pipeline_config()

    table_name = config["data_warehouse"]["bronze_table"]
    hook = PostgresHook(postgres_conn_id="postgres_default")

    # -------------------------------
    # ENSURE TABLE EXISTS
    # -------------------------------
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        logical_date DATE,
        source_name TEXT,
        raw_json_payload JSONB NOT NULL,
        extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (logical_date, source_name)
    );
    """
    hook.run(create_table_sql)

    # -------------------------------
    # PROCESS EACH SOURCE
    # -------------------------------
    for source_key in config["api_source"].keys():
        try:
            log.info(f"---- Extracting from source: {source_key} ----")

            raw_data = extract_from_api(source_key, config)

            upsert_sql = f"""
            INSERT INTO {table_name} (logical_date, source_name, raw_json_payload)
            VALUES (%s, %s, %s::jsonb)
            ON CONFLICT (logical_date, source_name)
            DO UPDATE SET
                raw_json_payload = EXCLUDED.raw_json_payload,
                extracted_at = CURRENT_TIMESTAMP;
            """

            hook.run(
                upsert_sql,
                parameters=(ds, source_key, json.dumps(raw_data))
            )

            log.info(f"[SUCCESS] Stored Bronze payload for {source_key} ({ds})")

        except Exception as e:
            log.error(f"[FAILED] Bronze ingestion for {source_key}: {e}")
            continue

    log.info("Bronze extraction task complete.")
