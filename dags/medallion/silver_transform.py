import logging
import json
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException
from psycopg2.extras import execute_batch

log = logging.getLogger("airflow.task")

CONFIG_PATH = "/opt/airflow/config/api_config.json"


# -----------------------------
# CONFIG LOADER
# -----------------------------
def load_config():
    try:
        with open(CONFIG_PATH, "r") as f:
            return json.load(f)
    except Exception as e:
        log.error(f"Critical Error: Could not load config at {CONFIG_PATH}: {e}")
        raise AirflowException("Transformation failed: Config missing.")


# -----------------------------
# PAYLOAD PARSING
# -----------------------------
def parse_payload(source_name, raw_payload, vs_currencies):
    """
    Normalizes API schemas into unified tuples:
    (coin_id, currency, price)
    """

    extracted = []

    # Ensure payload is dict
    if isinstance(raw_payload, str):
        raw_payload = json.loads(raw_payload)

    # ---------------------------
    # CoinGecko Bronze Format:
    # { "bitcoin": {"usd": 40000, "eur": 38000}, ... }
    # ---------------------------
    if source_name == "coingecko":

        for coin_id, price_map in raw_payload.items():
            for curr in vs_currencies:
                price = price_map.get(curr.lower())

                if price is None:
                    continue

                extracted.append(
                    (coin_id, curr.upper(), float(price))
                )

    # ---------------------------
    # CoinCap Bronze Format (your pipeline)
    # Bronze stores records per asset, ex:
    # {
    #   "bitcoin": {"id": "bitcoin", "priceUsd": "40230.12"},
    #   "ethereum": {...}
    # }
    # ---------------------------
    elif source_name == "coincap":

        for coin_id, data in raw_payload.items():

            price = data.get("priceUsd")

            if not price:
                continue

            extracted.append(
                (coin_id, "USD", float(price))
            )

    return extracted



# -----------------------------
# SILVER TRANSFORM TASK
# -----------------------------
def transform_and_load_silver(ds: str, **kwargs):
    """
    Bronze → Silver transformation
    - normalizes schemas
    - inserts batch records
    - preserves idempotency
    """

    config = load_config()

    bronze_table = config["data_warehouse"]["bronze_table"]
    silver_table = config["data_warehouse"]["silver_table"]

    hook = PostgresHook(postgres_conn_id="postgres_default")

    # ---------------------------------
    # Fetch Bronze payloads for date
    # ---------------------------------
    query = f"""
        SELECT source_name, raw_json_payload, extracted_at
        FROM {bronze_table}
        WHERE logical_date = %s;
    """

    records = hook.get_records(query, parameters=(ds,))

    if not records:
        log.warning(f"[{ds}] No Bronze records found — skipping silver stage.")
        return

    transformed_rows = []
    processed_at = datetime.utcnow()

    # ---------------------------------
    # Normalize per source
    # ---------------------------------
    for source_name, raw_payload, extracted_at in records:

        source_cfg = config["api_source"].get(source_name, {})
        vs_currencies = source_cfg.get("vs_currencies", ["usd"])

        parsed = parse_payload(source_name, raw_payload, vs_currencies)

        for coin_id, currency, price in parsed:
            transformed_rows.append((
                coin_id,
                currency,
                price,
                source_name,
                extracted_at,
                processed_at
            ))

    if not transformed_rows:
        log.warning(f"[{ds}] Silver transformation produced 0 rows.")
        return

    # ---------------------------------
    # Silver Table Schema + UPSERT
    # ---------------------------------
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {silver_table} (
            coin_id TEXT NOT NULL,
            vs_currency VARCHAR(10) NOT NULL,
            price NUMERIC(20,10) NOT NULL,
            source_name TEXT NOT NULL,
            source_timestamp TIMESTAMP NOT NULL,
            processed_timestamp TIMESTAMP,
            PRIMARY KEY (coin_id, vs_currency, source_timestamp, source_name)
        );
    """

    upsert_sql = f"""
        INSERT INTO {silver_table} (
            coin_id, vs_currency, price,
            source_name, source_timestamp, processed_timestamp
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (coin_id, vs_currency, source_timestamp, source_name)
        DO UPDATE SET
            price = EXCLUDED.price,
            processed_timestamp = EXCLUDED.processed_timestamp;
    """

    with hook.get_conn() as conn:
        with conn.cursor() as cur:

            cur.execute(create_sql)

            execute_batch(
                cur,
                upsert_sql,
                transformed_rows,
                page_size=500
            )

    log.info(
        f"[{ds}] Silver Load Complete — {len(transformed_rows)} rows written "
        f"into {silver_table}"
    )
