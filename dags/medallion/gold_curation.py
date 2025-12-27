import json
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException

log = logging.getLogger("airflow.task")
CONFIG_PATH = "/opt/airflow/config/api_config.json"


def load_config():
    """Load configuration with error handling."""
    try:
        with open(CONFIG_PATH, "r") as f:
            return json.load(f)
    except Exception as e:
        log.error(f"Critical Error: Could not load config at {CONFIG_PATH}: {e}")
        raise AirflowException("Gold curation failed: Config missing.")


def curate_gold_metrics(ds: str, **kwargs):
    """
    Silver -> Gold Curation Layer

    Produces daily business metrics:
        • avg_price
        • min_price
        • max_price
        • volatility_pct   = (max - min) / min
        • daily_change_pct = (today_avg - yesterday_avg) / yesterday_avg

    Idempotent + partitioned by metric_date.
    """

    log.info(f"[{ds}] Starting Gold Curation (Business Metrics Layer)")

    config = load_config()
    data_cfg = config["data_warehouse"]

    silver_table = data_cfg.get("silver_table")
    gold_table = data_cfg.get("gold_table")

    if not silver_table or not gold_table:
        raise AirflowException("Gold curation failed: Missing table names in config.")

    hook = PostgresHook(postgres_conn_id="postgres_default")

    # --------------------------
    # 1) Ensure table structure
    # --------------------------
    setup_sql = f"""
    CREATE TABLE IF NOT EXISTS {gold_table} (
        metric_date DATE NOT NULL,
        coin_id TEXT NOT NULL,
        vs_currency VARCHAR(10) NOT NULL,
        avg_price NUMERIC(20,10),
        min_price NUMERIC(20,10),
        max_price NUMERIC(20,10),
        volatility_pct NUMERIC(10,4),
        daily_change_pct NUMERIC(10,4),
        PRIMARY KEY (metric_date, coin_id, vs_currency)
    );
    
    CREATE INDEX IF NOT EXISTS idx_{gold_table}_date
        ON {gold_table} (metric_date, coin_id, vs_currency);
    """
    hook.run(setup_sql)

    # --------------------------
    # 2) Aggregate daily metrics
    # --------------------------
    upsert_sql = f"""
    WITH daily_stats AS (
        SELECT
            source_timestamp::date AS metric_date,
            coin_id,
            vs_currency,
            AVG(price) AS avg_price,
            MIN(price) AS min_price,
            MAX(price) AS max_price
        FROM {silver_table}
        WHERE source_timestamp::date = %s::date
        GROUP BY 1,2,3
    ),
    comparison AS (
        SELECT
            s.*,
            g.avg_price AS prev_avg_price
        FROM daily_stats s
        LEFT JOIN {gold_table} g
            ON s.coin_id = g.coin_id
            AND s.vs_currency = g.vs_currency
            AND g.metric_date = (%s::date - INTERVAL '1 day')::date
    )
    INSERT INTO {gold_table} (
        metric_date,
        coin_id,
        vs_currency,
        avg_price,
        min_price,
        max_price,
        volatility_pct,
        daily_change_pct
    )
    SELECT
        metric_date,
        coin_id,
        vs_currency,
        avg_price,
        min_price,
        max_price,

        -- Volatility = (Max − Min) / Min
        CASE
            WHEN min_price = 0 THEN NULL
            ELSE ((max_price - min_price) / min_price) * 100
        END AS volatility_pct,

        -- Daily Performance = Today vs Yesterday
        CASE
            WHEN prev_avg_price IS NULL OR prev_avg_price = 0 THEN 0
            ELSE ((avg_price - prev_avg_price) / prev_avg_price) * 100
        END AS daily_change_pct

    FROM comparison
    ON CONFLICT (metric_date, coin_id, vs_currency)
    DO UPDATE SET
        avg_price = EXCLUDED.avg_price,
        min_price = EXCLUDED.min_price,
        max_price = EXCLUDED.max_price,
        volatility_pct = EXCLUDED.volatility_pct,
        daily_change_pct = EXCLUDED.daily_change_pct;
    """

    hook.run(upsert_sql, parameters=(ds, ds))

    log.info(f"[{ds}] Gold curation completed successfully for {gold_table}.")
