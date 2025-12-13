# Dockerfile
FROM apache/airflow:2.9.1

# 🔑 Switch to root to install system packages
USER root

# Install system dependencies (like timezone data)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        tzdata \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 🔁 Switch back to airflow user (VERY IMPORTANT)
USER airflow

# Install required Python packages
RUN pip install --no-cache-dir \
    requests \
    psycopg2-binary \
    pandas \
    apache-airflow-providers-postgres \
    ccxt
