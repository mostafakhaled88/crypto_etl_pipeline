FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
# It is better to copy requirements to the airflow home
COPY --chown=airflow:root requirements.txt /opt/airflow/requirements.txt

RUN pip install --no-cache-dir "apache-airflow==2.9.3" -r /opt/airflow/requirements.txt