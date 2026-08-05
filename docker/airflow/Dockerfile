FROM apache/airflow:3.2.1-python3.12

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends docker.io \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow