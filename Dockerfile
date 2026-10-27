FROM apache/airflow:2.7.2-python3.9

COPY requirements.txt /opt/airflow/

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc python3-dev curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

ENV PYTHONPATH=/opt/airflow
