FROM apache/airflow:3.3.0-python3.12

USER airflow

RUN pip install --no-cache-dir \
    boto3 \
    clickhouse-connect \
    python-dotenv \
    requests

ENV PYTHONPATH=/opt/airflow/project/src