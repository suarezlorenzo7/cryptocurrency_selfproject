FROM apache/airflow:2.8.1

USER airflow

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt -v

ENV PYTHONPATH="/opt/airflow/:/opt/airflow/src:/opt/airflow/utils"
