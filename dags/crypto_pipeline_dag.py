import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta, timezone
from Program import fetch_data, process_data, load_data

utc_minus_3 = timezone(timedelta(hours=-3))

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 19),
    # 'email': 'lorenzo.silva@poatek.com',
    # 'email_on_failure': True
}

dag = DAG(
    dag_id='crypto_pipeline',
    default_args=default_args,
    description='Crypto wallet info extraction',
    schedule_interval="*/5 * * * *",
    catchup=False
)

def fetch_data_with_xcom(**kwargs):

    ti = kwargs['ti']
    df = fetch_data()
    ti.xcom_push(key='dataframe', value=df.to_dict())

def process_data_with_xcom(**kwargs):

    ti = kwargs['ti']
    df_dict = ti.xcom_pull(task_ids='fetch_data', key='dataframe')
    df = pd.DataFrame(df_dict)
    processed_data = process_data(df)
    ti.xcom_push(key='processed_data', value=processed_data)

def load_data_with_xcom(**kwargs):
    
    ti = kwargs['ti']
    processed_data = ti.xcom_pull(task_ids='process_data', key='processed_data')
    load_data(
        processed_data['datetime_col'],
        processed_data['btcusdt_rate'],
        processed_data['ethusdt_rate'],
        processed_data['solusdt_rate'],
        processed_data['btcbrl_rate'],
        processed_data['ethbrl_rate'],
        processed_data['solbrl_rate'],
        processed_data['btcusdt_total'],
        processed_data['ethusdt_total'],
        processed_data['solusdt_total'],
        processed_data['btcbrl_total'],
        processed_data['ethbrl_total'],
        processed_data['solbrl_total'],
        processed_data['brl_total'],
        processed_data['usdt_total']
    )

fetch_data_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data_with_xcom,
    provide_context=True,
    dag=dag
)

process_data_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data_with_xcom,
    provide_context=True,
    dag=dag
)

load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data_with_xcom,
    provide_context=True,
    dag=dag
)

fetch_data_task >> process_data_task >> load_data_task