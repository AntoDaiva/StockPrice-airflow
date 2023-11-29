from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from etl.extract.extract_kaggle import get_stock_info
from etl.transform.transform_info import filter_symbols
from etl.load.load import load_price

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 11, 26),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate a DAG
dag = DAG(
    'stockInfo_dag',
    default_args=default_args,
    description='ETL for stock profile (kaggle)',
    schedule_interval=None,  # Set the schedule interval
)

# Define tasks using PythonOperator
extract_info_task = PythonOperator(
    task_id='extract_info_task',
    python_callable=get_stock_info,
    dag=dag,
)

transform_info_task = PythonOperator(
    task_id='transform_info_task',
    python_callable=filter_symbols,
    dag=dag,
)

load_info_task = PythonOperator(
    task_id='load_info_task',
    python_callable=load_price,
    op_kwargs={'csv_file': 'stock_info.csv', 'table_name': 'profile'},
    dag=dag,
)

# Define the task dependencies
extract_info_task >> transform_info_task
transform_info_task >> load_info_task