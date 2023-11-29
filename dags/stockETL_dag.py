from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from etl.extract.extract_dummy import run_extract
from etl.extract.extract_yfinance import extract_prices
from etl.transform.transform_price import transform_price
from etl.load.load import load_price

# Define default_args dictionary to specify the default parameters of the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate a DAG
dag = DAG(
    'stockETL_dag',
    default_args=default_args,
    description='ETL for stock data',
    schedule_interval='0 0 * * 2-6',  # Set the schedule interval
)

# Define tasks using PythonOperator
extract_price_task = PythonOperator(
    task_id='extract_price_task',
    python_callable=extract_prices,
    dag=dag,
)

transform_price_task = PythonOperator(
    task_id='transform_price_task',
    python_callable=transform_price,
    dag=dag,
)

load_price_task = PythonOperator(
    task_id='load_price_task',
    python_callable=load_price,
    op_kwargs={'csv_file': 'stock_prices_clean.csv', 'table_name': 'price'},
    dag=dag,
)

# Define the task dependencies
extract_price_task >> transform_price_task
transform_price_task >> load_price_task