from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta,datetime
import os 
import sys
import pathlib

location = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0,location)

from pipeline.uber_etl import uber_main
from pipeline.upload import main
from pipeline.redshift_upload import redshift_main

default_args = {
    'owner': 'your_name',
    'start_date': datetime(2023, 12, 5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
}

with DAG('uber_etl',description="uber etl pipeline",schedule=timedelta(days=1),catchup=False,start_date=datetime(2023,12,5),tags=['reddit', 'etl', 'pipeline'],default_args=default_args) as dag:

    task_1 = PythonOperator(task_id='run_etl',
                            python_callable=uber_main)
    
    task_2 = PythonOperator(task_id='upload_to_s3',
                            python_callable=main)
    
    task_3 = PythonOperator(task_id = "upload_to_redshift",
                            python_callable=redshift_main)
    
    task_1 >> task_2 >> task_3