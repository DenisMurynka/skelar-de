################################################################################
# Filename: currency_etl_dag.py
# Author: Denys Murynka
# Date: 2024-07-27
# Requirements:
#    - Docker
#
################################################################################

import requests
import datetime
import pandas as pd
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable


start_date = '2024-07-01'

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime.strptime(start_date, '%Y-%m-%d'),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'exchange_rate_dag',
    default_args=default_args,
    description='A DAG to fetch exchange rates and save them to Parquet files',
    #schedule='@daily',
)

def fetch_and_save_exchange_rates(**kwargs):
    end = datetime.date.today()

    start = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.datetime.strptime(str(end), "%Y-%m-%d")
    date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end - start).days + 1)]

    for date in date_generated:
        # Create a directory for the year and month if they don't exist
        year = date.strftime("%Y")
        month = date.strftime("%B")
        dir_path = os.path.join('/opt/airflow/data', year, month)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        
        # Define the file path
        file_name = f"exchange_rates_{date.strftime('%Y-%m-%d')}.parquet"
        file_path = os.path.join(dir_path, file_name)
        
        # Check if the file already exists
        if os.path.exists(file_path):
            print(f"File for {date.strftime('%d-%m-%Y')} already exists. Skipping.")
            continue

        # API key retrieved from the var
        api_key = Variable.get("openexchangerates_api")

        url = f'https://openexchangerates.org/api/historical/{date.strftime("%Y-%m-%d")}.json?app_id={api_key}'  # Default API URL
        json_obj = requests.get(url)
        dataq = json_obj.json()

        # Check if the response contains 'rates' to avoid errors
        if 'rates' in dataq:
            # Convert the 'rates' dictionary to a DataFrame and add a column for the date
            df = pd.DataFrame(list(dataq['rates'].items()), columns=['Currency', 'Rate'])
            df['Date'] = date.strftime("%Y-%m-%d")

            # Save the DataFrame to a Parquet file
            df.to_parquet(file_path, index=False)

            print(f"Saved data for {date.strftime('%d-%m-%Y')} to {file_path}")
        else:
            print(f"No rates data available for {date.strftime('%d-%m-%Y')}")

        print(date.strftime("%d-%m-%Y"))

# Define the task using the PythonOperator
fetch_and_save_task = PythonOperator(
    task_id='fetch_and_save_exchange_rates',
    python_callable=fetch_and_save_exchange_rates,
    provide_context=True,
    dag=dag,
)

fetch_and_save_task
