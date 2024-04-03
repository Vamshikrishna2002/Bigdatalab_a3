import os
import zipfile
import requests
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

from datetime import datetime

# Function to scrape data from NOAA website

def scrape_data(ds, year, num_files, **kwargs):
    print('cwd:',os.getcwd())
    base_url = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/{year}'
    os.makedirs(f'/opt/airflow/data/{year}', exist_ok=True)
    url = base_url.format(year=year)
    res = requests.get(url)
    soup = BeautifulSoup(res.text, 'html.parser')
    table = soup.find('table')
    anchors = table.find_all('a')
    # Filter anchors to only include links with 'csv' in their text
    anchors = [a for a in anchors if 'csv' in a.text]
    files_downloaded=0
    crashflag=0
    # Loop through the filtered anchors to download CSV files
    for anchor in anchors:
        if crashflag>2:
            break
        if files_downloaded>=num_files:
            break
        file = anchor.text
        file_url = f'{url}/{file}'
        # Check if file URL is reachable
        if(requests.get(file_url)==None):
            crashflag+=1
            continue
        # Download CSV file and save it to the specified directory
        res = requests.get(file_url)
        csv = res.text
        with open(f'/opt/airflow/data/{year}/{file}', 'w') as f:
            f.write(csv)
        files_downloaded=files_downloaded+1


# Function to zip the scraped data files
def zip_directory():
    os.makedirs('/opt/airflow/output', exist_ok=True)
    source_dir = '/opt/airflow/data'
    zip_filename = '/opt/airflow/output/data.zip'
    # Create a ZIP file containing all files in the source directory
    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(source_dir):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, source_dir)
                zipf.write(file_path, arcname=arcname)

# Define default arguments for the DAG

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
}

#Defining the DAG
dag = DAG(
    'task1dag',
    default_args=default_args,
    schedule_interval="@daily",  
    catchup=False
)

# Dummy task to gather scraped files
gather_task = DummyOperator(
    task_id='gather_scraped_files',
    dag=dag,
)

# Python operator to archive the scraped data
archive_task = PythonOperator(
    task_id='archive_data',
    python_callable=zip_directory,
    provide_context=True,
    dag=dag,
)

# Loop through the years to scrape data for each year
for year in range(2010,2023):
    task_id = f'scrape_data_{year}'
    # Python operator to scrape data for a specific year
    scrape_task = PythonOperator(
        task_id=task_id,
        python_callable=scrape_data,
        op_kwargs={'year':year, 'num_files':10},
        provide_context=True,
        dag=dag,
    )
    scrape_task >> gather_task  



gather_task >> archive_task  
