from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import tarfile

# Task 3: Extract IP addresses
def extract_ipaddress():
    log_file = '/home/project/airflow/dags/capstone/accesslog.txt'
    output_file = '/home/project/airflow/output/extracted_data.txt'
    with open(log_file, 'r') as infile, open(output_file, 'w') as outfile:
        for line in infile:
            ip = line.split()[0]
            outfile.write(ip + '\n')

# Task 4: Transform data
def transform_ipaddress():
    input_file = '/home/project/airflow/output/extracted_data.txt'
    output_file = '/home/project/airflow/output/transformed_data.txt'
    exclude_ip = '198.46.149.143'
    with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
        for line in infile:
            ip = line.strip()
            if ip != exclude_ip:
                outfile.write(ip + '\n')

# Task 5: Load data
def load_data():
    input_file = '/home/project/airflow/output/transformed_data.txt'
    tar_file = '/home/project/airflow/output/weblog.tar'
    with tarfile.open(tar_file, 'w') as tar:
        tar.add(input_file, arcname='transformed_data.txt')

# Default arguments
default_args = {
    'owner': 'felipe',
    'depends_on_past': False,
    'email': ['alerts@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 3, 1),
}

# Define the DAG
with DAG(
    dag_id='process_web_log',
    default_args=default_args,
    description='DAG that processes web server logs daily for monitoring and analysis',
    schedule_interval='@daily',
    catchup=False,
    tags=['logs', 'monitoring', 'daily'],
) as dag:

    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract_ipaddress,
    )

    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_ipaddress,
    )

    load_data_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
    )

    # Task pipeline definition
    extract_data >> transform_data >> load_data_task
