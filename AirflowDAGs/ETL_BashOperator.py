# ETL_toll_data DAG: Demonstrates Airflow DAGs, BashOperators, task dependencies, and ETL (extract, transform, load) concepts
# Uses Bash commands (tar, cut, paste, awk) orchestrated by Airflow scheduling and DAG chaining
# CLI commands:
#   airflow dags list                # list all DAGs
#   airflow dags trigger ETL_toll_data   # trigger this DAG manually
#   airflow dags list-runs -d ETL_toll_data   # show DAG run history
#   airflow tasks list ETL_toll_data   # list tasks in this DAG
#   airflow tasks test ETL_toll_data unzip_data <date>   # test a single task


from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Define default arguments
default_args = {
    "owner": "Felipe",                # replace with any name
    "start_date": datetime.today(),        # today's date
    "email": ["felipe@example.com"],  # replace with any email
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id="ETL_toll_data",
    default_args=default_args,
    schedule_interval="@daily",  # run once per day
    catchup=False,
    description="Apache Airflow Final Assignment"
) as dag:

    # Single task: extract tar.gz file
    unzip_data = BashOperator(
        task_id="unzip_data",
        bash_command="tar -xzf /home/project/airflow/dags/finalassignment/tolldata.tgz -C /home/project/airflow/dags/finalassignment/staging/"
    )
    extract_data_from_csv = BashOperator( 
        task_id="extract_data_from_csv", 
        bash_command=("cut -d',' -f1,2,3,4 /home/project/airflow/dags/finalassignment/staging/vehicle-data.csv \
        > /home/project/airflow/dags/finalassignment/staging/csv_data.csv")
    )

    extract_data_from_tsv = BashOperator( 
        task_id="extract_data_from_tsv", 
        bash_command=("cut -f5,6,7 /home/project/airflow/dags/finalassignment/staging/tollplaza-data.tsv \
        | tr '\\t' ',' > /home/project/airflow/dags/finalassignment/staging/tsv_data.csv") 
    )

    extract_data_from_fixed_width = BashOperator( 
        task_id="extract_data_from_fixed_width", 
        bash_command=("cut -c59-67 /home/project/airflow/dags/finalassignment/staging/payment-data.txt \
        | tr ' ' ',' > /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv") 
    )

    consolidate_data = BashOperator( 
        task_id="consolidate_data", 
        bash_command=( "paste -d',' " 
        "/home/project/airflow/dags/finalassignment/staging/csv_data.csv " 
        "/home/project/airflow/dags/finalassignment/staging/tsv_data.csv " 
        "/home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv " " \
        > /home/project/airflow/dags/finalassignment/staging/extracted_data.csv" ) 
    )

    transform_data = BashOperator(
        task_id="transform_data", 
        bash_command=( "awk -F',' '{ $4=toupper($4); OFS=\",\"; print $0 }' " 
        "/home/project/airflow/dags/finalassignment/staging/extracted_data.csv " " \
        > /home/project/airflow/dags/finalassignment/staging/transformed_data.csv" ) )

    unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data

