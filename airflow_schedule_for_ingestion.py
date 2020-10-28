# [START tutorial]
from datetime import timedelta

# [START import_module]
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# [END import_module]

# [START default_args]

#Below is to schedule job hourly and it also runs the DAGS that are past on time due to catchup field

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    schedule_interval='@hourly',
    start_date=datetime(2020, 11, 1), 
    catchup=True
}
# [END default_args]

# [START instantiate_dag]
dag = DAG(
    'tutorial',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    tags=['example'],
)
# [END instantiate_dag]

# t1, t2 and t3 are examples of tasks created by instantiating operators
# [START basic_task]
t1 = BashOperator(
    task_id='load_data_from_mysql_to_delta_table.py',
    bash_command='python load_data_from_mysql_to_delta_table.py',
    dag=dag,
)

t2 = BashOperator(
    task_id='fetch_data_from_delta_to_temp_table.',
    bash_command='python fetch_data_from_delta_to_temp_table.py',
    dag=dag,,
)


t1 >> t2
# [END tutorial]
