from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2016, 4, 1),
    'end_date': datetime(2016, 5, 1),
    'email_on_retry': False,
    'retries': 3,
    'catchup': False,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': True,
    'wait_for_downstream': True
}

# dag is complete
dag = DAG('Capstone_DWH_Dag',
          default_args=default_args,
          description='Data Engineering Capstone DWH',
          schedule_interval='@daily'
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)
end_operator = DummyOperator(task_id='End_execution', dag=dag)



