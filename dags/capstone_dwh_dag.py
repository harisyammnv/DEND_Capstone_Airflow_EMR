from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
# from airflow.operators.capstone_plugin import S3DataCheckOperator
from plugins.operators.S3_Data_Check import S3DataCheckOperator
from configparser import ConfigParser
from airflow.contrib.hooks.aws_hook import AwsHook


config = ConfigParser()
config.read('./plugins/helpers/dwh_airflow.cfg')

aws_hook = AwsHook('aws_credentials')
credentials = aws_hook.get_credentials()

PARAMS = {'aws_access_key': credentials.access_key,
          'aws_secret': credentials.secret_key,
          'FINAL_DATA_BUCKET' : config.get('S3', 'FINAL_DATA_BUCKET'),
          'VISA_DATA_LOC' : config.get('S3', 'VISA_DATA'),
          'CODES_DATA_LOC' : config.get('S3','CODES_DATA'),
          'I94_RAW_DATA_LOC' : config.get('S3','I94_RAW_DATA'),
          'SAS_LABELS_DATA_LOC' : config.get('S3','SAS_LABELS_DATA'),
          'DEMOGRAPHICS_DATA_LOC' : config.get('S3','DEMOGRAPHICS_DATA'),
          'REGION': config.get('AWS','REGION'),
          }

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

i94_meta_data_S3Check = S3DataCheckOperator(
    task_id="i94_data_check",
    aws_conn_id='aws_credentials',
    region=PARAMS['REGION'],
    bucket=PARAMS['FINAL_DATA_BUCKET'],
    prefix=PARAMS['SAS_LABELS_DATA_LOC'],
    file_list=['i94addr.csv', 'i94cit_i94res.csv','i94mode.csv','i94port_i94code.csv','i94visa.csv'],
    dag=dag)

codes_data_S3Check = S3DataCheckOperator(
    task_id="codes_data_check",
    aws_conn_id='aws_credentials',
    region=PARAMS['REGION'],
    bucket=PARAMS['FINAL_DATA_BUCKET'],
    prefix=PARAMS['CODES_DATA_LOC'],
    wild_card_extension='*.parquet',
    dag=dag)

i94_sas_data_S3Check = S3DataCheckOperator(
    task_id="i94_sas_data_check",
    aws_conn_id='aws_credentials',
    region=PARAMS['REGION'],
    bucket=PARAMS['FINAL_DATA_BUCKET'],
    prefix=PARAMS['I94_RAW_DATA_LOC'],
    file_list=['i94addr.csv', 'i94cit_i94res.csv','i94mode.csv','i94port_i94code.csv','i94visa.csv'],
    dag=dag)

visa_data_S3Check = S3DataCheckOperator(
    task_id="visa_data_check",
    aws_conn_id='aws_credentials',
    region=PARAMS['REGION'],
    bucket=PARAMS['FINAL_DATA_BUCKET'],
    prefix=PARAMS['VISA_DATA_LOC'],
    file_list=['i94addr.csv', 'i94cit_i94res.csv','i94mode.csv','i94port_i94code.csv','i94visa.csv'],
    dag=dag)

demographics_data_S3Check = S3DataCheckOperator(
    task_id="demographics_data_check",
    aws_conn_id='aws_credentials',
    region=PARAMS['REGION'],
    bucket=PARAMS['FINAL_DATA_BUCKET'],
    prefix=PARAMS['DEMOGRAPHICS_DATA_LOC'],
    file_list=['i94addr.csv', 'i94cit_i94res.csv','i94mode.csv','i94port_i94code.csv','i94visa.csv'],
    dag=dag)


start_operator >> [i94_meta_data_S3Check, i94_sas_data_S3Check,
                   visa_data_S3Check, demographics_data_S3Check, codes_data_S3Check]

