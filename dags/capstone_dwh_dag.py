from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.operators.capstone_plugin import (S3DataCheckOperator, CreateTableOperator, CopyToRedshiftOperator)
from helpers import SqlQueries
from configparser import ConfigParser
from airflow.contrib.hooks.aws_hook import AwsHook


"""
This DAG is used to create DWH in Redshift by using the Data from S3 Staging area (Data Lake)
"""

config = ConfigParser()
config.read('./plugins/helpers/dwh_airflow.cfg')

aws_hook = AwsHook('aws_credentials')
credentials = aws_hook.get_credentials()

PARAMS = {'aws_access_key': credentials.access_key,
          'aws_secret': credentials.secret_key,
          'FINAL_DATA_BUCKET' : config.get('S3', 'FINAL_DATA_BUCKET'),
          'VISA_DATA_LOC' : config.get('S3_STAGING', 'VISA_PORTS'),
          'CODES_DATA_LOC' : config.get('S3_STAGING','CODES'),
          'I94_RAW_DATA_LOC' : config.get('S3_STAGING','I94_SAS_DATA'),
          'I94_LABELS_LOC': config.get('S3_STAGING', 'I94_LABELS'),
          'DEMOGRAPHICS_DATA_LOC' : config.get('S3_STAGING','DEMOGRAPHICS'),
          'VISA_TYPE_LOC' : config.get('S3_STAGING','VISA_TYPES'),
          'REGION': config.get('AWS','REGION'),
          'IAM_ROLE': config.get('AWS','IAM_ROLE')
          }

default_args = {
    'owner': 'harisyam manda',
    'start_date': datetime(2016, 1, 1),
    'end_date': datetime(2016, 12, 1),
    'email_on_retry': False,
    'retries': 0,
    'catchup': False,
    'depends_on_past': True,
    'wait_for_downstream': True,
    'provide_context': True,
}


dag = DAG('capstone_DWH_dag',
          default_args=default_args,
          description='Data Engineering Capstone S3 Staging -> Redshift DWH',
          schedule_interval='@monthly', max_active_runs=1
          )

start_operator = DummyOperator(task_id='begin_ETL',  dag=dag)
finish_operator = DummyOperator(task_id='end_ETL',  dag=dag)

i94_meta_data_S3Check = S3DataCheckOperator(
    task_id="i94_data_check",
    aws_conn_id='aws_credentials',
    region=PARAMS['REGION'],
    bucket=PARAMS['FINAL_DATA_BUCKET'],
    prefix=PARAMS['I94_LABELS_LOC'].lstrip("/"),
    file_list = ['port_codes', 'state_codes', 'country_codes', 'transportation', 'visa'],
    wild_card_extension='parquet',
    dag=dag)

codes_data_S3Check = S3DataCheckOperator(
    task_id="codes_data_check",
    aws_conn_id='aws_credentials',
    region=PARAMS['REGION'],
    bucket=PARAMS['FINAL_DATA_BUCKET'],
    prefix=PARAMS['CODES_DATA_LOC'].lstrip("/"),
    file_list=['airline_codes', 'airport_codes', 'country_code', 'port-of-entry-codes'],
    wild_card_extension='parquet',
    dag=dag)

i94_sas_data_S3Check = S3DataCheckOperator(
    task_id="i94_sas_data_check",
    aws_conn_id='aws_credentials',
    region=PARAMS['REGION'],
    bucket=PARAMS['FINAL_DATA_BUCKET'],
    folder_name="month_year={{ execution_date.strftime('%b').lower() }}_{{ execution_date.strftime('%y') }}",
    prefix=PARAMS['I94_RAW_DATA_LOC'].lstrip("/"),
    wild_card_extension='parquet',
    dag=dag)

visa_ports_S3Check = S3DataCheckOperator(
    task_id="visa_ports_check",
    aws_conn_id='aws_credentials',
    region=PARAMS['REGION'],
    bucket=PARAMS['FINAL_DATA_BUCKET'],
    prefix=PARAMS['VISA_DATA_LOC'].lstrip("/"),
    wild_card_extension='parquet',
    dag=dag)

visa_type_S3Check = S3DataCheckOperator(
    task_id="visa_type_check",
    aws_conn_id='aws_credentials',
    region=PARAMS['REGION'],
    bucket=PARAMS['FINAL_DATA_BUCKET'],
    prefix=PARAMS['VISA_TYPE_LOC'].lstrip("/"),
    wild_card_extension='parquet',
    dag=dag)

demographics_data_S3Check = S3DataCheckOperator(
    task_id="demographics_data_check",
    aws_conn_id='aws_credentials',
    region=PARAMS['REGION'],
    bucket=PARAMS['FINAL_DATA_BUCKET'],
    prefix=PARAMS['DEMOGRAPHICS_DATA_LOC'].lstrip("/"),
    wild_card_extension='parquet',
    dag=dag)

create_empty_dim_tables = CreateTableOperator(
    task_id='create_dim_tables',
    dag=dag,
    redshift_conn_id="redshift",
    create_tables=SqlQueries.create_dim_tables
)
copy_dims_to_redshift = CopyToRedshiftOperator(
    task_id='copy_S3_redshift_dim_tables',
    dag=dag,
    redshift_conn_id="redshift",
    table_list=SqlQueries.tables,
    iam_role= PARAMS['IAM_ROLE'],
    s3_bucket=PARAMS['FINAL_DATA_BUCKET'],
    s3_key_list=SqlQueries.parquet_tables,
    write_mode="overwrite",
    provide_context=True,
)
create_empty_fct_table = CreateTableOperator(
    task_id='create_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    create_tables=SqlQueries.create_fact_tables
)
copy_fact_to_redshift = CopyToRedshiftOperator(
    task_id='copy_S3_redshift_fact_tables',
    dag=dag,
    redshift_conn_id="redshift",
    table_list=["immigration"],
    iam_role=PARAMS['IAM_ROLE'],
    s3_bucket=PARAMS['FINAL_DATA_BUCKET'],
    s3_key="lake/immigration/month_year={{ execution_date.strftime('%b').lower() }}_{{ execution_date.strftime('%y') }}",
    write_mode="append",
    provide_context=True,
)
    
start_operator >> [i94_meta_data_S3Check, i94_sas_data_S3Check,visa_type_S3Check,
                   visa_ports_S3Check, demographics_data_S3Check, codes_data_S3Check]
[i94_meta_data_S3Check, i94_sas_data_S3Check,visa_type_S3Check,
 visa_ports_S3Check, demographics_data_S3Check, codes_data_S3Check] >> create_empty_dim_tables
create_empty_dim_tables >> copy_dims_to_redshift >> create_empty_fct_table >> copy_fact_to_redshift >> finish_operator
