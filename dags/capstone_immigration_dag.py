import logging
from configparser import ConfigParser
from lib.emr_cluster_provider import *
from lib.emr_session_provider import *
import boto3
import os
# airflow
from datetime import datetime
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow import AirflowException
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.operators.capstone_plugin import (S3DataCheckOperator, EmrAddStepsOperatorV2)


"""
This DAG is used for transforming the immigration SAS data to S3 parquet data (Data Lake)
"""

config = ConfigParser()
config.read('./plugins/helpers/dwh_airflow.cfg')

aws_hook = AwsHook('aws_credentials')
credentials = aws_hook.get_credentials()

# uncomment this when running this from a local Airflow instance
#os.environ['AWS_PROFILE'] = "Profile1"
os.environ['AWS_DEFAULT_REGION'] = "us-west-2"


PARAMS = {'aws_access_key': credentials.access_key,
          'aws_secret': credentials.secret_key,
          'FINAL_DATA_BUCKET' : config.get('S3', 'FINAL_DATA_BUCKET'),
          'RAW_DATA_BUCKET' : config.get('S3', 'RAW_DATA_BUCKET'),
          'I94_RAW_DATA_LOC' : config.get('S3','I94_RAW_DATA'),
          'REGION': config.get('AWS','REGION'),
          'PYTHON_APPS': config.get('S3','PYTHON_APPS'),
          'EC2_KEY_PAIR': config.get('AWS','AWS_EC2_KEY_PAIR')
          }


def upload_scripts(**kwargs):
    s3_client = boto3.client('s3', region_name=kwargs['region'],
                             aws_access_key_id=kwargs['aws_access_key'],
                             aws_secret_access_key=kwargs['aws_secret'])
    s3_client.upload_file("dags/transforms/transform_immigration.py", kwargs['bucket'], kwargs['file_path'] + "transform_immigration.py")
    s3_client.upload_file("dags/transforms/data_lake_quality_check.py", kwargs['bucket'],
                          kwargs['file_path'] + "data_lake_quality_check.py")


JOB_FLOW = {
    'Name' : 'sas-immigration',
    'LogUri' : 's3://dend-capstone-data/emr-logs/',
    'ReleaseLabel' : 'emr-5.25.0',
    'Instances' : {
      'InstanceGroups': [
            {
                'Name': 'Master nodes',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
            },
            {
                'Name': 'Slave nodes',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 3,
            }
        ],
        'Ec2KeyName': PARAMS['EC2_KEY_PAIR'],
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False
    },
    'BootstrapActions': [
        {
            'Name': 'copy python files to local',
            'ScriptBootstrapAction': {
                'Path': 's3://dend-capstone-data/awsemr/bootstrap_action.sh'
            }
        },
    ],
    'Applications':[{
        'Name': 'Spark'
    },{
        'Name': 'Livy'
    },{
        'Name': 'Hadoop'
    },{
        'Name': 'Zeppelin'
    },{
        'Name': 'Ganglia'
    }],
    'JobFlowRole':'EMR_EC2_DefaultRole',
    'ServiceRole':'EMR_DefaultRole'
}


default_args = {
    'owner': 'harisyam manda',
    'depends_on_past': False,
    'start_date': datetime(2016, 1, 1),
    'end_date': datetime(2016, 12, 1),
    'retries': 0,
    'email_on_failure': False,
    'email_on_retry': False,
    'provide_context': True
}


dag = DAG('immigration_transform_Dag',
          default_args=default_args,
          description='Transform Immigration data in EMR with Airflow',
          schedule_interval='@monthly', max_active_runs=1)

start_operator = DummyOperator(task_id='begin_immigration_transform',  dag=dag)
finish_operator = DummyOperator(task_id='end_immigration_transform',  dag=dag)

TRANSFORM_IMMIGRATION_SAS_DATA = [
    {
        'Name': 'sas_i94_immigration',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--packages',
                'saurfang:spark-sas7bdat:2.0.0-s_2.10',
                '--deploy-mode',
                'client',
                '--master',
                'yarn',
                '/home/hadoop/python_apps/transform_immigration.py',
                '--input', PARAMS['RAW_DATA_BUCKET'],
                '--output', PARAMS['FINAL_DATA_BUCKET'],
                '--month', "{{ execution_date.strftime('%b').lower() }}",
                '--year', "{{ execution_date.strftime('%y') }}"
            ]
        }
    }
]

DATA_QUALITY_SAS_DATA = [
    {
        'Name': 'sas_i94_data_quality',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--packages',
                'saurfang:spark-sas7bdat:2.0.0-s_2.10',
                '--deploy-mode',
                'client',
                '--master',
                'yarn',
                '/home/hadoop/python_apps/data_lake_quality_check.py',
                '--data', PARAMS['FINAL_DATA_BUCKET'],
                '--livy_session', "No",
                '--month', "{{ execution_date.strftime('%b').lower() }}",
                '--year', "{{ execution_date.strftime('%y') }}"
            ]
        }
    }
]

immigration_data_check = S3DataCheckOperator(
    task_id="immigration_data_check",
    aws_conn_id='aws_credentials',
    region=PARAMS['REGION'],
    bucket=PARAMS['RAW_DATA_BUCKET'],
    prefix=PARAMS['I94_RAW_DATA_LOC'].lstrip("/"),
    file_name= "i94_{{ execution_date.strftime('%b').lower() }}{{ execution_date.strftime('%y') }}_sub.sas7bdat",
    provide_context=True,
    dag=dag)

transform_script_upload = PythonOperator(
    task_id='S3_upload_transform_script',
    python_callable=upload_scripts,
    provide_context=True,
    op_kwargs={'region':PARAMS['REGION'], 'aws_access_key':PARAMS['aws_access_key'],'aws_secret':PARAMS['aws_secret'],
               'bucket': PARAMS['RAW_DATA_BUCKET'],'file_path':PARAMS['PYTHON_APPS']},
    dag=dag)


cluster_creator = EmrCreateJobFlowOperator(
    task_id='create_immigration_job',
    job_flow_overrides=JOB_FLOW,
    aws_conn_id='aws_default',
    emr_conn_id='emr_default',
    region_name=PARAMS['REGION'],
    dag=dag
)

add_transform_step_task = EmrAddStepsOperatorV2(
    task_id='add_transform_step',
    job_flow_id="{{ task_instance.xcom_pull('create_immigration_job', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=TRANSFORM_IMMIGRATION_SAS_DATA,
    region_name=PARAMS['REGION'],
    dag=dag
)

watch_immigration_transform_task = EmrStepSensor(
    task_id='watch_immigration_transform',
    job_flow_id="{{ task_instance.xcom_pull('create_immigration_job', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull('add_transform_step', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    region_name=PARAMS['REGION'],
    dag=dag
)

add_data_quality_check_task = EmrAddStepsOperatorV2(
    task_id='data_quality_check',
    job_flow_id="{{ task_instance.xcom_pull('create_immigration_job', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=DATA_QUALITY_SAS_DATA,
    region_name=PARAMS['REGION'],
    dag=dag
)

watch_prev_data_check_task = EmrStepSensor(
    task_id='watch_data_quality_check',
    job_flow_id="{{ task_instance.xcom_pull('create_immigration_job', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull('data_quality_check', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    region_name=PARAMS['REGION'],
    dag=dag
)

terminate_job_flow_task = EmrTerminateJobFlowOperator(
    task_id='terminate_job_flow',
    job_flow_id="{{ task_instance.xcom_pull('create_immigration_job', key='return_value') }}",
    aws_conn_id='aws_default',
    trigger_rule="all_done",
    region_name=PARAMS['REGION'],
    dag=dag
)

start_operator >> immigration_data_check
immigration_data_check >> transform_script_upload >> cluster_creator
cluster_creator >> add_transform_step_task >> watch_immigration_transform_task
watch_immigration_transform_task >> add_data_quality_check_task >> watch_prev_data_check_task >> terminate_job_flow_task
terminate_job_flow_task >> finish_operator