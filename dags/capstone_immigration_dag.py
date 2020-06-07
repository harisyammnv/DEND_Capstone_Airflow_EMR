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

config = ConfigParser()
config.read('./plugins/helpers/dwh_airflow.cfg')

aws_hook = AwsHook('aws_credentials')
credentials = aws_hook.get_credentials()

os.environ['AWS_PROFILE'] = "Profile1"
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
    s3_client.upload_file("dags/transforms/transform_immigration.py", kwargs['bucket'],
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
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2016, 4, 1),
    'end_date': datetime(2016, 5, 1),
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
    python_callable=upload_transform_script,
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
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
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

'''
# create boto3 emr client
emr_cp = EMRClusterProvider(aws_key=PARAMS['aws_access_key'], aws_secret=PARAMS['aws_secret'],
                            region=PARAMS['REGION'], key_pair=PARAMS['EC2_KEY_PAIR'], num_nodes=3)
emr = emr_cp.create_client()


def create_emr_cluster(**kwargs):
    cluster_id = emr_cp.create_cluster(cluster_name='Immigration-Airflow')
    Variable.set("cluster_id",cluster_id)
    return cluster_id


def wait_for_emr_completion(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_emr_cluster')
    emr_cp.wait_for_cluster_creation(cluster_id=cluster_id)


def terminate_emr_cluster(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_emr_cluster')
    emr_cp.terminate_cluster(cluster_id=cluster_id)
    Variable.set("cluster_id", "na")


def submit_transform(**kwargs):
    cluster_id = Variable.get("cluster_id")
    cluster_dns = emr_cp.get_cluster_dns(cluster_id)
    emr_session = EMRSessionProvider(master_dns=cluster_dns)
    headers = emr_session.create_spark_session('pyspark')
    emr_session.wait_for_idle_session(headers)
    # Get execution date format
    execution_date = kwargs["execution_date"]
    month = execution_date.strftime("%b").lower()
    year = execution_date.strftime("%y")
    logging.info(r"Extracting Data for Month: {} and Year: {}".format(month, year))
    statement_response = emr_session.submit_statement(kwargs['params']['file'],
                                                      kwargs['params']['args'].append("--month_year={}".format(month+year)))
    logs = emr_session.track_statement_progress(statement_response.headers)
    emr_session.kill_spark_session()
    if kwargs['params']['log']:
        for line in logs:
            logging.info(line)
            if 'FAIL' in str(line):
                logging.info(line)
                raise AirflowException("Normalize data Quality check Fail!")


transform_immigration = PythonOperator(
    task_id='transform_demographics',
    python_callable=submit_transform,
    params={"file" : '/root/airflow/dags/transforms/transform_immigration.py', "log":False,
            "args":["--input={}".format(PARAMS['RAW_DATA_BUCKET']),
                    "--output={}".format(PARAMS['FINAL_DATA_BUCKET'])]}, dag=dag)

create_cluster = PythonOperator(
    task_id='create_emr_cluster',
    python_callable=create_emr_cluster,
    dag=dag)

wait_for_cluster = PythonOperator(
    task_id='wait_for_emr_cluster',
    python_callable=wait_for_emr_completion,
    dag=dag)

terminate_cluster = PythonOperator(
    task_id='terminate_emr_cluster',
    python_callable=terminate_emr_cluster,
    trigger_rule='all_done',
    dag=dag)

start_operator >> create_cluster
create_cluster >> wait_for_cluster
wait_for_cluster >> transform_immigration
transform_immigration >> terminate_cluster
terminate_cluster >> finish_operator

'''
