import logging
from configparser import ConfigParser
from lib.emr_cluster_provider import *
from lib.emr_session_provider import *
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


PARAMS = {'aws_access_key': credentials.access_key,
          'aws_secret': credentials.secret_key,
          'FINAL_DATA_BUCKET' : config.get('S3', 'FINAL_DATA_BUCKET'),
          'RAW_DATA_BUCKET' : config.get('S3', 'RAW_DATA_BUCKET'),
          'I94_RAW_DATA_LOC' : config.get('S3','I94_RAW_DATA'),
          'REGION': config.get('AWS','REGION'),
          'EC2_KEY_PAIR': config.get('AWS','AWS_EC2_KEY_PAIR')
          }

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


# def create_step_params(**kwargs):
#    exec_month = kwargs['execution_date'].strftime("%b").lower()
#    exec_year = kwargs['execution_date'].strftime("%y")
#    result = {'month': exec_month, 'year': exec_year}
#    return result


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
          schedule_interval='@monthly',max_active_runs=1
        )

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

#create_step_template = PythonOperator(
#    task_id='create_emr_step_template',
#    python_callable=create_step_params,
#    op_kwargs={'input_bucket': PARAMS['RAW_DATA_BUCKET'], 'output_bucket': PARAMS['FINAL_DATA_BUCKET']},
#    provide_context=True,
#    dag=dag
#)
# "i94_{{ task_instance.xcom_pull('create_emr_step_template', key='return_value')['month'] }}{{ task_instance.xcom_pull('create_emr_step_template', key='return_value')['year'] }}_sub.sas7bdat"


immigration_data_check = S3DataCheckOperator(
    task_id="immigration_data_check",
    aws_conn_id='aws_credentials',
    region=PARAMS['REGION'],
    bucket=PARAMS['RAW_DATA_BUCKET'],
    prefix=PARAMS['I94_RAW_DATA_LOC'].lstrip("/"),
    file_name= "i94_{{ execution_date.strftime('%b').lower() }}{{ execution_date.strftime('%y') }}_sub.sas7bdat",
    provide_context=True,
    dag=dag)

cluster_creator = EmrCreateJobFlowOperator(
    task_id='create_job_flow',
    job_flow_overrides=JOB_FLOW,
    aws_conn_id='aws_default',
    emr_conn_id='emr_default',
    region_name=PARAMS['REGION'],
    dag=dag
)

add_step_task = EmrAddStepsOperatorV2(
    task_id='add_step',
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=TRANSFORM_IMMIGRATION_SAS_DATA,
    region_name=PARAMS['REGION'],
    dag=dag
)

watch_prev_step_task = EmrStepSensor(
    task_id='watch_prev_step',
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull('add_step', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    region_name=PARAMS['REGION'],
    dag=dag
)

terminate_job_flow_task = EmrTerminateJobFlowOperator(
    task_id='terminate_job_flow',
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    aws_conn_id='aws_default',
    trigger_rule="all_done",
    dag=dag
)
start_operator >> immigration_data_check  # create_step_template
immigration_data_check >> cluster_creator
cluster_creator >> add_step_task
add_step_task >> watch_prev_step_task
watch_prev_step_task >> terminate_job_flow_task
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
