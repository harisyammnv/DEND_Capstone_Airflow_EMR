import logging
import s3fs
from configparser import ConfigParser
import re
import pandas as pd
from lib.emr_cluster_provider import *
from lib.emr_session_provider import *
# airflow
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow import AirflowException
from airflow.operators.capstone_plugin import S3DataCheckOperator

config = ConfigParser()
config.read('./plugins/helpers/dwh_airflow.cfg')

aws_hook = AwsHook('aws_credentials')
credentials = aws_hook.get_credentials()

PARAMS = {'aws_access_key': credentials.access_key,
          'aws_secret': credentials.secret_key,
          'FINAL_DATA_BUCKET' : config.get('S3', 'FINAL_DATA_BUCKET'),
          'RAW_DATA_BUCKET' : config.get('S3', 'RAW_DATA_BUCKET'),
          'VISA_DATA_LOC' : config.get('S3', 'VISA_DATA'),
          'CODES_DATA_LOC' : config.get('S3','CODES_DATA'),
          'I94_RAW_DATA_LOC' : config.get('S3','I94_RAW_DATA'),
          'DEMOGRAPHICS_DATA_LOC' : config.get('S3','DEMOGRAPHICS_DATA'),
          'REGION': config.get('AWS','REGION'),
          'EC2_KEY_PAIR': config.get('AWS','AWS_EC2_KEY_PAIR')
          }


def sas_labels_to_csv(*args, **kwargs):
    s3 = s3fs.S3FileSystem(anon=False,
                           key=PARAMS['aws_access_key'],
                           secret=PARAMS['aws_secret'])

    with s3.open(PARAMS['RAW_DATA_BUCKET'] + PARAMS['SAS_LABELS_DATA_LOC'] +
                 'I94_SAS_Labels_Descriptions.SAS',"rb") as i94_description:
        i94_label_content = i94_description.read()

    data_dict = {}
    df_dict = {}
    key_name = ''
    for line in i94_label_content.split("\n"):
        line = re.sub(r"\s+", " ", line)
        if '/*' in line and '-' in line:
            line = line.strip('/*')
            key_name = line.split('-')[0].strip()
            data_dict[key_name] = []
        if '=' in line and key_name != '':
            data_dict[key_name].append(
                [item.strip(';').strip(" ").replace('\'', '').lstrip().rstrip() for item in line.split('=')])

    for key in data_dict.keys():
        if len(data_dict[key]) > 0:
            if 'CIT' in key and 'RES' in key:
                i94cit_i94res = pd.DataFrame(data_dict[key], columns=['i94_country_code', 'country_name'])
                i94cit_i94res.loc[i94cit_i94res.country_name.str.contains('MEXICO'), 'country_name'] = 'MEXICO'
                df_dict['i94cit_i94res'] = i94cit_i94res
            if 'PORT' in key:
                i94port_i94code = pd.DataFrame(data_dict[key], columns=['i94_port_code', 'i94_airport_location'])
                i94port_i94code[['port_city', 'port_state']] = i94port_i94code['i94_airport_location'].str.rsplit(',',1,expand=True)
                i94port_i94code.loc[i94port_i94code.port_city == 'MARIPOSA AZ', 'port_state'] = 'AZ'
                i94port_i94code.loc[i94port_i94code.port_city == 'MARIPOSA AZ', 'port_city'] = 'MARIPOSA'
                i94port_i94code.loc[i94port_i94code.port_city == 'WASHINGTON DC', 'port_state'] = 'DC'
                i94port_i94code.drop(['i94_airport_location'], axis=1, inplace=True)
                df_dict['i94port_i94code'] = i94port_i94code
            if 'MODE' in key:
                i94mode = pd.DataFrame(data_dict[key], columns=['i94_mode_code', 'i94_mode'])
                df_dict['i94mode'] = i94mode
            if 'ADDR' in key:
                i94addr = pd.DataFrame(data_dict[key], columns=['i94_state_code', 'i94_state_name'])
                df_dict['i94addr'] = i94addr
            if 'VISA' in key:
                i94visa = pd.DataFrame(data_dict[key], columns=['i94_visa_code', 'visa_purpose'])
                df_dict['i94visa'] = i94visa

    for key in df_dict.keys():
        logging.info("Writing {} Table to Final S3 Bucket".format(key))
        with s3.open(r"{}/raw/i94_meta_data/{}.csv".format(PARAMS['RAW_DATA_BUCKET'], key), "wb") as f:
            df_dict[key].to_csv(f, index=False)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2016, 1, 1),
    'retries': 0,
    'email_on_failure': False,
    'email_on_retry': False,
    'provide_context': True
}

dag = DAG('MetaData_Transform_Dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval=None,
        )

start_operator = DummyOperator(task_id='Begin_ELT',  dag=dag)
finish_operator = DummyOperator(task_id='End_ELT',  dag=dag)

# create boto3 emr client
emr_cp = EMRClusterProvider(aws_key=PARAMS['aws_access_key'], aws_secret=PARAMS['aws_secret'],
                            region=PARAMS['REGION'], key_pair=PARAMS['EC2_KEY_PAIR'], num_nodes=3)
emr = emr_cp.create_client()


def create_emr_cluster(**kwargs):
    cluster_id = emr_cp.create_cluster(cluster_name='Udac-Airflow')
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
    statement_response = emr_session.submit_statement(kwargs['params']['file'])
    logs = emr_session.track_statement_progress(statement_response.headers)
    emr_session.kill_spark_session()
    if kwargs['params']['log']:
        for line in logs:
            logging.info(line)
            if 'FAIL' in str(line):
                logging.info(line)
                raise AirflowException("Normalize data Quality check Fail!")


create_cluster = PythonOperator(
    task_id='create_emr_cluster',
    python_callable=create_emr_cluster,
    dag=dag)

wait_for_cluster_completion = PythonOperator(
    task_id='wait_for_emr_cluster_completion',
    python_callable=wait_for_emr_completion,
    dag=dag)

terminate_cluster = PythonOperator(
    task_id='terminate_emr_cluster',
    python_callable=terminate_emr_cluster,
    trigger_rule='all_done',
    dag=dag)

task_write_sas_codes_to_s3 = PythonOperator(
    task_id='write_sas_labels_to_s3',
    python_callable=sas_labels_to_csv,
    dag=dag
)
visa_data_S3Check = S3DataCheckOperator(
    task_id="visa_data_check",
    aws_conn_id='aws_credentials',
    region=PARAMS['REGION'],
    bucket=PARAMS['RAW_DATA_BUCKET'],
    prefix=PARAMS['VISA_DATA_LOC'],
    file_list=['visa-type.csv', 'visa-issuing-ports.csv'],
    dag=dag)

transform_visa = PythonOperator(
    task_id='transform_visa',
    python_callable=submit_transform,
    params={"file" : '/root/airflow/dags/transforms/transform_visa.py', "log":False,
            "args":["--input={}".format(PARAMS['RAW_DATA_BUCKET']),
                    "--output={}".format(PARAMS['FINAL_DATA_BUCKET'])]},
    dag=dag)

i94_data_S3Check = S3DataCheckOperator(
    task_id="visa_data_check",
    aws_conn_id='aws_credentials',
    region=PARAMS['REGION'],
    bucket=PARAMS['RAW_DATA_BUCKET'],
    prefix=PARAMS['VISA_DATA_LOC'],
    file_list=['i94addr.csv', 'i94cit_i94res.csv','i94mode.csv','i94port_i94code.csv','i94visa.csv'],
    dag=dag)

transform_i94 = PythonOperator(
    task_id='transform_i94_meta_data',
    python_callable=submit_transform,
    params={"file" : '/root/airflow/dags/transforms/transform_i94_meta_data.py', "log":False,
            "args":["--input={}".format(PARAMS['RAW_DATA_BUCKET']),
                    "--output={}".format(PARAMS['FINAL_DATA_BUCKET'])]},
    dag=dag)

demographics_data_S3Check = S3DataCheckOperator(
    task_id="visa_data_check",
    aws_conn_id='aws_credentials',
    region=PARAMS['REGION'],
    bucket=PARAMS['RAW_DATA_BUCKET'],
    prefix=PARAMS['VISA_DATA_LOC'],
    file_list=['us-cities-demographics.csv'],
    dag=dag)

transform_demographics = PythonOperator(
    task_id='transform_demographics',
    python_callable=submit_transform,
    params={"file" : '/root/airflow/dags/transforms/transform_demographics.py', "log":False,
            "args":["--input={}".format(PARAMS['RAW_DATA_BUCKET']),
                    "--output={}".format(PARAMS['FINAL_DATA_BUCKET'])]},
    dag=dag)

codes_data_S3Check = S3DataCheckOperator(
    task_id="visa_data_check",
    aws_conn_id='aws_credentials',
    region=PARAMS['REGION'],
    bucket=PARAMS['RAW_DATA_BUCKET'],
    prefix=PARAMS['VISA_DATA_LOC'],
    file_list=['nationality-codes.csv', 'port-of-entry-codes.csv','airport-codes.csv','airlines-codes.csv'],
    dag=dag)

transform_codes = PythonOperator(
    task_id='transform_codes',
    python_callable=submit_transform,
    params={"file" : '/root/airflow/dags/transforms/transform_codes.py', "log":False,
            "args":["--input={}".format(PARAMS['RAW_DATA_BUCKET']),
                    "--output={}".format(PARAMS['FINAL_DATA_BUCKET'])]},
    dag=dag)

start_operator >> [task_write_sas_codes_to_s3, create_cluster]
[task_write_sas_codes_to_s3, create_cluster] >> wait_for_cluster_completion
wait_for_cluster_completion >> [visa_data_S3Check, demographics_data_S3Check, codes_data_S3Check, i94_data_S3Check]
visa_data_S3Check >> transform_visa
demographics_data_S3Check >> transform_demographics
codes_data_S3Check >> transform_codes
i94_data_S3Check >> transform_i94
[transform_visa, transform_demographics, transform_codes, transform_i94] >> terminate_cluster
terminate_cluster >> finish_operator
