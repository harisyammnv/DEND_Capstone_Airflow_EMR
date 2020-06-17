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
