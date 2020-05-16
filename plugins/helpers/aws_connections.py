import json
from airflow.hooks.base_hook import BaseHook


class AirflowConnectionIds:
    S3 = 'aws_credentials'
    REDSHIFT = 'capstone-final'


def get_aws_access_id(conn_id):

    hook = BaseHook(conn_id)
    conn = hook.get_connection(conn_id)
    return json.loads(conn.extra)

