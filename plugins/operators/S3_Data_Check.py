from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
import os
from botocore.client import ClientError
import boto3


class S3DataCheckOperator(BaseOperator):

    @apply_defaults
    def __init__(self, aws_conn_id: str,region: str,
                 bucket: str,files_list: list, *args, **kwargs):

        super(S3DataCheckOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.region_name = region
        self.bucket_name = bucket
        self.files_list = files_list

    def execute(self, context):

        aws_hook = AwsHook(self.aws_conn_id)
        credentials = aws_hook.get_credentials()
        s3 = boto3.resource('S3', region_name= self.region_name, aws_access_key_id=credentials.access_key,
                            aws_secret_access_key=credentials.secret_key)
        exists = True
        accessible = False
        try:
            s3.meta.client.head_bucket(Bucket=self.bucket_name)
            accessible = True
        except ClientError as e:
            error_code = int(e.response['Error']['Code'])
            if error_code == 403:
                self.log.error("Private Bucket. Forbidden Access!")
                exists = True
            elif error_code == 404:
                self.log.error("Bucket Does Not Exist!")
                exists = False

        if exists and accessible:
            self.log.info("Bucket - {} is available and accessible")
        else:
            raise FileNotFoundError(" No S3 BUcket named {} or permissions are not sufficient")

        