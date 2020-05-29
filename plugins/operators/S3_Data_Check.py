from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
import os
from botocore.client import ClientError
import boto3


class S3DataCheckOperator(BaseOperator):

    @apply_defaults
    def __init__(self, aws_conn_id: str,region: str,
                 bucket: str, prefix: str, file_list: list, *args, **kwargs):

        super(S3DataCheckOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.region_name = region
        self.bucket_name = bucket
        self.prefix = prefix
        self.file_list = file_list

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
                self.log.error("Private Bucket. Forbidden Access to {}".format(self.bucket_name))
                exists = True
            elif error_code == 404:
                self.log.error("Bucket - {} Does Not Exist in S3".format(self.bucket_name))
                exists = False

        if exists and accessible:
            self.log.info("Bucket - {} is available and accessible".format(self.bucket_name))
            bucket = s3.Bucket(self.bucket_name)
            objs = list(bucket.objects.filter(Prefix=self.prefix.lstrip('/')))
            for file in self.file_list:
                if any([w.key == os.path.join(self.prefix.lstrip('/'), file) for w in objs]):
                    self.log.info("File - {} exists")
                else:
                    raise FileNotFoundError("File - {} not found in S3 Bucket - {}".format(file,self.bucket_name))
        else:
            raise FileNotFoundError("Bucket - {} does not exists or not accessible!".format(self.bucket_name))

