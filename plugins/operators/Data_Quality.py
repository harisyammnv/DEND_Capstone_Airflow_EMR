from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
import os


class DataQualityCheckOperator(BaseOperator):
    template_fields = ['file_name']

    @apply_defaults
    def __init__(self, aws_conn_id="", region=None,
                 bucket=None, *args, **kwargs):

        super(DataQualityCheckOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.region_name = region
        self.bucket_name = bucket

    def execute(self, context):

        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        exists = s3_hook.check_for_bucket(self.bucket_name)
