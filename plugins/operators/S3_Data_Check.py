from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
import os


class S3DataCheckOperator(BaseOperator):

    @apply_defaults
    def __init__(self, aws_conn_id="", region=None,
                 bucket=None, prefix=None, file_list=None, wild_card_extension=None, *args, **kwargs):

        super(S3DataCheckOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.region_name = region
        self.bucket_name = bucket
        self.prefix = prefix
        self.file_list = file_list
        self.wild_card_extension = wild_card_extension

    def execute(self, context):

        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)

        exists = s3_hook.check_for_bucket(self.bucket_name)
        if exists:
            self.log.info("S3 Bucket - {} exists".format(self.bucket_name))
        else:
            raise FileNotFoundError("Bucket - {} does not exists or not accessible!".format(self.bucket_name))

        exists = s3_hook.check_for_prefix(prefix=self.prefix, delimiter='/', bucket_name=self.bucket_name)
        if exists:
            self.log.info("Prefix - {} exists in the Bucket - {}".format(self.prefix, self.bucket_name))
        else:
            raise FileNotFoundError("Prefix - {} not found in S3 Bucket - {}".format(self.prefix, self.bucket_name))

        if self.wild_card_extension is None and len(self.file_list)>0:
            for file in self.file_list:
                if s3_hook.check_for_key(key = os.path.join(self.prefix.lstrip('/'), file), bucket_name=self.bucket_name):
                    self.log.info("File - {} exists in S3 Bucket - {}/{}".format(file, self.bucket_name, self.prefix.lstrip('/')))
                else:
                    raise FileNotFoundError("File - {} not found in S3 Bucket - {}".format(file, self.bucket_name))
        else:
            raise ValueError("File list has to be provided if the there is no wild card extension")

        if self.wild_card_extension is not None:
            full_key = os.path.join(self.prefix, self.wild_card_extension)
            success = s3_hook.check_for_wildcard_key(wildcard_key=full_key,
                                                     bucket_name=self.bucket_name,
                                                     delimiter='/')
            if success:
                self.log.info("Found the key: {}".format(full_key))
            else:
                self.log.info("Invalid key: {}".format(full_key))
                raise FileNotFoundError("No key named {}/{} ".format(self.bucket_name, full_key))


