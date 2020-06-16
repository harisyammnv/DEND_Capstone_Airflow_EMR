from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
import os


class S3DataCheckOperator(BaseOperator):
    """
    AWS S3 Data check operator

    Parameters:
    aws_conn_id: Connection Id of the Airflow connection to AWS
    region_name: region name of the S3 bucket
    bucket_name: name of S3 bucket
    prefix: name of S3 key or bucket prefix
    file_list: files to check for in the bucket
    folder_name: for specifying for folder name to search the files
    file_name: if file name is specified
    wild_card_extension: for parquet files

    Returns: None
    """

    template_fields = ['file_name','wild_card_extension','folder_name']

    @apply_defaults
    def __init__(self, aws_conn_id="", region=None,
                 bucket=None, prefix=None, file_list=None, file_name=None,folder_name=None,
                 wild_card_extension=None, *args, **kwargs):

        super(S3DataCheckOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.region_name = region
        self.bucket_name = bucket
        self.prefix = prefix
        self.file_list = file_list
        self.folder_name = folder_name
        self.file_name = file_name
        self.wild_card_extension = wild_card_extension

    def execute(self, context):

        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        exists = s3_hook.check_for_bucket(self.bucket_name)
        self.log.info("Extension provided - {}".format(self.wild_card_extension))
        if exists:
            self.log.info("S3 Bucket - {} exists".format(self.bucket_name))
        else:
            raise FileNotFoundError("Bucket - {} does not exists or not accessible!".format(self.bucket_name))

        exists = s3_hook.check_for_prefix(prefix=self.prefix, delimiter='/', bucket_name=self.bucket_name)
        if exists:
            self.log.info("Prefix - {} exists in the Bucket - {}".format(self.prefix, self.bucket_name))
        else:
            raise FileNotFoundError("Prefix - {} not found in S3 Bucket - {}".format(self.prefix, self.bucket_name))

        if self.wild_card_extension is None and self.file_list is not None:
            for file in self.file_list:
                if s3_hook.check_for_key(key=os.path.join(self.prefix, file), bucket_name=self.bucket_name):
                    self.log.info("File - {} exists in S3 Bucket - {}/{}".format(file, self.bucket_name, self.prefix))
                else:
                    raise FileNotFoundError("File - {} not found in S3 Bucket - {}".format(file, self.bucket_name))
        elif self.wild_card_extension is None and self.file_name is not None and self.file_list is None:
            if s3_hook.check_for_key(key=os.path.join(self.prefix, self.file_name), bucket_name=self.bucket_name):
                self.log.info("File - {} exists in S3 Bucket - {}/{}".format(self.file_name, self.bucket_name, self.prefix))
            else:
                raise FileNotFoundError("File - {} not found in S3 Bucket - {}".format(self.file_name, self.bucket_name))

        elif self.wild_card_extension is not None and self.file_list is None and self.folder_name is None:
            full_key = os.path.join(self.prefix, '*.'+self.wild_card_extension)
            success = s3_hook.check_for_wildcard_key(wildcard_key=full_key,
                                                     bucket_name=self.bucket_name,
                                                     delimiter='/')
            if success:
                self.log.info("Found the key: {}".format(full_key))
            else:
                self.log.info("Invalid key: {}".format(full_key))
                raise FileNotFoundError("No key named {}/{} ".format(self.bucket_name, full_key))

        elif self.wild_card_extension is not None and self.file_list is not None and self.folder_name is None:
            for fld in self.file_list:
                fld_key = os.path.join(os.path.join(self.prefix, fld), '*.'+self.wild_card_extension)
                fld_success = s3_hook.check_for_wildcard_key(wildcard_key=fld_key,
                                                             bucket_name=self.bucket_name,delimiter='/')
                if fld_success:
                    self.log.info("Found the key: {}".format(fld_key))
                else:
                    self.log.info("Invalid key: {}".format(fld_key))
                    raise FileNotFoundError("No key named {}/{} ".format(self.bucket_name, fld_key))
        elif self.wild_card_extension is not None and self.folder_name is not None:
            f_key = os.path.join(os.path.join(self.prefix, self.folder_name),'*.' + self.wild_card_extension)
            fld_success = s3_hook.check_for_wildcard_key(wildcard_key=f_key,
                                                         bucket_name=self.bucket_name, delimiter='/')
            if fld_success:
                self.log.info("Found the key: {}".format(f_key))
            else:
                self.log.info("Invalid key: {}".format(f_key))
                raise FileNotFoundError("No key named {}/{} ".format(self.bucket_name, f_key))
        else:
            raise ValueError("File list has to be provided if the there is no wild card extension")


