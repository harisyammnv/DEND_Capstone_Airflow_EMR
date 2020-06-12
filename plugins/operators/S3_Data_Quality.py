from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
import os


class S3DataQualityCheckOperator(BaseOperator):

    @apply_defaults
    def __init__(self, aws_conn_id="", region=None,
                 bucket=None, *args, **kwargs):

        super(S3DataQualityCheckOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.region_name = region
        self.bucket_name = bucket

    def execute(self, context):

        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        exists = s3_hook.check_for_bucket(self.bucket_name)
        if exists:
            self.log.info("S3 Bucket - {} exists".format(self.bucket_name))
        else:
            raise FileNotFoundError("Bucket - {} does not exists or not accessible!".format(self.bucket_name))
        '''
        df = spark.read.parquet(path)
        if len(df.columns) > 0 and df.count() > 0:
            logger.warn("Data Quality check for - {} SUCCESS".format(table))
        else:
            logger.warn("Data Quality check for - {} FAIL".format(table))
            raise ValueError("Data Quality Check not passed")
        out_bucket = args.data
        check_data_quality_livy("s3://{}/lake/codes/country_code/".format(out_bucket), 'country_code', logger)
        check_data_quality_livy("s3://{}/lake/codes/port-of-entry-codes/".format(out_bucket), 'port-of-entry-codes',
                                logger)
        check_data_quality_livy("s3://{}/lake/codes/airport_codes/".format(out_bucket), 'airport_codes', logger)
        check_data_quality_livy("s3://{}/lake/codes/airline_codes/".format(out_bucket), 'airline_codes', logger)
        check_data_quality_livy("s3://{}/lake/demographics/".format(args.data), 'demographics', logger)
        check_data_quality_livy("s3://{}/lake/i94_meta_data/transportation/".format(out_bucket), 'transportation',
                                logger)
        check_data_quality_livy("s3://{}/lake/i94_meta_data/country_codes/".format(out_bucket), 'country_codes', logger)
        check_data_quality_livy("s3://{}/lake/i94_meta_data/state_codes/".format(out_bucket), 'state_codes', logger)
        check_data_quality_livy("s3://{}/lake/i94_meta_data/port_codes/".format(out_bucket), 'port_codes', logger)
        check_data_quality_livy("s3://{}/lake/i94_meta_data/visa/".format(out_bucket), 'visa', logger)
        check_data_quality_livy("s3://{}/lake/visa-issue-port/".format(out_bucket), 'visa-issue-port', logger)
        check_data_quality_livy("s3://{}/lake/visa-type/".format(out_bucket), 'visa-type', logger)
        
        '''
