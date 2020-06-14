from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CopyToRedshiftOperator(BaseOperator):
    """
    Transfer data from AWS S3 to staging tables in AWS Redshift

    Parameters:
    redshift_conn_id: Connection Id of the Airflow connection to AWS Redshift database (Postgres Type)
    table_name: name of the staging table in AWS Redshift
    aws_credentials_id: Connection Id of the Airflow connection to AWS
    s3_bucket: name of S3 bucket, e.g. "udacity-dend"
    s3_key: name of S3 key. This field is templatable when context is enabled, e.g. "log_data/{execution_date.year}/{execution_date.month}/"
    delimiter: csv field delimiter
    ignore_headers: '0' or '1'
    data_files_format: 'csv' or 'json'
    jsonpaths: path to JSONpaths file

    Returns: None
    """
    template_fields = ['s3_key']

    copy_parquet_cmd = """
                        COPY {table_name} FROM '{s3_path}'
                        IAM_ROLE '{iam_role}'
                        FORMAT AS PARQUET;
                       """

    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_list="",
                 iam_role="",
                 s3_bucket="",s3_key="",
                 s3_key_list="", write_mode="",
                 *args, **kwargs):

        super(CopyToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_list = table_list
        self.iam_role = iam_role
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_key_list = s3_key_list
        self.write_mode = write_mode

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.s3_key=="" and len(self.s3_key_list)>0:
            for table, s3key in zip(self.table_list, self.s3_key_list):
                if self.write_mode == "overwrite":
                    self.log.info("Cleaning data from Redshift table - {}".format(table))
                    redshift.run("TRUNCATE TABLE {}".format(table))

                self.log.info("Copying data from AWS S3 to Redshift table - {}".format(table))

                s3_path = "s3://{}/{}".format(self.s3_bucket, s3key)

                formatted_sql = CopyToRedshiftOperator.copy_parquet_cmd.format(
                    table_name=table,
                    s3_path=s3_path, iam_role=self.iam_role)
                redshift.run(formatted_sql)
        elif self.s3_key_list=="" and self.s3_key!="":
            s3_file = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
            if self.write_mode == "append":
                self.log.info("Cleaning data from Staging Fact table - {}".format(self.table_list[0]))
                redshift.run("TRUNCATE TABLE {}".format("staging_"+self.table_list[0]))
                formatted_sql = CopyToRedshiftOperator.copy_parquet_cmd.format(
                    table_name='staging_'+self.table_list[0],
                    s3_path=s3_file, iam_role=self.iam_role)
                redshift.run(formatted_sql)
                redshift.run("""insert into {} (select * from {})""".format(self.table_list[0],"staging_"+self.table_list[0]))
                redshift.run(
                    """TRUNCATE TABLE {}""".format("staging_" + self.table_list[0]))
            else:
                self.log.info("Cleaning data from Fact table - {}".format(self.table_list[0]))
                redshift.run("TRUNCATE TABLE {}".format(self.table_list[0]))
                formatted_sql = CopyToRedshiftOperator.copy_parquet_cmd.format(
                    table_name=self.table_list[0],
                    s3_path=s3_file, iam_role=self.iam_role)
                redshift.run(formatted_sql)
        else:
            raise ValueError("Either an S3 key or S3 Key list has to be specified in the arguments")
