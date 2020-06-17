from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DWHDataQualityCheckOperator(BaseOperator):
    """
    Checks the Data Quality of the Tables in AWS Redshift

    Parameters:
    redshift_conn_id: Connection Id of the Airflow connection to AWS Redshift database (Postgres Type)
    tables_list: List of table names for which the Quality check has to be performed

    Returns: None
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,redshift_conn_id="",
                 tables_list=[],
                 *args, **kwargs):

        super(DWHDataQualityCheckOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables_list = tables_list

    def execute(self, context):

        self.log.info('Fetching redshift hook')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Checking the Data Quality')
        for table_name in self.tables_list:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table_name}")
            if len(records) < 1 or len(records[0]) < 1 or records[0][0] < 1:
                self.log.error(f"Data quality check failed: {table_name} returned no results")
                raise ValueError(f"Data quality check failed: {table_name} returned no results")
            self.log.info(f"Data quality on table: {table_name} check passed with {records[0][0]} records")
