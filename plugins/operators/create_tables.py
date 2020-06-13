from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CreateTableOperator(BaseOperator):
    """
    Creates Emtpy SQL Tables in AWS Redshift

    Parameters:
    redshift_conn_id: Connection Id of the Airflow connection to AWS Redshift database (Postgres Type)
    create_tables: List of Table names to create empty tables in AWS Redshift

    Returns: None
    """

    ui_color = '#FFFF00'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 create_tables=[],
                 *args, **kwargs):
        super(CreateTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.create_tables = create_tables

    def execute(self, context):
        self.log.info('Fetching redshift hook')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for table_query in self.create_tables:
            self.log.info(f'Creating {table_query} Table in AWS Redshift')
            redshift.run(table_query)
