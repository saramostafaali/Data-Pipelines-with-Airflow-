from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """This Operator loads data into the target Dimension table in redshidt."""

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define operators params (with defaults)
                 redshift_conn_id = "",
                 table = "",
                 insert_statement = "",  
                 truncate = True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params 
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.insert_statement = insert_statement
        self.truncate = truncate

    def execute(self, context):
        self.log.info("LoadDimensionOperator not implemented yet")
        # Getting Redshift connection
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate:
            self.log.info(f"Truncating dimension table: {self.table}")
            redshift.run(f'TRUNCATE TABLE {self.table}')

        self.log.info(f"Appending the data into the Dimension table {self.table}")
        insert_script = f"INSERT INTO {self.table} {self.insert_statement}"
        redshift.run(insert_script)
        self.log.info(f"Successfully Loading dimension table {self.table} in Redshift")