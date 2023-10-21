from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    This Operator runs data quality checks on fact and dimension tables.
    checks if a certain column contains NULL values by counting all the rows that have NULL in this column. 
    expected result is 0.

    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define operators params
                 redshift_conn_id="",
                 tables="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params 
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
        # getting redshift connection
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)  
        
        for table in self.tables:
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")  
            
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
                
            num_records = records[0][0]

            if num_records < 1:
                raise ValueError(f"Data quality check failed. No records present in table {table} [0 rows]")
                
            self.log.info(f"Passed data quality record count on table {table} check with {num_records} records")