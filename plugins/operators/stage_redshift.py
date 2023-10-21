from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    '''Operator loads any JSON formatted file from S3 bucket to Amazon Redshift tables.

    redshift_conn_id: the Redshift connection which configured in Airflow Connection
    aws_credentials_id: the AWS credential connection which configured in Airflow Connection for granting access to s3 bucket
    table: the target Amazon Redshift table where the data is loaded into
    s3_path: the s3 bucket where the data is extracted from

    '''
    ui_color = '#358140'

    """ templated field allows to load timestamped files from S3 based on the execution time and run backfills.
    template_field is a special property of the operator class to tell the compiler that the s3_key is a variable 
    that contains system context/template variables encoded within the {<system variable name key identifier>}.
    """
    template_fields = ("s3_key",)

    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}'
        REGION '{}'
    """

# Define operators params
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 table = "",
                 s3_bucket= "",
                 s3_key = "",
                 json_path = "",
                 region="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params 
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.json_path = json_path

    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')
        self.log.info('Getting Redshift credentials')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        self.log.info('Getting Redshift connection')
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        self.log.info('Clearing data from destination Redshift table')
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        """ ` context` argument is Airflow Operator's feature that s3_key needs it to be formatted by. 
        the compiler matches the system variable key identifier name (Eg: execute_date.year) to the key values stored in the **context dictionary.
        Once matched, the value corresponding to this key is then substituted into the s3_key.
        """
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.sercret_key,
            self.json_path,
            self.region
        )

        try:
            redshift.run(formatted_sql)
        except Exception as e:
            self.log.info(e)
            result = redshift.run("""
                SELECT *
                FROM sys_load_error_detail
                ORDER BY start_time DESC
                LIMIT 10;
                """)
            rows = result.fetchall()
            for row in rows:
                self.log.info(row)

        self.log.info(f" Success Copying data from '{s3_path}' to '{self.table}'")
        
        





