from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON {}
        region 'us-west-2'
        BLANKSASNULL EMPTYASNULL;
    """
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_mode="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_mode = json_mode

    def execute(self, context):
        
        # Retrieves credentials from connection and creates an instance of AwsHook
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        # Creates an instance of PostgresHook passing the redshit connection
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Clean destination table before copying data
        self.log.info("Clearing data from destination Redshift table `{}`".format(self.table))
        redshift.run(f"DELETE FROM {self.table}")

        # Defines s3 path 
        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"
        
        self.log.info("Copying data from S3 ({}) to Redshift table `{}`".format(s3_path, 
                                                                                self.table))
        # Formats SQL statement to be run
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_mode
        )
                      
        self.log.info("Running SQL Statement: {}".format(formatted_sql))
        redshift.run(formatted_sql)





