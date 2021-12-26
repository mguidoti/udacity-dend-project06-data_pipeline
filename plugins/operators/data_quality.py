from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    
    check_nulls = """
        SELECT COUNT(*)
        FROM {}
        WHERE {} IS NULL
    """
    
    check_count = """
        SELECT COUNT(*)
        FROM {}
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id = "",
                 tables = list(),
                 quality_params = dict(),
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.quality_params = quality_params

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)    
        
        
        for table in self.tables:
            # Checking if a tables is empty
            self.log.info("Data Quality: Checking number of rows of table `{}`...".format(table))
            checking_rows = redshift_hook.get_records(DataQualityOperator.check_count.format(table))
            if len(checking_rows) < 1 or len(checking_rows[0]) < 1:
                raise ValueError("Data Quality FAILED! Table `{}` returned no results".format(table))
                                
            num_rows = checking_rows[0][0]
            if num_rows < self.quality_params[table]["min_number_rows"]:
                raise ValueError(("Data quality FAILED! Table {} contained less than" 
                                  "the minimum expected number of rows ({})").format(table,
                                                                                     self.quality_params[table]["min_number_rows"]))            
            # Check if any of the target columns for a specific table have nulls
            for column in self.quality_params[table]["not_null_columns"]:
                self.log.info("Data Quality: Checking nulls on column `{}` of table `{}`...".format(column, 
                                                                                                 table))
                
                checking_nulls = redshift_hook.get_records(DataQualityOperator.check_nulls.format(table, 
                                                                                                  column))
            
                num_rows_with_nulls = checking_nulls[0][0]
                if num_rows_with_nulls > 0:
                    raise ValueError("Data quality FAILED! Table `{}` contained nulls on column `{}`".format(table,
                                                                                                           column))
                else:
                    self.log.info("Data Quality: No nulls on column `{}` of table `{}` were found...".format(column, 
                                                                                                             table))
            