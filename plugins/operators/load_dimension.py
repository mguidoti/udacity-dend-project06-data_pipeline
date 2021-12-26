from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 insert_statement="",                                  
                 append_mode=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.insert_statement = insert_statement
        self.append_mode = append_mode

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.append_mode == False:
            self.log.info("Clearing data from destination Redshift table `{}`".format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))

        self.log.info(("Running SQL Statement to insert lines" 
                       "into table `{}`: {}").format(self.table,
                                                     self.insert_statement))
        redshift.run(self.insert_statement)
