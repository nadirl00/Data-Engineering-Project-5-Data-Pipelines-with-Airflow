from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_statement="",
                 #destination table is always the same. It does not need to be specified.
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.sql_statement=sql_statement

    def execute(self, context):
        """
        This operator collects data from the staging tables and inserts it into the fact table. It uses the following parameters: 
            redshift_conn_id : contains the connection details to the data warehouse in Amazon Redshift (from in Airflow)
            sql_statement : contains the SQL statement collect the data that should be in the fact table.
        """
        self.log.info("Loading data into the songplays table")
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)   
        # Combine the insert statement with the select statement that collects the data for the fact table.
        completeSql = "INSERT INTO public.songplays " + self.sql_statement
        #insert the data
        redshift.run(completeSql)
        self.log.info("Loading fact table completed")