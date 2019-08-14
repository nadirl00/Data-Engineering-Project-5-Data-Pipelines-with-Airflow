from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table = "",
                 sql_statement = "",
                 truncate_insert = True, # default value is true
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.sql_statement = sql_statement
        self.table=table
        self.truncate_insert=truncate_insert

    def execute(self, context):
        """
        This operator loads data from the staging tables to the dimension tables. It requires the following parameters:
            redshift_conn_id : contains the connection details to the data warehouse in Amazon Redshift (from in Airflow)
            table : the name of the destination dimension table
            sql_statement : the SQL query to get the data to be inserted in the destination table
            truncate-insert : boolean indicating whether the dimension table is to be emptied before inserting the data
        """
        self.log.info("Loading data into the {} table".format(self.table))
        #connect to redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        #if the parameter is true, we empty the dimension before loading the data
        if self.truncate_insert:
            self.log.info("Deleting rows in the {} table".format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))

        completeSql = "INSERT INTO {} ".format(self.table) + self.sql_statement
        self.log.info(completeSql)
        redshift.run(completeSql)
        self.log.info("Insertion into the {} table completed.".format(self.table))        
