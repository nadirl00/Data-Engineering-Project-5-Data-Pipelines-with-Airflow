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
                 stl_statement = "",
                 #missing target table
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.stl_statement = stl_statement
        self.table=table

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        #dimension must be emptied before a load

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("DELETE FROM {}".format(self.table))
        redshift.run("DELETE FROM {}".format(self.table))
        completeSql = "INSERT INTO {} ".format(self.table) + self.stl_statement
        self.log.info("Inserting to dim table")
        redshift.run(completeSql)
