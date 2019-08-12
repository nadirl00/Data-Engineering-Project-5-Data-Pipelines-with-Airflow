from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 stl_statement="",
                 #destination table is always the same. Does not need to be specified.
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.stl_statement=stl_statement

    def execute(self, context):
        self.log.info('Inserting into fact table')
        

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)        
        # a simple insert is required here
        completeSql = "INSERT INTO public.songplays " + self.stl_statement
        self.log.info(completeSql)
        redshift.run(completeSql)
