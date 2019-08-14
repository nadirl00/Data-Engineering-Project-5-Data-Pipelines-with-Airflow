from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults



class StageToRedshiftOperator(BaseOperator):

    ui_color = '#358140'
    
    template_fields = ("s3_key",)
    copy_sql    = """
     COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}' 
        FORMAT AS JSON '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 region="",
                 json="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.region=region
        self.json=json

    def execute(self, context):
        """
        This operator copies data from S3 to Redshift staging tables. It has the following parameters:
        - redshift_conn_id: contains the connection details to the data warehouse in Amazon Redshift (from in Airflow)
        - aws_credentials_id: contains the credentials to connect to the S3 bucket (from in Airflow)
        - table: contains the name of the table where the data from S3 is to be copied.
        - s3_bucket: contains info on the S3 bucket where the information is located
        - s3_key: contains info on the S3 bucket where the information is located
        - region: contains the region where the S3 bucket is located
        - json: JSON formatting parameter
        """

        self.log.info(f"Starting the staging step for {self.table} table.")

        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Clearing data from destination {self.table} table on Redshift")
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info(f"Copying data from S3 to {self.table} table on Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.json
        )
        redshift.run(formatted_sql)
        self.log.info(f"Copying to {self.table} table completed successfully ")



