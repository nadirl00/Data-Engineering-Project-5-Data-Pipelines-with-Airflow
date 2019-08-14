from datetime import datetime, timedelta
import os
from airflow import DAG

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator,
                               LoadFactOperator,
                               LoadDimensionOperator, 
                               DataQualityOperator, 
                               PythonOperator,
                               PostgresOperator)
from helpers import SqlQueries
import create_tables

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 8, 12),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
#          schedule_interval='@once'         
          schedule_interval='0 * * * *'
        )

start_operator = PostgresOperator(
    task_id='Begin_execution',  
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables.CREATE_TABLES_SQL
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="public.staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id= "aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data", 
    region="us-west-2",
    json="s3://udacity-dend/log_json_path.json"

)


stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="public.staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id= "aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    region="us-west-2",
    json="auto"

)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_statement = SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table = "public.users",
    sql_statement = SqlQueries.user_table_insert,
    truncate_insert = True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.songs",
    sql_statement = SqlQueries.song_table_insert,
    truncate_insert = True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table = "public.artists",
    sql_statement = SqlQueries.artist_table_insert,
    truncate_insert = True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.\"time\"",
    sql_statement = SqlQueries.time_table_insert,
    truncate_insert = True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    retries=3,
    tables = ({"public.artists":[["artistid"],{"SELECT COUNT(*) FROM public.artists": 10025}],       
              "public.songplays":[["playid","start_time","userid"],{}],"public.songs":[["songid"],{"SELECT COUNT(*) FROM public.songs": 14896}], 
              "public.staging_events":[[],[]],
              "public.staging_songs":[[],[]], 
              "public.\"time\"":[["start_time"],[]],
              "public.users":[["userid"],{"SELECT COUNT(*) FROM public.users": 104}]}),
    redshift_conn_id = "redshift"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >>  load_songplays_table

load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator

