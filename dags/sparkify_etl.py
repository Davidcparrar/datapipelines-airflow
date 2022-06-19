from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
)
from helpers import SqlQueries

create_staging_events = """
    CREATE TABLE public.staging_events (
        artist varchar(256),
        auth varchar(256),
        firstname varchar(256),
        gender varchar(256),
        iteminsession int4,
        lastname varchar(256),
        length numeric(18,0),
        "level" varchar(256),
        location varchar(256),
        "method" varchar(256),
        page varchar(256),
        registration numeric(18,0),
        sessionid int4,
        song varchar(256),
        status int4,
        ts int8,
        useragent varchar(256),
        userid int4);
    """
create_staging_songs = """
    CREATE TABLE public.staging_songs (
        num_songs int4,
        artist_id varchar(256),
        artist_name varchar(400),
        artist_latitude numeric(18,0),
        artist_longitude numeric(18,0),
        artist_location varchar(300),
        song_id varchar(256),
        title varchar(300),
        duration numeric(18,0),
        "year" int4);
    """

queries = SqlQueries()
# If the credentials arte not set in the Airflow UI, the user can set
# them as environment variables
# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    "owner": "udacity",
    "start_date": datetime(2019, 1, 12),
    "retries": 3,
    "retry_delay": timedelta(300),
    "email_on_retry": False,
    "depends_on_past": False,
}

dag = DAG(
    "sparkify_etl_dag",
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="0 * * * *",
    catchup=False,
)

start_operator = DummyOperator(
    task_id="Begin_execution", dag=dag, **default_args
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    dag=dag,
    table="staging_events",
    s3_key="log-data",
    s3_bucket="udacity-dend",
    json_path="log_json_path.json",
    region="us-west-2",
    create_sql=create_staging_events,
    aws_credentials_id="aws_credentials",
    redshift_conn_id="redshift",
    **default_args
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="Stage_songs",
    dag=dag,
    table="staging_songs",
    s3_key="song-data/A/A/A/",
    s3_bucket="udacity-dend",
    region="us-west-2",
    create_sql=create_staging_songs,
    aws_credentials_id="aws_credentials",
    redshift_conn_id="redshift",
    **default_args
)

# load_songplays_table = LoadFactOperator(
#     task_id="Load_songplays_fact_table", dag=dag
# )

# load_user_dimension_table = LoadDimensionOperator(
#     task_id="Load_user_dim_table", dag=dag
# )

# load_song_dimension_table = LoadDimensionOperator(
#     task_id="Load_song_dim_table", dag=dag
# )

# load_artist_dimension_table = LoadDimensionOperator(
#     task_id="Load_artist_dim_table", dag=dag
# )

# load_time_dimension_table = LoadDimensionOperator(
#     task_id="Load_time_dim_table", dag=dag
# )

# run_quality_checks = DataQualityOperator(
#     task_id="Run_data_quality_checks", dag=dag
# )

# end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
