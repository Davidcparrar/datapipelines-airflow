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

# If the credentials arte not set in the Airflow UI, the user can set
# them as environment variables
# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    "owner": "udacity",
    "start_date": datetime(2018, 11, 1),
    "retries": 3,
    "retry_delay": timedelta(300),
    "email_on_retry": False,
    "depends_on_past": False,
    "catchup": False,
}

dag = DAG(
    "sparkify_etl_dag",
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    # schedule_interval="0 * * * *",
    schedule_interval="@once",
)

start_operator = DummyOperator(
    task_id="Begin_execution", dag=dag, **default_args
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    dag=dag,
    table="staging_events",
    s3_key="log-data",  # /{execution_date.year}/{execution_date.month}/{ds}-events.json",
    s3_bucket="udacity-dend",
    json_path="log_json_path.json",
    region="us-west-2",
    aws_credentials_id="aws_credentials",
    redshift_conn_id="redshift",
    provide_context=True,
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="Stage_songs",
    dag=dag,
    table="staging_songs",
    s3_key="song-data/A/",
    s3_bucket="udacity-dend",
    region="us-west-2",
    aws_credentials_id="aws_credentials",
    redshift_conn_id="redshift",
)

load_songplays_table = LoadFactOperator(
    task_id="Load_songplays_fact_table",
    dag=dag,
    table="songplays",
    staging_songs="staging_songs",
    staging_events="staging_events",
    select=SqlQueries.songplay_table_insert,
    redshift_conn_id="redshift",
)

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
stage_songs_to_redshift >> load_songplays_table
stage_events_to_redshift >> load_songplays_table
