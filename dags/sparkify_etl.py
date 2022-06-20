from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    DataQualityOperator,
)
from helpers import SqlQueries
from load_dimension_subdag import load_dimensions_dag

# If the credentials are not set in the Airflow UI, the user can set
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
    schedule_interval="0 * * * *",
)

start_operator = DummyOperator(
    task_id="Begin_execution", dag=dag, **default_args
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    dag=dag,
    table="staging_events",
    s3_key="log_data/{execution_date.year}/{execution_date.month}/{ds}-events.json",
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
    s3_key="song_data/",
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

tables = {
    "users": SqlQueries.user_table_insert,
    "songs": SqlQueries.song_table_insert,
    "artists": SqlQueries.artist_table_insert,
    "time": SqlQueries.time_table_insert,
}


load_dimension_task = SubDagOperator(
    subdag=load_dimensions_dag(
        parent_dag_name="sparkify_etl_dag",
        task_id="load_dimensions",
        tables=tables,
        redshift_conn_id="redshift",
        append=False,
        start_date=default_args["start_date"],
    ),
    task_id="load_dimensions",
    dag=dag,
)

tables_quality = list(tables.keys())

data_quality_template = [
    {
        "name": "Empty rows",
        "sql": "SELECT COUNT(*) FROM public.{table}",
        "result": 0,
        "operator": "gt",
        "format": [{"table": x for x in tables_quality}],
    },
    {
        "name": "Null rows",
        "sql": "SELECT COUNT(*) - COUNT({column}) FROM public.{table}",
        "result": 0,
        "operator": "eq",
        "format": [{"table": "users", "column": "userid"}],
    },
]

run_quality_checks = DataQualityOperator(
    task_id="Run_data_quality_checks",
    dag=dag,
    redshift_conn_id="redshift",
    template=data_quality_template,
)

end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_songs_to_redshift >> load_songplays_table
stage_events_to_redshift >> load_songplays_table
load_songplays_table >> load_dimension_task
load_dimension_task >> run_quality_checks
run_quality_checks >> end_operator
