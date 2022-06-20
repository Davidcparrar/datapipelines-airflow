import datetime

from airflow import DAG
from airflow.operators import LoadDimensionOperator
from helpers import SqlQueries


def load_dimensions_dag(
    parent_dag_name,
    tables,
    task_id,
    redshift_conn_id,
    append,
    *args,
    **kwargs,
):
    # Set up subdag
    dag = DAG(f"{parent_dag_name}.{task_id}", **kwargs)

    # Loop thorugh dimensions and fill tables
    load_dimension_table = []
    for table, select in tables.items():
        load_dimension_table.append(
            LoadDimensionOperator(
                task_id=f"Load_{table}_dim_table",
                dag=dag,
                table=table,
                select=select,
                redshift_conn_id=redshift_conn_id,
                append=append,
            )
        )

    return dag
