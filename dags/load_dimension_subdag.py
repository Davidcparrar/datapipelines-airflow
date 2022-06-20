import datetime

from airflow import DAG
from airflow.operators import LoadDimensionOperator, DataQualityOperator
from helpers import SqlQueries


def load_dimensions_dag(
    parent_dag_name,
    tables,
    task_id,
    redshift_conn_id,
    tables_quality,
    check_null_columns,
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

    # Make quality checks
    run_quality_checks = DataQualityOperator(
        task_id="Run_data_quality_checks",
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        tables=tables_quality,
        table_nnull_columns=check_null_columns,
    )

    # Create dependencies
    for dimension in load_dimension_table:
        dimension >> run_quality_checks

    return dag
