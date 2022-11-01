"""
Airflow DAG for running the spark application system
"""

import os
from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

# regsiter the environment variables defined in the airflow user profile
# TODO: use airflow variables instead?
run_mode = os.environ['MODE']
base_path = os.environ['HOME']
git = f'{base_path}/git-personal'

default_args = {
    'owner': 'foo',
    'depends_on_past': False,
    'start_date': datetime(2022, 9, 1),
}

spark_defaults = {
    'spark.executor.memory': '2g',
    'spark.executor.cores': '2',
    'spark.dynamicAllocation.enabled': 'true',
    'spark.dynamicAllocation.maxExecutors': '4',
    'spark.shuffle.service.enabled': 'true',
    # add the jdbc jar driver class to spark
    'spark.driver.extraClassPath': (
        '/opt/spark-2.4.4-bin-hadoop2.7/jars/mssql-jdbc-11.2.0.jre8.jar'
    ),
}

if run_mode == 'debug':  # or UAT?
    repo_path = f'{git}/scheduler'
    schedule = None
    input_path = f'{repo_path}/store/input.csv'
    output_path = f'{repo_path}/store/output'
    input_table = 'dbo.Companies'
    output_table = 'dbo.Reporting'

# XXX: dummy dag for testing
elif run_mode == 'dev':
    repo_path = 's3://some_s3_repo_location'
    schedule = None
    input_path = 's3://foo/bar_dev'
    output_path = 's3://foo/bar_dev'
    input_table = 'dbo.Companies'
    output_table = 'dbo.Reporting'

elif run_mode == 'prod':
    repo_path = 's3://some_s3_repo_location'
    schedule = '@daily'
    input_path = 's3://foo/bar'
    output_path = 's3://foo/bar'
    input_table = 'dbo.Companies'
    output_table = 'dbo.Analytics'

else:
    raise ValueError(f'Unknown run mode: {run_mode}')


# DAG definition
with DAG(
    'process-sql-data',
    default_args=default_args,
    schedule_interval=schedule,
    catchup=False,
    doc_md=__doc__,
) as dag:

    spark_local = SparkSubmitOperator(
        dag=dag,
        conn_id='spark_local',
        task_id='spark_submit_boilerplate',
        verbose=True,
        application=f'{repo_path}/reporting/formatting.py',
        application_args=[
            input_path,
            output_path,
        ],
        conf=spark_defaults,
    )

    create_table = SparkSubmitOperator(
        dag=dag,
        conn_id='spark_local',
        task_id='aggregate_table',
        verbose=True,
        application=f'{repo_path}/reporting/aggregation.py',
        application_args=[
            input_table,
            output_table,
        ],
        conf=spark_defaults,
    )

    spark_local >> create_table
