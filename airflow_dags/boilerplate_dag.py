"""
Spark dbc for testing remote job execution
"""

from datetime import datetime
from email.mime import application
import os
from airflow import DAG

# from utils.spark import SparkOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator


# regsiter the environment variables defined in the airflow user profile
# TODO: use airflow variables instead?
run_mode = os.environ["MODE"]
user = os.environ["USER"]

default_args = {
    "owner": "foo",
    "depends_on_past": False,
    "start_date": datetime(2022, 9, 1),
}

spark_defaults = {
    "spark.executor.memory": "2g",
    "spark.executor.cores": "2",
    "spark.dynamicAllocation.enabled": "true",
    "spark.dynamicAllocation.maxExecutors": "4",
    "spark.shuffle.service.enabled": "true",
}

if run_mode == "debug": # or UAT?
    repo_path = f"/home/{user}/git/scheduler"
    schedule = None
    input_path = "store/input.csv"
    output_path = "store/output.csv"
    # spark_defaults["spark.executor.memory"] = "1g"
    # spark_defaults["spark.executor.cores"] = "1"
    # spark_defaults["spark.dynamicAllocation.maxExecutors"] = "1"

elif run_mode == "dev":
    repo_path = "s3://some_s3_repo_location"
    schedule = None
    input_path = "s3://foo/bar_dev"
    output_path = "s3://foo/bar_dev"

elif run_mode == "prod":
    repo_path = "s3://some_s3_repo_location"
    schedule = "@daily"
    input_path = "s3://foo/bar"
    output_path = "s3://foo/bar"

else:
    raise ValueError(f"Unknown run mode: {run_mode}")


# DAG definition
with DAG(
    "spark-boilerplate",
    default_args=default_args,
    schedule_interval=schedule,
    catchup=False,
    doc_md=__doc__,
) as dag:

    # boilerplate = SparkOperator(
    #     dag=dag,
    #     repo_path="/home/marouane-skandaji/git/scheduler",
    #     task_id="spark-databricks",
    #     verbose=True,
    #     application="/home/marouane-skandaji/git/scheduler/boilerplate.py",
    #     **spark_defaults,
    # )

    boilerplate = SparkSubmitOperator(
        dag=dag,
        task_id="spark-on-databricks",
        verbose=True,
        application=f"{repo_path}/reporting/boilerplate.py",
        application_args=[
            input_path,
            output_path
        ],
    )

    # graph dependencies
    boilerplate
