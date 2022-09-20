"""
Fixtures for pytest
"""
from __future__ import annotations

import pytest
import yaml

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import DagBag

from pyspark.sql import SparkSession
# pylint: disable=redefined-outer-name

# path definition for the airflow user profile
config_loc = "profiles/config_dev.yaml"

# get the path to the dag folder
with open(config_loc, "r") as file:
    config = yaml.safe_load(file)
    DAG_FOLDER = config["env_vars"]["DAGS_FOLDER"]


@pytest.fixture(scope="session")
def dag_bag() -> DagBag:
    """
    Fixture to load all dags in the DAGS_FOLDER
    """
    return DagBag(dag_folder=DAG_FOLDER, include_examples=False)


@pytest.fixture
def dag_() -> DAG:
    """
    Create a DAG for testing
    """
    default_args = {"owner": "foo", "start_date": days_ago(1)}
    assert len(dag_bag.import_errors) == 0
    return DAG("test_dag", default_args=default_args, schedule_interval="@daily")


@pytest.fixture(scope="session", autouse=True)
def spark() -> SparkSession:
    """
    Create a spark session
    @TODO: use a generator function instead?
    """
    return (
        SparkSession
        .builder
        .master("local[1]")
        .getOrCreate()
    )

    # clean up
    # spark.stop()
