"""
Fixtures for pytest
"""
from __future__ import annotations
from pathlib import Path
import shutil
import tempfile

import pytest
import yaml

import logging

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


@pytest.fixture(scope='session')
def spark() -> SparkSession:
    """
    This fixture provides preconfigured SparkSession with Hive support.
    After the test session, temporary warehouse directory is deleted.
    :return: SparkSession
    """
    logging.info('Configuring Spark session for testing environment')
    warehouse_dir = tempfile.TemporaryDirectory().name
    _builder = (
        SparkSession.builder.master('local[1]')
        .config('spark.hive.metastore.warehouse.dir', Path(warehouse_dir).as_uri())
        .config('spark.sql.session.timeZone', 'UTC')
    )
    spark: SparkSession = _builder.getOrCreate()
    logging.info('Spark session configured')

    yield spark

    logging.info('Shutting down Spark session')
    spark.stop()  # TODO: remove since unit tests are run in a separate process?

    if Path(warehouse_dir).exists():
        shutil.rmtree(warehouse_dir)
