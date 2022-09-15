"""
Fixtures for pytest
"""

import pytest
import yaml

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import DagBag


# path definition for the airflow user profile
config_loc = "profiles/dev/user.yml"

# get the path to the dag folder
with open(config_loc, "r") as f:
    config = yaml.safe_load(f)
    DAGS_FOLDER = config["DAGS_FOLDER"]


@pytest.fixture(scope="session")
def dag_bag():
    """
    Fixture to load all dags in the DAGS_FOLDER
    """
    return DagBag(dag_folder=DAGS_FOLDER, include_examples=False)


@pytest.fixture
def dag_():
    """
    Create a DAG for testing
    """
    default_args = {"owner": "foo", "start_date": days_ago(1)}
    assert len(dag_bag.import_errors) == 0
    return DAG("test_dag", default_args=default_args, schedule_interval="@daily")