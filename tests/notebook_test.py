"""
Set of tests for notebook helper module.
"""

import pytest

from utils.notebook_utils import generate_session


def test_generate_session():
    """
    Tests generate_session by setting up inputs generating a local
    sparksession.

    if the expected settings mismatch those of the session, fail test.
    """
    # variables for the test
    conf_override = {
        "spark.shuffle.service.enabled" : "false",
        "spark.dynamicAllocation.enabled" : "false",
        "spark.master": 'local[1]'

    }

    # Expected options in conf given that the function was successful
    expected_conf = {
        "spark.shuffle.service.enabled" : "false",
        "spark.dynamicAllocation.enabled" : "false",
        "spark.master": 'local[1]',
        "spark.dynamicAllocation.maxExecutors" : "1",
        "spark.executor.cores" : "1",
        "spark.executor.memory" : "1g",
        "spark.yarn.queue": "root.local"
    }

    # Run function and extract conf of resulting conf object
    spark_session = generate_session('test_session',
                                     'root.local',
                                     max_executors=1,
                                     executor_memory="1g",
                                     num_cores=1,
                                     conf=conf_override)
    session_config = dict(spark_session.sparkContext.getConf().getAll())

    mismatch_list = []
    for key, value in expected_conf.items():
        if session_config[key] != value:
            err_message = f"Error for key {key}. expected {value}, got {session_config[key]}"
            print(err_message)
            mismatch_list.append(err_message)

    # assert that there were no mismatches.
    assert len(mismatch_list) == 0, str(mismatch_list)
