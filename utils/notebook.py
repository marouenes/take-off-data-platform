"""
Helper module for spark sessions in a jupyter notebook.
contains common tools to manage ways of working with jupyter Lab.
"""
from __future__ import annotations

from optparse import Option
import os
import getpass
from threading import Timer
from datetime import datetime, timedelta
from typing import Dict, Optional

from pyspark.sql import SparkSession


def generate_session(session_name: str,
                     queue: str,
                     max_executors: int = 2,
                     executor_memory: str ='5g',
                     num_cores: int = 5,
                     conf: Optional[Dict] = None,
                     automatic_shutdown_in_seconds: Optional[int] = None) -> SparkSession:
    """
    Function to generate a jupyter-lab configured spark session object
    returns sparksession object with dynamic allocation configured correctly

    Function starts by creating a dict of conf settings given the exposed variables,
    as well as some standardized paramters set in DSI analytics.

    :param automatic_shutdown_in_seconds: If provided, a timer will shut down the
                                          spark session automatically. Expects a number in seconds.

    If user inputs conf variable, then these can override the functions defaults.
    """
    conf_dict = {
        "spark.sql.session.timeZone": "UTC",
        "spark.shuffle.service.enabled" : "true",
        "spark.dynamicAllocation.enabled" : "true",
        "spark.dynamicAllocation.maxExecutors" : str(max_executors),
        "spark.executor.cores" : str(num_cores),
        "spark.executor.memory" : executor_memory,
        "spark.master": 'yarn',
        "spark.yarn.queue": str(queue)
    }

    if conf:
        for key, value in conf.items():
            conf_dict[key] = conf[key]

    # crate spark session without submitting it to YARN
    spark_session = SparkSession.builder
    for key, value in conf_dict.items():
        # Add config parameters
        spark_session = spark_session.config(key, value)

    spark_session = (
        spark_session
        .appName(session_name)
        .enableHiveSupport()
        .getOrCreate()
    )

    if automatic_shutdown_in_seconds is None:
        print("Parameter 'automatic_shutdown_in_seconds' not set. "
              'Remember to shut down your spark session manually!')
    else:
        shutdown_time = datetime.now() + timedelta(seconds=automatic_shutdown_in_seconds)
        print('Automatic shutdown of spark session', shutdown_time)

        def stop_spark():
            print('=================================================')
            print('            Shutdown timer triggered:            ')
            print('              Stoping spark session              ')
            print('=================================================')
            spark_session.stop()
        Timer(automatic_shutdown_in_seconds, stop_spark).start()

    return spark_session
