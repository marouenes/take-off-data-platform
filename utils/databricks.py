"""
Helper module for databricks connect and remote execution
"""
from __future__ import annotations

from typing import Any, Tuple, Callable

import logging
import IPython as ip

import pyspark.sql.functions as sf
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, ArrayType


# pylint: disable=invalid-name
clusters = {
    "dev": {"id": "cluster id", "port": "port"},
    "prod": {"id": "cluster id", "port": "port"},
}

# Logging
class SilenceFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return False


logging.basicConfig(
    format="%(asctime)s — %(name)s — %(levelname)s — %(message)s",
    level=logging.INFO
)
logging.getLogger("py4j.java_gateway").addFilter(SilenceFilter())
log = logging.getLogger("dbconnect")


def _check_is_databricks() -> bool:
    user_ns = ip.get_ipython().user_ns
    return "displayHTML" in user_ns


def _get_spark() -> SparkSession:
    user_ns = ip.get_ipython().user_ns
    if "spark" in user_ns:
        return user_ns["spark"]
    else:
        spark = SparkSession.builder.getOrCreate()
        user_ns["spark"] = spark
        return spark


def _display(df: DataFrame):
    df.show(truncate=False)


def _display_with_json(df: DataFrame):
    for column in df.schema:
        t = type(column.dataType)
        if t == StructType or t == ArrayType:
            df = df.withColumn(column.name, sf.to_json(column.name))
    df.show(truncate=False)


def _get_display() -> Callable[[DataFrame], None]:
    fn = ip.get_ipython().user_ns.get("display")
    return fn or _display_with_json


def _get_dbutils(spark: SparkSession) -> dbutils:
    try:
        from pyspark.dbutils import DBUtils

        dbutils = DBUtils(spark)
    except ImportError:
        import IPython

        dbutils = IPython.get_ipython().user_ns.get("dbutils")
        if not dbutils:
            log.warning("could not initialise dbutils!")
    return dbutils


# initialise Spark variables
is_databricks: bool = _check_is_databricks()
spark: SparkSession = _get_spark()
display: Callable[[DataFrame], None] = _get_display()
dbutils: dbutils = _get_dbutils(spark)


def use_cluster(cluster_name: str):
    """
    When running via Databricks Connect, specify to which cluster to connect instead of the default cluster.
    This call is ignored when running in Databricks environment.

    :param cluster_name: Name of the cluster as defined in the beginning of this file.
    """
    real_cluster_name = spark.conf.get(
        "spark.databricks.clusterUsageTags.clusterName", None
    )

    # do not configure if we are already running in Databricks
    if not real_cluster_name:
        cluster_config = clusters.get(cluster_name)
        log.info(
            f"attaching to cluster '{cluster_name}'"
            f"(id: {cluster_config['id']}, port: {cluster_config['port']})"
        )

        spark.conf.set("spark.driver.host", "127.0.0.1")
        spark.conf.set(
            "spark.databricks.service.clusterId", cluster_config["id"]
        )
        spark.conf.set("spark.databricks.service.port", cluster_config["port"])
