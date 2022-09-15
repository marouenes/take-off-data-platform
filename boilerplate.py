"""
Test the databricks conection
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

import logging


# initialize the logger and set the level
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
logger.addHandler(handler)

def main(spark: SparkSession) -> None:
    """
    Running a dummy spark job for testing
    """
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    ])

    data = [
    (1, "John", 19),
    (2, "Smith", 29),
    (3, "Adam", 35),
    ]

    df = spark.createDataFrame(data=data, schema=schema)
    df.show()


if __name__ == '__main__':
    # pylint: disable=value-for-parameter
    main(*sys.argv[1:])
