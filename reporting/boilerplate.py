"""
Dummy spark job to test airflow
"""
import sys
from typing import List

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as sf
import pyspark.sql.types as st

import logging


# initialize the logger and set the level
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
logger.addHandler(handler)


def main(input_path: DataFrame, output_path: DataFrame):
    """Running a dummy spark job for testing"""
    spark = (
        SparkSession
        .builder()
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("INFO")
    spark.conf.set("spark.submit.deployMode", "local")

    # read the input data
    input_df = spark.read.csv(input_path, header=True, inferSchema=True)

    # transform X
    transformed_df = to_uppercase(input_df, ["foo", "bar"])

    # collect the result
    transformed_df.show()

    # write to the output path as (parquet, csv, ...)
    # input_df.write.csv(output_path, header=True)
    transformed_df.write.csv(output_path, header=True)

    # teardown
    spark.stop()


def to_uppercase(df: DataFrame, input_cols: List[str]) -> DataFrame:
    """Uppercase the columns provided in the dataframe

    :param df           : input dataframe
    :param input_cols   : list of columns to be transformed
    :return             : the transformed dataframe
    """
    for col_ in input_cols:
        if col_ in df.columns:
            df = df.withColumn(col_, sf.upper(sf.col(col_)))

    return df


if __name__ == '__main__':
    # pylint: disable=value-for-parameter
    main(*sys.argv[1:])
