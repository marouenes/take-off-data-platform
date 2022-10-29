"""
Dummy spark job to test airflow

@TODO: move logging to a separate utils module?
"""
from __future__ import annotations

import sys

import pyspark.sql.functions as sf
from pyspark import SparkContext as sc
from pyspark.sql import DataFrame, SparkSession

# initialize the spark logger
log4jLogger = sc._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger(__name__)
logger.setLevel(log4jLogger.Level.INFO)


def main(input_path: DataFrame, output_path: DataFrame):
    """Running a dummy spark job for testing"""
    spark = (
        SparkSession
        .builder
        .appName('boilerplate_spark')
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel('INFO')
    spark.conf.set('spark.submit.deployMode', 'local')

    # read the input data
    input_df = spark.read.csv(input_path, header=True, inferSchema=True)

    # transform X
    transformed_df = to_uppercase(input_df, ['foo', 'bar'])

    # collect the result
    transformed_df.show()

    # write to the output path as (parquet, csv, ...)
    transformed_df.write.csv(output_path, header=True)

    # teardown
    spark.stop()


def to_uppercase(df: DataFrame, input_cols: list[str]) -> DataFrame:
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
