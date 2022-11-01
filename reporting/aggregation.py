"""
Spark application to read data from Azure SQL Server and write back to the data store.

@TODO: move logging to a separate utils module?
"""
from __future__ import annotations

import os
import re
import sys

import pyspark.sql.functions as sf
from faker import Faker
from pyspark import SparkContext as sc
from pyspark.sql import DataFrame, SparkSession

# append to sys path for airflow to find the modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.schema import companies_schema  # noqa: E402

base_path = os.environ['HOME']
git = f'{base_path}/git-personal'
path_matcher = re.compile(r'\$\{([^}^{]+)\}')
host = os.environ['MSSQL_HOST']
port = os.environ['MSSQL_PORT']
database = os.environ['MSSQL_DATABASE']
user = os.environ['MSSQL_USER']
password = os.environ['MSSQL_PASSWORD']
driver = os.environ['MSSQL_DRIVER']

# ! parse the environment variables from the YAML config file
'''
TODO: Append the logger properties for parsing the yaml config file.
PY-yaml library doesn't resolve environment variables by default.
You need to define an implicit resolver that will find the regex
that defines an environment variable and execute a function to resolve it.

def path_constructor(loader: yaml.Loader, node: yaml.Node) -> str:
    """
    This function is called when the yaml parser finds a match for the regex
    defined in path_matcher. It will replace the environment variable with
    the value defined in the environment.

    :param loader: the yaml loader
    :param node: the node that contains the environment variable
    :return: the value of the environment variable
    """
    value = node.value
    match = path_matcher.match(value)
    env_var = match.group()[2:-1]
    return os.environ.get(env_var) + value[match.end():]


yaml.add_implicit_resolver('!path', path_matcher)
yaml.add_constructor('!path', path_constructor)


with open(f'{git}/scheduler/profiles/config_dev.yaml') as stream:
    profile_dict = yaml.load(stream, Loader=yaml.FullLoader)
    credentials = profile_dict['datasources']
    host = credentials['host']
    port = credentials['port']
    database = credentials['database']
    user = credentials['user']
    password = credentials['password']
    driver = credentials['driver']
'''


def main(input_table_name: str, output_table_name: str):
    """
    Read data from Azure SQL Server and write back to the data store.
    """
    # check for active spark sessions
    if sc._active_spark_context is not None:
        spark = sc._active_spark_context.getOrCreate()
    else:
        spark = (
            SparkSession
            .builder
            .getOrCreate()
        )
    # initialize the logger
    log4jLogger = sc._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(__name__)
    logger.setLevel(log4jLogger.Level.INFO)

    # set the url for the jdbc connection
    logger.info(f'Connecting to Azure SQL Server: {host}:{port}')
    url = f'jdbc:sqlserver://{host}:{port};database={database}'

    # read tables from mssql
    try:
        mssql_df = (
            spark.read.format('jdbc')
            .options(
                url=url,
                driver=driver,
                dbtable=input_table_name,
                user=user,
                password=password,
            )
            .load()
        )
    except Exception as e:
        logger.error(f'Failed to read table {input_table_name}' + str(e))
        sys.exit(1)

    mssql_df.show()

    # +---------+-----------+-----------------+-------+---------+---------+
    # |CompanyId|CompnayName|NumberOfEmployees|FieldId|PackageId|ContactId|
    # +---------+-----------+-----------------+-------+---------+---------+
    # |        2| Mehdi Inc.|                1|      3|        2|        2|
    # +---------+-----------+-----------------+-------+---------+---------+

    # run the transformations stages on the dataframe
    transformed_df = main_transformations(spark, mssql_df)
    transformed_df.show()

    (
        transformed_df
        .write
        .format('jdbc')
        .options(
            url=url,
            driver=driver,
            dbtable=output_table_name,
            user=user,
            password=password,
        )
        .mode('overwrite')
        .save()
    )
    logger.info('New table created and written to MS SQL Server')

    # teardown
    spark.stop()


def main_transformations(spark: SparkSession, parsed_df: DataFrame) -> DataFrame:
    """
    Transform the dataframe and return the new dataframe.

    :param parsed_df: the input dataframe
    :return: the transformed dataframe
    """
    # populate the dataframe with the fake data
    refined_df = duplicate_rows(spark, parsed_df)

    # aggregate the dataframe
    aggergated_df = aggregate_rows(refined_df)

    # create a transaction log
    transactional_df = transaction_logs(aggergated_df)

    return transactional_df


def generate_fake_data_with_duplicates():
    """
    Generate fake data with duplicates
    """
    fake = Faker()
    fake_data = []

    for _ in range(10):
        fake_data.append(
            {
                'CompanyId': fake.random_int(min=1, max=100),
                'CompnayName': fake.company(),
                'NumberOfEmployees': fake.random_int(min=1, max=10000),
                'FieldId': fake.random_int(min=1, max=10000),
                'PackageId': fake.random_int(min=1, max=100000),
                'ContactId': fake.random_int(min=1, max=100000),
            },
        )

    return fake_data


def duplicate_rows(spark: SparkSession, input_df: DataFrame) -> DataFrame:
    """
    Create 20 dummy rows incrementing the CompanyID

    :param input_df: the input dataframe
    :return: the transformed dataframe
    """
    # create a new dataframe with 20 rows
    fake_data = generate_fake_data_with_duplicates()
    fake_df = spark.createDataFrame(fake_data, companies_schema)

    # union the two dataframes
    union_df = input_df.union(fake_df)

    return union_df


def transaction_logs(input_df: DataFrame):
    """
    Create a transaction log for the dataframe
    """
    silver_df = (
        input_df
        .withColumn('transaction_date', sf.current_timestamp())
    )

    return silver_df


def aggregate_rows(fake_df: DataFrame) -> DataFrame:
    """
    Aggregate the rows by CompanyId and sum the NumberOfEmployees
    """
    # group by the CompanyId and aggregate the rest of the columns
    aggergated_df = (
        fake_df
        .groupBy('CompanyId')
        .agg(
            sf.first('CompnayName').alias('CompnayName'),
            sf.first('NumberOfEmployees').alias('NumberOfEmployees'),
            sf.first('FieldId').alias('FieldId'),
            sf.first('PackageId').alias('PackageId'),
            sf.first('ContactId').alias('ContactId'),
        )
    )

    return aggergated_df


if __name__ == '__main__':
    # pylint: disable=value-for-parameter
    main(*sys.argv[1:])
