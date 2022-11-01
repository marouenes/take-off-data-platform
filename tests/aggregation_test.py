"""
Unit test for the aggregate module
"""
from __future__ import annotations

import pyspark.sql.types as st
import pytest
from pyspark.sql import SparkSession

from reporting.aggregation import duplicate_rows
from testing.fixtures import spark  # noqa: F401

# pylint: disable=redefined-outer-name


@pytest.mark.skip(reason='not implemented yet')
@pytest.mark.parametrize(
    'input_data, expected_data', [
        (
            [
                (1, 'foo', 1, 1, 1),
                (2, 'bar', 2, 2, 2),
                (3, 'baz', 3, 3, 3),
            ],
            [
                (1, 'foo', 1, 1, 1),
                (2, 'bar', 2, 2, 2),
                (3, 'baz', 3, 3, 3),
            ],
        ),
        (
            [
                (1, 'foo', 1, 1, 1),
                (2, 'bar', 2, 2, 2),
                (3, 'baz', 3, 3, 3),
                (4, 'qux', 4, 4, 4),
            ],
            [
                (1, 'foo', 1, 1, 1),
                (2, 'bar', 2, 2, 2),
                (3, 'baz', 3, 3, 3),
                (4, 'qux', 4, 4, 4),
            ],
        ),
    ],
)
def test_duplicate_rows(
        spark: SparkSession,  # noqa: F811
        input_data: list[str],
        expected_data: list[str],
):
    """
    Test the duplicate_rows function

    :param spark: spark session
    :param input_data: input data
    :param expected_data: expected data
    """
    # create a dataframe
    schema = st.StructType([
        st.StructField('id', st.IntegerType(), True),
        st.StructField('name', st.StringType(), True),
        st.StructField('foo', st.IntegerType(), True),
        st.StructField('bar', st.IntegerType(), True),
        st.StructField('baz', st.IntegerType(), True),
    ])
    input_df = spark.createDataFrame(input_data, schema=schema)

    # transform X
    transformed_df = duplicate_rows(input_df)

    # collect the result
    result = transformed_df.collect()

    # compare the result
    assert result == expected_data
