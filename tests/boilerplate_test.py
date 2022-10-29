"""
unit test for the spark boilerplate code
"""
from __future__ import annotations

import pyspark.sql.types as st
import pytest
from pyspark.sql import DataFrame, SparkSession

from reporting.boilerplate import to_uppercase
from testing.fixtures import spark  # noqa: F401

# pylint: disable=redefined-outer-name


@pytest.mark.parametrize(
    'input_data, expected_data',
    [
        (
            [
                ('foo', 'bar'),
                ('baz', 'qux'),
            ],
            [
                ('FOO', 'BAR'),
                ('BAZ', 'QUX'),
            ],
        ),
        (
            [
                ('foo', 'bar'),
                ('baz', 'qux'),
                ('quux', 'quuz'),
            ],
            [
                ('FOO', 'BAR'),
                ('BAZ', 'QUX'),
                ('QUUX', 'QUUZ'),
            ],
        ),
    ],
)
def test_to_uppercase(
    spark: SparkSession,  # noqa: F811
    input_data: list[str],
    expected_data: list[str],
):
    """Test the to_uppercase function

    :param spark: spark session
    :param input_data: input data
    :param expected_data: expected data
    """
    # create a dataframe
    schema = st.StructType(
        [
            st.StructField('foo', st.StringType(), True),
            st.StructField('bar', st.StringType(), True),
        ],
    )
    df: DataFrame = spark.createDataFrame(input_data, schema)
    expected_df: DataFrame = spark.createDataFrame(expected_data, schema)

    # transform the dataframe
    actual_df = to_uppercase(df, ['foo', 'bar'])

    # gather the results
    rows = actual_df.collect()
    expected_rows = expected_df.collect()

    # assert the results
    for row_num, row in enumerate(rows):
        assert row == expected_rows[row_num]
