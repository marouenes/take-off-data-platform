"""
Set of tests for databricks helper module.
"""
from __future__ import annotations

import pytest

from utils.databricks import (
    _check_is_databricks,
    _get_spark,
    _display,
    _display_with_json
)


@pytest.mark.skip("This test is not implemented yet")
def test_check_is_databricks():
    """
    Tests _check_is_databricks by setting up inputs and
    checking if the function returns the expected output.
    """
    # variables for the test
    user_ns = {
        "displayHTML": "test"
    }

    # Run function and extract conf of resulting conf object
    is_databricks = _check_is_databricks(user_ns)

    # assert that there were no mismatches.
    assert is_databricks == True


@pytest.mark.skip("This test is not implemented yet")
def test_get_spark():
    """
    Tests _get_spark by setting up inputs and
    checking if the function returns the expected output.
    """
    # variables for the test
    user_ns = {
        "spark": "test"
    }

    # Run function and extract conf of resulting conf object
    spark = _get_spark(user_ns)

    # assert that there were no mismatches.
    assert spark == "test"


@pytest.mark.skip("This test is not implemented yet")
def test_display():
    """
    Tests _display by setting up inputs and
    checking if the function returns the expected output.
    """
    # variables for the test
    df = "test"

    # Run function and extract conf of resulting conf object
    display = _display(df)

    # assert that there were no mismatches.
    assert display == None


@pytest.mark.skip("This test is not implemented yet")
def test_display_with_json():
    """
    Tests _display_with_json by setting up inputs and
    checking if the function returns the expected output.
    """
    # variables for the test
    df = "test"

    # Run function and extract conf of resulting conf object
    display = _display_with_json(df)

    # assert that there were no mismatches.
    assert display == None
