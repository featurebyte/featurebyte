"""
Common test fixtures used across test files in core directory
"""
import pytest

from featurebyte.core.series import Series
from featurebyte.enum import DBVarType


@pytest.fixture()
def bool_series(dataframe):
    """
    Series with boolean var type
    """
    series = dataframe["MASK"]
    assert isinstance(series, Series)
    assert series.name == "MASK"
    assert series.var_type == DBVarType.BOOL
    yield series


@pytest.fixture()
def int_series(dataframe):
    """
    Series with integer var type
    """
    series = dataframe["CUST_ID"]
    assert isinstance(series, Series)
    assert series.name == "CUST_ID"
    assert series.var_type == DBVarType.INT
    yield series


@pytest.fixture()
def float_series(dataframe):
    """
    Series with float var type
    """
    series = dataframe["VALUE"]
    assert isinstance(series, Series)
    assert series.name == "VALUE"
    assert series.var_type == DBVarType.FLOAT
    yield series


@pytest.fixture()
def varchar_series(dataframe):
    """
    Series with string var type
    """
    series = dataframe["PRODUCT_ACTION"]
    assert isinstance(series, Series)
    assert series.name == "PRODUCT_ACTION"
    assert series.var_type == DBVarType.VARCHAR
    yield series
