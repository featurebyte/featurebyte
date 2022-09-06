"""
Unit tests for featurebyte.core.timedelta
"""
import pytest

from featurebyte.core.timedelta import to_timedelta
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeType


def test_to_timedelta(int_series):
    """Test to_timedelta() can construct a timedelta Series"""
    timedelta_series = to_timedelta(int_series, unit="second")
    assert timedelta_series.dtype == DBVarType.TIMEDELTA
    series_dict = timedelta_series.dict()
    assert series_dict["node"] == {
        "name": "timedelta_1",
        "output_type": "series",
        "parameters": {"unit": "second"},
        "type": NodeType.TIMEDELTA,
    }


def test_to_timedelta__not_int(float_series):
    """Test to_timedelta() rejects non-INT Series"""
    with pytest.raises(ValueError) as exc:
        _ = to_timedelta(float_series, unit="second")
    assert str(exc.value) == "to_timedelta only supports INT type series; got FLOAT"
