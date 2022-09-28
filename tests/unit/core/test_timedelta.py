"""
Unit tests for featurebyte.core.timedelta
"""
import pytest

from featurebyte.core.timedelta import to_timedelta
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeType
from tests.util.helper import get_node


@pytest.mark.parametrize(
    "unit",
    [
        "day",
        "hour",
        "minute",
        "second",
        "millisecond",
        "microsecond",
    ],
)
def test_to_timedelta(int_series, unit):
    """Test to_timedelta() can construct a timedelta Series"""
    timedelta_series = to_timedelta(int_series, unit=unit)
    assert timedelta_series.dtype == DBVarType.TIMEDELTA
    series_dict = timedelta_series.dict()
    assert series_dict["node_name"] == "timedelta_1"
    timedelta_node = get_node(series_dict["graph"], "timedelta_1")
    assert timedelta_node == {
        "name": "timedelta_1",
        "output_type": "series",
        "parameters": {"unit": unit},
        "type": NodeType.TIMEDELTA,
    }


def test_to_timedelta__unsupported_unit(int_series):
    """Test to_timedelta() with a non-supported time unit"""
    with pytest.raises(TypeError) as exc:
        _ = to_timedelta(int_series, unit="month")
    assert 'the value of argument "unit" must be one of' in str(exc.value)


def test_to_timedelta__not_int(float_series):
    """Test to_timedelta() rejects non-INT Series"""
    with pytest.raises(ValueError) as exc:
        _ = to_timedelta(float_series, unit="second")
    assert str(exc.value) == "to_timedelta only supports INT type series; got FLOAT"
