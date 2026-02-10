"""
Test atan2 module
"""

import pytest

from featurebyte.core.trigonometry import atan2
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeType
from tests.util.helper import get_node


def test_atan2(int_series):
    """
    Test atan2
    """
    atan2_series = atan2(int_series, int_series)
    assert atan2_series.dtype == DBVarType.FLOAT
    series_dict = atan2_series.model_dump()
    assert series_dict["node_name"] == "atan2_1"
    atan2_node = get_node(series_dict["graph"], "atan2_1")
    assert atan2_node == {
        "name": "atan2_1",
        "output_type": "series",
        "parameters": {},
        "type": NodeType.ATAN2,
    }


def test_atan2_non_numeric(varchar_series):
    """
    Test atan2 raises error for non-numeric series
    """
    with pytest.raises(ValueError, match="Expected all series to be numeric"):
        atan2(varchar_series, varchar_series)
