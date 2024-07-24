"""
Test haversine module
"""

from featurebyte.core.distance import haversine
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeType
from tests.util.helper import get_node


def test_to_haversine(int_series):
    """
    Test to haversine
    """
    haversine_series = haversine(int_series, int_series, int_series, int_series)
    assert haversine_series.dtype == DBVarType.FLOAT
    series_dict = haversine_series.model_dump()
    assert series_dict["node_name"] == "haversine_1"
    timedelta_node = get_node(series_dict["graph"], "haversine_1")
    assert timedelta_node == {
        "name": "haversine_1",
        "output_type": "series",
        "parameters": {},
        "type": NodeType.HAVERSINE,
    }
