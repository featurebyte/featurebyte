"""Test request column node related functionality."""

import pytest

from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType
from featurebyte.query_graph.model.dtype import DBVarTypeInfo
from featurebyte.query_graph.node.request import RequestColumnNode


@pytest.mark.parametrize(
    "dtype_data",
    [
        # old graph
        {"dtype": DBVarType.VARCHAR},
        # new graph that breaks old client
        {"dtype_info": DBVarTypeInfo(dtype=DBVarType.VARCHAR)},
        {"dtype_info": {"dtype": DBVarType.VARCHAR, "metadata": None}},
        {
            # new graph that does not break old client
            "dtype_info": DBVarTypeInfo(dtype=DBVarType.VARCHAR, metadata=None),
            "dtype": DBVarType.VARCHAR,
        },
    ],
)
def test_request_column_node_backward_compatibility(dtype_data):
    """
    Test backward compatibility of RequestColumnNode
    """
    node_dict = {
        "name": "request_column_node",
        "output_type": NodeOutputType.SERIES,
        "parameters": {
            "column_name": "request_column",
        },
    }
    node_dict["parameters"].update(dtype_data)
    node = RequestColumnNode(**node_dict)

    # check that old way of specifying dtype works
    assert node.parameters.dtype == DBVarType.VARCHAR

    # check that dtype_info is set correctly
    assert node.parameters.dtype_info == DBVarTypeInfo(dtype=DBVarType.VARCHAR)
