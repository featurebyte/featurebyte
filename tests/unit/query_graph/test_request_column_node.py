"""Test request column node related functionality."""

from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType
from featurebyte.query_graph.model.dtype import DBVarTypeInfo
from featurebyte.query_graph.node.request import RequestColumnNode


def test_request_column_node_backward_compatibility():
    """
    Test backward compatibility of RequestColumnNode
    """
    node = RequestColumnNode(
        name="request_column_node",
        output_type=NodeOutputType.SERIES,
        parameters={
            "column_name": "request_column",
            # old format does not have dtype_info, but we can still use it
            "dtype": DBVarType.VARCHAR,
        },
    )
    # check that old way of specifying dtype works
    assert node.parameters.dtype == DBVarType.VARCHAR

    # check that dtype_info is set correctly
    assert node.parameters.dtype_info == DBVarTypeInfo(dtype=DBVarType.VARCHAR)
