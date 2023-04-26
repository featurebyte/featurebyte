"""
RequestColumn related classes for on-demand features
"""
from __future__ import annotations

from featurebyte.core.series import Series
from featurebyte.enum import DBVarType, SpecialColumnName
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph


class RequestColumn(Series):
    """
    RequestColumn class
    """

    @classmethod
    def create_request_column(cls, column_name: str, column_dtype: DBVarType) -> RequestColumn:
        node = GlobalQueryGraph().add_operation(
            node_type=NodeType.REQUEST_COLUMN,
            node_params={"column_name": column_name, "dtype": column_dtype},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[],
        )
        return cls(
            feature_store=None,
            tabular_source=None,
            node_name=node.name,
            name=column_name,
            dtype=column_dtype,
        )


point_in_time = RequestColumn.create_request_column(
    SpecialColumnName.POINT_IN_TIME.value, DBVarType.TIMESTAMP
)
