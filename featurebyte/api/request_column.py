"""
RequestColumn related classes for on-demand features
"""
from __future__ import annotations

from typing import Optional

from pydantic import Field

from featurebyte.core.series import Series
from featurebyte.enum import DBVarType, SpecialColumnName
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.query_graph.model.common_table import TabularSource


class RequestColumn(Series):
    """
    RequestColumn class
    """

    tabular_source: Optional[TabularSource] = Field(  # type: ignore[assignment]
        allow_mutation=False, default=None
    )
    feature_store: Optional[FeatureStoreModel] = Field(  # type: ignore[assignment]
        exclude=True, allow_mutation=False, default=None
    )

    @classmethod
    def create_request_column(cls, column_name: str, column_dtype: DBVarType) -> RequestColumn:
        """
        Create a RequestColumn object.

        Parameters
        ----------
        column_name: str
            Column name in the request data.
        column_dtype: DBVarType
            Variable type of the column.

        Returns
        -------
        RequestColumn
        """
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

    @property
    def binary_op_output_class_priority(self) -> int:
        return 1


point_in_time = RequestColumn.create_request_column(
    SpecialColumnName.POINT_IN_TIME.value, DBVarType.TIMESTAMP
)
