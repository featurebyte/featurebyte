"""
RequestColumn related classes for on-demand features
"""
from __future__ import annotations

from typing import Any, Optional

from pydantic import Field
from typeguard import typechecked

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

    @typechecked
    def preview_sql(self, limit: int = 10, **kwargs: Any) -> str:
        # preview_sql() is provided by QueryObject and requires feature_store. Ideally, it would be
        # better to move preview_sql() out of QueryObject so RequestColumn doesn't have this method.
        # But there are many tests that require Series to have preview_sql(), so the change is not
        # trivial. Raising NotImplementedError() specifically for RequestColumn for now.
        raise NotImplementedError("preview_sql is not supported for RequestColumn")


point_in_time = RequestColumn.create_request_column(
    SpecialColumnName.POINT_IN_TIME.value, DBVarType.TIMESTAMP
)
