"""
TargetTable API payload schema
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import Field, StrictStr, root_validator

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.observation_table import ObservationInput
from featurebyte.models.target_table import TargetTableModel
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node import Node
from featurebyte.schema.common.base import PaginationMixin
from featurebyte.schema.common.feature_or_target import FeatureOrTargetTableCreate
from featurebyte.schema.materialized_table import BaseMaterializedTableListRecord


class TargetTableCreate(FeatureOrTargetTableCreate):
    """
    TargetTable creation payload
    """

    serving_names_mapping: Optional[Dict[str, str]]
    target_id: Optional[PydanticObjectId]
    graph: Optional[QueryGraph] = Field(default=None)
    # Note that even though node_names is a list, we typically only expect one node. We can't change this to a non-list
    # for backwards compatibility reasons.
    node_names: Optional[List[StrictStr]] = Field(default=None)
    request_input: ObservationInput
    context_id: Optional[PydanticObjectId]
    skip_entity_validation_checks: bool = Field(default=False)

    @root_validator(pre=True)
    @classmethod
    def _check_graph_and_node_names(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        graph = values.get("graph", None)
        node_names = values.get("node_names", None)
        target_id = values.get("target_id", None)
        both_are_none = graph is None and node_names is None

        # Check if target is provided
        if target_id is not None:
            # Valid if target_id is provided, and no graph and node_names are provided
            if both_are_none:
                return values
            raise ValueError(
                "If target_id is provided, graph and node_names should not be provided."
            )

        # If target is not provided, graph and node_names should be provided.
        both_are_not_none = graph is not None and node_names is not None
        if both_are_not_none:
            return values
        raise ValueError(
            "Both graph and node_names should be provided, or neither should be provided."
        )

    @property
    def nodes(self) -> List[Node]:
        """
        Get target nodes

        Returns
        -------
        List[Node]
        """
        if self.graph is None or self.node_names is None:
            return []
        return [self.graph.get_node_by_name(name) for name in self.node_names]


class TargetTableList(PaginationMixin):
    """
    Schema for listing targe tables
    """

    data: List[TargetTableModel]


class TargetTableListRecord(BaseMaterializedTableListRecord):
    """
    Schema for listing target tables as a DataFrame
    """

    feature_store_id: PydanticObjectId
    observation_table_id: PydanticObjectId

    @root_validator(pre=True)
    @classmethod
    def _extract(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        values["feature_store_id"] = values["location"]["feature_store_id"]
        return values
