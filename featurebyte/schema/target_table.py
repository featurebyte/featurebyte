"""
TargetTable API payload schema
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import StrictStr, root_validator

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
    graph: QueryGraph
    node_names: List[StrictStr]
    request_input: ObservationInput
    context_id: Optional[PydanticObjectId]

    @property
    def nodes(self) -> List[Node]:
        """
        Get target nodes

        Returns
        -------
        List[Node]
        """
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
