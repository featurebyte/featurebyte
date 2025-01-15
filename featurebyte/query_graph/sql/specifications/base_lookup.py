"""
Base lookup spec
"""

from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import Any, Optional

from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import (
    EventLookupParameters,
    LookupNode,
    LookupTargetNode,
    SCDLookupParameters,
)
from featurebyte.query_graph.sql.specs import NonTileBasedAggregationSpec


@dataclass
class BaseLookupSpec(NonTileBasedAggregationSpec, ABC):
    """
    BaseLookupSpec contains the basic information required to generate sql for a lookup feature or target.
    """

    input_column_name: str
    entity_column: str
    serving_names: list[str]
    scd_parameters: Optional[SCDLookupParameters]
    event_parameters: Optional[EventLookupParameters]
    is_parent_lookup: bool = False

    @property
    def agg_result_name(self) -> str:
        if self.is_parent_lookup:
            return self.feature_name
        args = self._get_additional_agg_result_name_params()
        return self.construct_agg_result_name(self.input_column_name, *args)

    def _get_additional_agg_result_name_params(self) -> list[Any]:
        args = []
        if self.scd_parameters is not None and self.scd_parameters.offset is not None:
            args.append(self.scd_parameters.offset)
        return args

    def get_source_hash_parameters(self) -> dict[str, Any]:
        params: dict[str, Any] = {
            "source_expr": self.source_expr.sql(),
            "entity_column": self.entity_column,
        }
        if self.scd_parameters is not None:
            params["scd_parameters"] = self.scd_parameters.model_dump()
            if params["scd_parameters"].get("end_timestamp_metadata") is None:
                params["scd_parameters"].pop("end_timestamp_metadata", None)
        if self.event_parameters is not None:
            params["event_parameters"] = self.event_parameters.model_dump()
            if params["event_parameters"].get("event_timestamp_metadata") is None:
                params["event_parameters"].pop("event_timestamp_metadata", None)
        return params

    @classmethod
    def should_filter_scd_by_current_flag(cls, graph: QueryGraphModel, node: Node) -> bool:
        assert isinstance(node, (LookupNode, LookupTargetNode))
        scd_parameters = node.parameters.scd_parameters
        if scd_parameters is not None:
            return cls.get_scd_filter_flag_from_scd_parameters(scd_parameters)
        return False

    @staticmethod
    def get_scd_filter_flag_from_scd_parameters(scd_parameters: SCDLookupParameters) -> bool:
        """
        Returns whether the current flag should be used to filter SCD join

        Parameters
        ----------
        scd_parameters: SCDLookupParameters
            SCD related parameters in the lookup node

        Returns
        -------
        bool
        """
        # Online serving might not have to use SCD join if current flag is applicable
        current_flag_usable_for_online_serving = (
            scd_parameters.current_flag_column is not None and scd_parameters.offset is None
        )
        return current_flag_usable_for_online_serving
