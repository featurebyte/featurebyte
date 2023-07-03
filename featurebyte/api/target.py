"""
Target API object
"""
from __future__ import annotations

from typing import List, Optional

from http import HTTPStatus

import pandas as pd
from pydantic import Field, StrictStr
from typeguard import typechecked

from featurebyte.api.api_object_util import ForeignKeyMapping
from featurebyte.api.entity import Entity
from featurebyte.api.savable_api_object import SavableApiObject
from featurebyte.common.utils import dataframe_from_json, enforce_observation_set_row_order
from featurebyte.config import Configurations
from featurebyte.core.series import Series
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.target import TargetModel
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node import Node
from featurebyte.schema.target import TargetPreview, TargetUpdate


class Target(Series, SavableApiObject):
    """
    Target class used to represent a Target in FeatureByte.
    """

    internal_entity_ids: Optional[List[PydanticObjectId]] = Field(alias="entity_ids")
    internal_horizon: Optional[StrictStr] = Field(alias="horizon")
    internal_graph: Optional[QueryGraph] = Field(allow_mutation=False, alias="graph")
    internal_node_name: Optional[str] = Field(allow_mutation=False, alias="node_name")

    _route = "/target"
    _update_schema_class = TargetUpdate

    _list_schema = TargetModel
    _get_schema = TargetModel
    _list_fields = ["name", "entities"]
    _list_foreign_keys = [
        ForeignKeyMapping("entity_ids", Entity, "entities"),
    ]

    @property
    def entities(self) -> List[Entity]:
        """
        Returns a list of entities associated with this target.

        Returns
        -------
        List[Entity]
        """
        try:
            entity_ids = self.cached_model.entity_ids  # type: ignore[attr-defined]
        except RecordRetrievalException:
            entity_ids = self.internal_entity_ids
        return [Entity.get_by_id(entity_id) for entity_id in entity_ids]

    @property
    def horizon(self) -> Optional[str]:
        """
        Returns the horizon of this target.

        Returns
        -------
        Optional[str]
        """
        try:
            return self.cached_model.horizon
        except RecordRetrievalException:
            return self.internal_horizon

    @property
    def graph(self) -> Optional[QueryGraph]:
        """
        Returns the horizon of this target.

        Returns
        -------
        Optional[str]
        """
        try:
            return self.cached_model.graph
        except RecordRetrievalException:
            return self.internal_graph

    @property
    def node(self) -> Node:
        """
        Representative of the current object in the graph

        Returns
        -------
        Node
        """
        try:
            node_to_return = self.cached_model.node
        except RecordRetrievalException:
            node_name = self.internal_node_name
            node_to_return = self.graph.get_node_by_name(node_name)
        return node_to_return

    @classmethod
    @typechecked
    def create(
        cls,
        name: str,
        entities: Optional[List[str]] = None,
        horizon: Optional[str] = None,
    ) -> Target:
        """
        Create a new Target.

        Parameters
        ----------
        name : str
            Name of the Target
        entities : Optional[List[str]]
            List of entity names, by default None
        horizon : Optional[str]
            Horizon of the Target, by default None

        Returns
        -------
        Target
            The newly created Target
        """
        entity_ids = None
        if entities:
            entity_ids = [Entity.get(entity_name).id for entity_name in entities]
        target = Target(
            name=name,
            entity_ids=entity_ids,
            horizon=horizon,
        )
        target.save()
        return target

    def _get_pruned_target_model(self) -> TargetModel:
        """
        Get pruned model of target

        Returns
        -------
        FeatureModel
        """
        pruned_graph, mapped_node = self.extract_pruned_graph_and_node()
        target_dict = self.dict(by_alias=True)
        target_dict["graph"] = pruned_graph.dict()
        target_dict["node_name"] = mapped_node.name
        return TargetModel(**target_dict)

    @enforce_observation_set_row_order
    @typechecked
    def preview(
        self,
        observation_set: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Materializes a Target object using a small observation set of up to 50 rows.

        The small observation set should combine historical points-in-time and key values of the primary entity from
        the target. Associated serving entities can also be utilized.

        Parameters
        ----------
        observation_set : pd.DataFrame
            Observation set DataFrame which combines historical points-in-time and values of the target primary entity
            or its descendant (serving entities). The column containing the point-in-time values should be named
            `POINT_IN_TIME`, while the columns representing entity values should be named using accepted serving
            names for the entity.

        Returns
        -------
        pd.DataFrame
            Materialized target values.
            The returned DataFrame will have the same number of rows, and include all columns from the observation set.

            **Note**: `POINT_IN_TIME` values will be converted to UTC time.

        Raises
        ------
        RecordRetrievalException
            Failed to materialize feature preview.
        """
        target = self._get_pruned_target_model()
        graph = target.graph
        node_name = target.node_name
        assert graph is not None
        assert node_name is not None
        payload = TargetPreview(
            feature_store_name=self.feature_store.name,
            graph=graph,
            node_name=node_name,
            point_in_time_and_serving_name_list=observation_set.to_dict(orient="records"),
        )

        client = Configurations().get_client()
        response = client.post(url="/target/preview", json=payload.json_dict())
        if response.status_code != HTTPStatus.OK:
            raise RecordRetrievalException(response)
        result = response.json()

        return dataframe_from_json(result)  # pylint: disable=no-member
