"""
Target API object
"""
from __future__ import annotations

from typing import List, Optional

from pydantic import Field, StrictStr
from typeguard import typechecked

from featurebyte.api.api_object_util import ForeignKeyMapping
from featurebyte.api.entity import Entity
from featurebyte.api.savable_api_object import SavableApiObject
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.target import TargetModel
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.schema.target import TargetUpdate


class Target(SavableApiObject):
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
