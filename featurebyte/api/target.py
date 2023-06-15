"""
Target API object
"""
from __future__ import annotations

from typing import List, Optional

from pydantic import Field, StrictStr

from featurebyte.api.api_object import ForeignKeyMapping
from featurebyte.api.entity import Entity
from featurebyte.api.savable_api_object import SavableApiObject
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.target import TargetModel
from featurebyte.schema.target import TargetUpdate


class Target(SavableApiObject):
    """
    Target class used to represent a Target in FeatureByte.
    """

    internal_entity_ids: Optional[List[PydanticObjectId]] = Field(alias="entity_ids")
    horizon: Optional[StrictStr]
    blind_spot: Optional[StrictStr]

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

    @classmethod
    def create(
        cls,
        name: str,
        entities: Optional[List[str]] = None,
        horizon: Optional[str] = None,
        blind_spot: Optional[str] = None,
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
        blind_spot : Optional[str]
            Blind spot of the Target, by default None

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
            blind_spot=blind_spot,
        )
        target.save()
        return target
