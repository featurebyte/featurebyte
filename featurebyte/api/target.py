"""
Target API object
"""
from typing import Any, List, Optional

from pydantic import Field

from featurebyte import Entity
from featurebyte.api.api_object import ForeignKeyMapping
from featurebyte.api.savable_api_object import SavableApiObject
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.target import TargetModel
from featurebyte.schema.target import TargetUpdate


class Target(SavableApiObject):
    """
    Target class used to represent a Target in FeatureByte.
    """

    # Fields that a Target can be created with
    internal_entity_ids: Optional[List[PydanticObjectId]] = Field(alias="entity_ids")
    horizon: str
    blind_spot: str

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
        return [Entity.get_by_id(entity_id) for entity_id in self.internal_entity_ids]

    def __init__(
        self, name: str, entities: Optional[List[str]], horizon: str, blind_spot: str, **kwargs: Any
    ):
        entity_ids = [Entity.get(entity_name).id for entity_name in entities]
        super().__init__(
            name=name, entity_ids=entity_ids, horizon=horizon, blind_spot=blind_spot, **kwargs
        )
