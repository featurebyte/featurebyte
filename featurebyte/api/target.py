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
        if not self.internal_entity_ids:
            return []
        return [Entity.get_by_id(entity_id) for entity_id in self.internal_entity_ids]

    def __init__(
        self, name: str, entities: Optional[List[str]], horizon: str, blind_spot: str, **kwargs: Any
    ):
        internal_kwargs = kwargs.copy()
        entity_ids = None
        if "entity_ids" in internal_kwargs:
            entity_ids = internal_kwargs.pop("entity_ids")
        if entities:
            entity_ids = [Entity.get(entity_name).id for entity_name in entities]
        super().__init__(
            name=name,
            entity_ids=entity_ids,
            horizon=horizon,
            blind_spot=blind_spot,
            **internal_kwargs,
        )

    def _get_init_params_from_object(self) -> dict[str, Any]:
        entity_names = None
        if self.internal_entity_ids:
            entity_names = [
                Entity.get_by_id(entity_id).name for entity_id in self.internal_entity_ids
            ]
        return {"entities": entity_names}

    @classmethod
    def _get_init_params(cls) -> dict[str, Any]:
        return {"entities": None}
