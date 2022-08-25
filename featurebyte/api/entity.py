"""
Entity class
"""
from __future__ import annotations

from typing import Any

from typeguard import typechecked

from featurebyte.api.api_object import ApiObject
from featurebyte.models.entity import EntityModel
from featurebyte.schema.entity import EntityCreate, EntityUpdate


class Entity(EntityModel, ApiObject):
    """
    Entity class
    """

    # class variables
    _route = "/entity"
    _update_schema_class = EntityUpdate

    def _get_create_payload(self) -> dict[str, Any]:
        data = EntityCreate(serving_name=self.serving_names[0], **self.json_dict())
        return data.json_dict()

    @property
    def serving_name(self) -> str:
        """
        Serving name

        Returns
        -------
        str
        """
        return self.serving_names[0]

    @typechecked
    def update_name(self, name: str) -> None:
        """
        Change entity name

        Parameters
        ----------
        name: str
            New entity name
        """
        self.update({"name": name})

    @property
    def name_history(self) -> list[dict[str, Any]]:
        """
        List of name history entries

        Returns
        -------
        list[dict[str, Any]]
        """
        return self._get_audit_history(field_name="name")
