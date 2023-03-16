"""
Entity class
"""
from __future__ import annotations

from typing import Any

from http import HTTPStatus

from typeguard import typechecked

from featurebyte.api.api_object import SavableApiObject
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.config import Configurations
from featurebyte.exception import RecordUpdateException
from featurebyte.models.entity import EntityModel, ParentEntity
from featurebyte.schema.entity import EntityCreate, EntityUpdate


class Entity(EntityModel, SavableApiObject):
    """
    Entity class
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["Entity"])

    # class variables
    _route = "/entity"
    _update_schema_class = EntityUpdate
    _list_schema = EntityModel
    _get_schema = EntityModel
    _list_fields = ["name", "serving_names", "created_at"]

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
        self.update(update_payload={"name": name}, allow_update_local=True)

    @property
    def name_history(self) -> list[dict[str, Any]]:
        """
        List of name history entries

        Returns
        -------
        list[dict[str, Any]]
        """
        return self._get_audit_history(field_name="name")

    @typechecked
    def add_parent(self, parent_entity_name: str, relation_dataset_name: str) -> None:
        """
        Adds other entity as the parent of this current entity.

        Parameters
        ----------
        parent_entity_name: str
            the entity that will become the parent of this entity.
        relation_dataset_name: str
            the name of the dataset that the parent is from

        Raises
        ------
        RecordUpdateException
            error updating record
        """

        client = Configurations().get_client()
        response = client.get("/table", params={"name": relation_dataset_name})
        assert response.status_code == HTTPStatus.OK
        json_response = response.json()
        data_response = json_response["data"]
        assert len(data_response) == 1

        parent_entity = Entity.get(parent_entity_name)
        data = ParentEntity(
            data_type=data_response[0]["type"],
            data_id=data_response[0]["_id"],
            id=parent_entity.id,
        )

        post_response = client.post(
            f"{self._route}/{self.id}/parent",
            json=data.json_dict(),
        )
        if post_response.status_code != HTTPStatus.CREATED:
            raise RecordUpdateException(post_response)

    @typechecked
    def remove_parent(self, parent_entity_name: str) -> None:
        """
        Removes other entity as the parent of this current entity.

        Parameters
        ----------
        parent_entity_name: str
            the other entity that we want to remove as a parent.

        Raises
        ------
        RecordUpdateException
            error updating record
        """

        client = Configurations().get_client()
        parent_entity = Entity.get(parent_entity_name)
        post_response = client.delete(
            f"{self._route}/{self.id}/parent/{parent_entity.id}",
        )
        if post_response.status_code != HTTPStatus.OK:
            raise RecordUpdateException(post_response)
