"""
Entity class
"""
from __future__ import annotations

from typing import Any

from http import HTTPStatus

from typeguard import typechecked

from featurebyte.api.api_object import ApiObject
from featurebyte.config import Configurations
from featurebyte.exception import (
    DuplicatedRecordException,
    RecordRetrievalException,
    RecordUpdateException,
)
from featurebyte.models.entity import EntityModel
from featurebyte.schema.entity import EntityCreate, EntityUpdate


class Entity(EntityModel, ApiObject):
    """
    Entity class
    """

    # class variables
    _route = "/entity"

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

        Raises
        ------
        DuplicatedRecordException
            When there exists entity with the same name or serving name
        RecordUpdateException
            When exception happens during record update at persistent
        """
        data = EntityUpdate(name=name)
        client = Configurations().get_client()
        response = client.patch(f"/entity/{self.id}", json=data.json_dict())
        if response.status_code == HTTPStatus.NOT_FOUND:
            # entity not saved, update local object
            self.name = name
        else:
            if response.status_code != HTTPStatus.OK:
                if response.status_code == HTTPStatus.CONFLICT:
                    raise DuplicatedRecordException(response=response)
                raise RecordUpdateException(response=response)
            super().__init__(**response.json(), saved=True)

    @property
    def name_history(self) -> list[dict[str, Any]]:
        """
        List of name history entries

        Returns
        -------
        list[dict[str, Any]]

        Raises
        ------
        RecordRetrievalException
            When unexpected retrieval failure
        """
        client = Configurations().get_client()
        response = client.get(url=f"/entity/history/name/{self.id}")
        if response.status_code == HTTPStatus.OK:
            history: list[dict[str, Any]] = response.json()
            return history
        raise RecordRetrievalException(response)
