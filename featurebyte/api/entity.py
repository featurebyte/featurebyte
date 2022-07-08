"""
Entity class
"""
from __future__ import annotations

from typing import Any

from http import HTTPStatus

from requests.models import Response

from featurebyte.config import Configurations
from featurebyte.exception import (
    DuplicatedRecordException,
    RecordCreationException,
    RecordUpdateException,
)
from featurebyte.models.entity import EntityModel
from featurebyte.schema.entity import EntityCreate, EntityUpdate


class Entity(EntityModel):
    """
    Entity class
    """

    def __init__(self, name: str, serving_name: str):
        """
        Entity constructor

        Parameters
        ----------
        name: str
            Entity name
        serving_name: str
            Entity serving name

        Raises
        ------
        DuplicatedRecordException
            When there exists entity with the same name or serving name
        RecordCreationException
            When exception happens during record creation at persistent
        """
        data = EntityCreate(name=name, serving_name=serving_name)
        client = Configurations().get_client()
        response = client.post("/entity", json=data.dict())
        if response.status_code != HTTPStatus.CREATED:
            if response.status_code == HTTPStatus.CONFLICT:
                raise DuplicatedRecordException(response=response)
            raise RecordCreationException(response=response)
        super().__init__(**self._response_to_dict(response))

    @property
    def serving_name(self) -> str:
        """
        Serving name

        Returns
        -------
        str
        """
        return self.serving_names[0]

    @staticmethod
    def _response_to_dict(response: Response) -> dict[str, Any]:
        """
        Convert API response to dictionary format

        Parameters
        ----------
        response: Response
            API response object

        Returns
        -------
        dict[str, Any]
        """
        response_dict: dict[str, Any] = response.json()
        response_dict["_id"] = response_dict.pop("id")
        return response_dict

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
        response = client.patch(f"/entity/{self.id}", json=data.dict())
        if response.status_code != HTTPStatus.OK:
            if response.status_code == HTTPStatus.CONFLICT:
                raise DuplicatedRecordException(response=response)
            raise RecordUpdateException(response=response)
        super().__init__(**self._response_to_dict(response))
