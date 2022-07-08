"""
Entity class
"""
from __future__ import annotations

from http import HTTPStatus

from featurebyte.api.util import convert_response_to_dict
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
        super().__init__(**convert_response_to_dict(response))

    @property
    def serving_name(self) -> str:
        """
        Serving name

        Returns
        -------
        str
        """
        return self.serving_names[0]

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
        super().__init__(**convert_response_to_dict(response))
