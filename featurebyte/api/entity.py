"""
Entity class
"""
from __future__ import annotations

from http import HTTPStatus

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

    def save(self) -> None:
        """
        save entity at persistent layer

        Raises
        ------
        DuplicatedRecordException
            When there exists entity with the same name or serving name
        RecordCreationException
            When exception happens during record creation at persistent
        """
        data = EntityCreate(name=self.name, serving_name=self.serving_names[0])
        client = Configurations().get_client()
        response = client.post("/entity", json=data.json_dict())
        if response.status_code != HTTPStatus.CREATED:
            if response.status_code == HTTPStatus.CONFLICT:
                raise DuplicatedRecordException(response=response)
            raise RecordCreationException(response=response)
        type(self).__init__(self, **response.json())

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
        response = client.patch(f"/entity/{self.id}", json=data.json_dict())
        if response.status_code != HTTPStatus.OK:
            if response.status_code == HTTPStatus.CONFLICT:
                raise DuplicatedRecordException(response=response)
            raise RecordUpdateException(response=response)
        super().__init__(**response.json())
