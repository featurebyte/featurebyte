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

    @classmethod
    def create(cls, name: str, serving_name: str) -> Entity:
        """
        Create entity at persistent layer

        Parameters
        ----------
        name: str
            Entity name
        serving_name: str
            Entity serving name

        Returns
        -------
        Entity
            Newly created entity object

        Raises
        ------
        DuplicatedRecordException
            When there exists entity with the same name or serving name
        RecordCreationException
            When exception happens during record creation at persistent
        """
        data = EntityCreate(name=name, serving_name=serving_name)
        client = Configurations().get_client()
        response = client.post("/entity", json=data.json_dict())
        if response.status_code != HTTPStatus.CREATED:
            if response.status_code == HTTPStatus.CONFLICT:
                raise DuplicatedRecordException(response=response)
            raise RecordCreationException(response=response)
        return Entity(**response.json())

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
