"""
Entity class
"""
from __future__ import annotations

from http import HTTPStatus

from featurebyte.config import Configurations
from featurebyte.exception import DuplicatedRecordException, RecordCreationException
from featurebyte.models.entity import EntityModel


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
        client = Configurations().get_client()
        payload = {"name": name, "serving_names": [serving_name]}
        response = client.post("/entity", json=payload)
        if response.status_code != HTTPStatus.CREATED:
            if response.status_code == HTTPStatus.CONFLICT:
                raise DuplicatedRecordException(response=response)
            raise RecordCreationException(response=response)
        super().__init__(name=name, serving_names=[serving_name])

    @property
    def serving_name(self) -> str:
        """
        Serving name

        Returns
        -------
        str
        """
        return self.serving_names[0]
