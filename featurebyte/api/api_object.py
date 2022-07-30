"""
APIObjectMixin class
"""
from __future__ import annotations

from typing import Any

from http import HTTPStatus

from featurebyte.config import Configurations
from featurebyte.core.generic import ExtendedFeatureStoreModel
from featurebyte.exception import (
    DuplicatedRecordException,
    RecordCreationException,
    RecordRetrievalException,
)
from featurebyte.models.base import FeatureByteBaseModel


class ApiObject(FeatureByteBaseModel):
    """
    APIObjectMixin contains common methods used to interact with API routes
    """

    # class variables
    _route = ""

    @classmethod
    def _get_object_name(cls, class_name: str) -> str:
        """
        Convert camel case class name to snake case object name

        Parameters
        ----------
        class_name: str
            Class name

        Returns
        -------
        str
        """
        object_name = "".join(
            "_" + char.lower() if char.isupper() else char for char in class_name
        ).lstrip("_")
        return object_name

    def _get_init_params(self) -> dict[str, Any]:
        """
        Additional parameters pass to constructor (other than those parameters from response)

        Returns
        -------
        dict[str, Any]
        """
        return {}

    def _get_create_payload(self) -> dict[str, Any]:
        """
        Construct payload used for post route

        Returns
        -------
        dict[str, Any]
        """
        return self.json_dict()

    @classmethod
    def get(cls, name: str) -> ApiObject:
        """
        Retrieve object dictionary from the persistent given object name

        Parameters
        ----------
        name: str
            Object name

        Returns
        -------
        ApiObject
            APIObject object of the given event data name

        Raises
        ------
        RecordRetrievalException
            When the object not found
        """
        client = Configurations().get_client()
        response = client.get(url=cls._route, params={"name": name})
        if response.status_code == HTTPStatus.OK:
            response_dict = response.json()
            if response_dict["data"]:
                object_dict = response_dict["data"][0]
                tabular_source = object_dict.get("tabular_source")
                if tabular_source:
                    feature_store_id, _ = tabular_source
                    feature_store_response = client.get(url=f"/feature_store/{feature_store_id}")
                    if feature_store_response.status_code == HTTPStatus.OK:
                        feature_store = ExtendedFeatureStoreModel(**feature_store_response.json())
                        return cls(**object_dict, feature_store=feature_store)
                    raise RecordRetrievalException(
                        response,
                        f'FeatureStore (feature_store.id: "{feature_store_id}") not found!',
                    )
                return cls(**object_dict)

            class_name = cls.__name__
            object_name = cls._get_object_name(class_name)
            raise RecordRetrievalException(
                response, f'{class_name} ({object_name}.name: "{name}") not found!'
            )
        raise RecordRetrievalException(response, f"Failed to retrieve object!")

    @classmethod
    def list(cls) -> list[str]:
        """
        List the object name store at the persistent

        Returns
        -------
        list[str]
            List of object name
        """
        client = Configurations().get_client()
        response = client.get(url=cls._route)
        if response.status_code == HTTPStatus.OK:
            response_dict = response.json()
            return [elem["name"] for elem in response_dict["data"]]
        raise RecordRetrievalException(response, "Failed to list object name!")

    def save(self) -> None:
        """
        Save object to the persistent

        Raises
        ------
        DuplicatedRecordException
            When record with the same key exists at the persistent
        RecordCreationException
            When fail to save the event data (general failure)
        """
        client = Configurations().get_client()
        response = client.post(url=self._route, json=self._get_create_payload())
        if response.status_code != HTTPStatus.CREATED:
            if response.status_code == HTTPStatus.CONFLICT:
                raise DuplicatedRecordException(response=response)
            raise RecordCreationException(response=response)
        type(self).__init__(self, **response.json(), **self._get_init_params())
