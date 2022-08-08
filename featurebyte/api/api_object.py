"""
ApiObject class
"""
from __future__ import annotations

from typing import Any

from http import HTTPStatus

from bson.objectid import ObjectId

from featurebyte.config import Configurations
from featurebyte.exception import (
    DuplicatedRecordException,
    RecordCreationException,
    RecordRetrievalException,
)
from featurebyte.models.base import FeatureByteBaseDocumentModel


class ApiObject(FeatureByteBaseDocumentModel):
    """
    ApiObject contains common methods used to interact with API routes
    """

    # class variables
    _route = ""

    def _get_init_params_from_object(self) -> dict[str, Any]:
        """
        Additional parameters pass to constructor from object of the same class
        (other than those parameters from response)

        Returns
        -------
        dict[str, Any]
        """
        return {}

    @classmethod
    def _get_init_params(cls) -> dict[str, Any]:
        """
        Additional parameters pass to constructor (without referencing any object)

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
            ApiObject object of the given event data name

        Raises
        ------
        RecordRetrievalException
            When the object not found or unexpected response status code
        """
        client = Configurations().get_client()
        response = client.get(url=cls._route, params={"name": name})
        if response.status_code == HTTPStatus.OK:
            response_dict = response.json()
            if response_dict["data"]:
                object_dict = response_dict["data"][0]
                return cls(**object_dict, **cls._get_init_params())

            class_name = cls.__name__
            raise RecordRetrievalException(
                response,
                f'{class_name} (name: "{name}") not found. Please save the {class_name} object first.',
            )
        raise RecordRetrievalException(response, "Failed to retrieve the specified object.")

    @classmethod
    def get_by_id(cls, id: ObjectId) -> ApiObject:  # pylint: disable=redefined-builtin,invalid-name
        """
        Get the API object by specifying the object ID

        Parameters
        ----------
        id: ObjectId
            Object ID value

        Returns
        -------
        ApiObject
            ApiObject object of the given object ID

        Raises
        ------
        RecordRetrievalException
            When the object not found
        """
        client = Configurations().get_client()
        response = client.get(url=f"{cls._route}/{id}")
        if response.status_code == HTTPStatus.OK:
            return cls(**response.json(), **cls._get_init_params())
        raise RecordRetrievalException(response, "Failed to retrieve specified object.")

    @classmethod
    def list(cls) -> list[str]:
        """
        List the object name store at the persistent

        Returns
        -------
        list[str]
            List of object name

        Raises
        ------
        RecordRetrievalException
            When the response status code is unexpected
        """
        client = Configurations().get_client()
        response = client.get(url=cls._route)
        if response.status_code == HTTPStatus.OK:
            response_dict = response.json()
            return [elem["name"] for elem in response_dict["data"]]
        raise RecordRetrievalException(response, "Failed to list object names.")

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
        type(self).__init__(self, **response.json(), **self._get_init_params_from_object())

    def audit(self) -> Any:
        """
        Get list of persistent audit logs which records the object update history

        Returns
        -------
        Any
            List of audit log

        Raises
        ------
        RecordRetrievalException
            When the response status code is unexpected
        """
        client = Configurations().get_client()
        response = client.get(url=f"{self._route}/audit/{self.id}")
        if response.status_code == HTTPStatus.OK:
            return response.json()
        raise RecordRetrievalException(response, "Failed to list object audit log.")
