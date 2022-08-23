"""
ApiObject class
"""
from __future__ import annotations

from typing import Any, Dict, cast

import time
from http import HTTPStatus

from bson.objectid import ObjectId
from pydantic import Field

from featurebyte.config import Configurations
from featurebyte.exception import (
    DuplicatedRecordException,
    ObjectHasBeenSavedError,
    RecordCreationException,
    RecordRetrievalException,
)
from featurebyte.models.base import FeatureByteBaseDocumentModel
from featurebyte.schema.task import TaskStatus


class ApiGetObject(FeatureByteBaseDocumentModel):
    """
    ApiGetObject contains common methods used to retrieve data
    """

    # class variables
    _route = ""

    # other ApiGetObject attributes
    saved: bool = Field(default=False, allow_mutation=False, exclude=True)

    @classmethod
    def _get_init_params(cls) -> dict[str, Any]:
        """
        Additional parameters pass to constructor (without referencing any object)

        Returns
        -------
        dict[str, Any]
        """
        return {}

    @classmethod
    def get(cls, name: str) -> ApiGetObject:
        """
        Retrieve object dictionary from the persistent given object name

        Parameters
        ----------
        name: str
            Object name

        Returns
        -------
        ApiGetObject
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
                return cls(**object_dict, **cls._get_init_params(), saved=True)

            class_name = cls.__name__
            raise RecordRetrievalException(
                response,
                f'{class_name} (name: "{name}") not found. Please save the {class_name} object first.',
            )
        raise RecordRetrievalException(response, "Failed to retrieve the specified object.")

    @classmethod
    def get_by_id(
        cls, id: ObjectId  # pylint: disable=redefined-builtin,invalid-name
    ) -> ApiGetObject:
        """
        Get the API object by specifying the object ID

        Parameters
        ----------
        id: ObjectId
            Object ID value

        Returns
        -------
        ApiGetObject
            ApiGetObject object of the given object ID

        Raises
        ------
        RecordRetrievalException
            When the object not found
        """
        client = Configurations().get_client()
        response = client.get(url=f"{cls._route}/{id}")
        if response.status_code == HTTPStatus.OK:
            return cls(**response.json(), **cls._get_init_params(), saved=True)
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
        output = []
        to_request = True
        page = 1
        while to_request:
            client = Configurations().get_client()
            response = client.get(url=cls._route, params={"page": page})
            if response.status_code == HTTPStatus.OK:
                response_dict = response.json()
                total = response_dict["total"]
                page_size = response_dict["page_size"]
                to_request = total > page * page_size
                page += 1
                output.extend([elem["name"] for elem in response_dict["data"]])
            else:
                raise RecordRetrievalException(response, "Failed to list object names.")
        return output

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


class ApiObject(ApiGetObject):
    """
    ApiObject contains common methods used to interact with API routes
    """

    def _get_create_payload(self) -> dict[str, Any]:
        """
        Construct payload used for post route

        Returns
        -------
        dict[str, Any]
        """
        return self.json_dict()

    def _get_init_params_from_object(self) -> dict[str, Any]:
        """
        Additional parameters pass to constructor from object of the same class
        (other than those parameters from response)

        Returns
        -------
        dict[str, Any]
        """
        return {}

    def _pre_save_operations(self) -> None:
        """
        Operations to be executed before saving the api object
        """
        return

    def save(self) -> None:
        """
        Save object to the persistent

        Raises
        ------
        ObjectHasBeenSavedError
            If the object has been saved before
        DuplicatedRecordException
            When record with the same key exists at the persistent
        RecordCreationException
            When fail to save the event data (general failure)
        """
        if self.saved:
            raise ObjectHasBeenSavedError(
                f'{type(self).__name__} (id: "{self.id}") has been saved before.'
            )

        self._pre_save_operations()
        client = Configurations().get_client()
        response = client.post(url=self._route, json=self._get_create_payload())
        if response.status_code != HTTPStatus.CREATED:
            if response.status_code == HTTPStatus.CONFLICT:
                raise DuplicatedRecordException(response=response)
            raise RecordCreationException(response=response)
        type(self).__init__(
            self, **response.json(), **self._get_init_params_from_object(), saved=True
        )

    @staticmethod
    def post_async_task(route: str, payload: dict[str, Any], delay: float = 0.1) -> dict[str, Any]:
        """
        Post async task to the worker & retrieve the results (blocking)

        Parameters
        ----------
        route: str
            Async task route
        payload: dict[str, Any]
            Task payload
        delay: float
            Delay used in polling the task

        Returns
        -------
        dict[str, Any]
            Response data

        Raises
        ------
        RecordCreationException
            When failed to generate feature job setting analysis task
        RecordRetrievalException
            When failed to retrieve feature job setting analysis result
        """
        client = Configurations().get_client()
        create_response = client.post(url=route, json=payload)
        if create_response.status_code == HTTPStatus.CREATED:
            create_response_dict = create_response.json()
            status = create_response_dict["status"]

            # poll the task route (if the task is still running)
            task_get_response = None
            while status == TaskStatus.STARTED:
                task_get_response = client.get(url=f'/task/{create_response_dict["id"]}')
                if task_get_response.status_code == HTTPStatus.OK:
                    status = task_get_response.json()["status"]
                    time.sleep(delay)
                else:
                    raise RecordRetrievalException(task_get_response)

            # check the task status
            if status != TaskStatus.SUCCESS:
                raise RecordCreationException(response=task_get_response or create_response)

            # retrieve task result
            result_response = client.get(url=create_response_dict["output_path"])
            if result_response.status_code == HTTPStatus.OK:
                return cast(Dict[str, Any], result_response.json())
            raise RecordRetrievalException(response=result_response)
        raise RecordCreationException(response=create_response)
