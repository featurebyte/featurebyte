"""
ApiObject class
"""
from __future__ import annotations

from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    Iterator,
    List,
    Literal,
    Optional,
    Type,
    TypeVar,
    cast,
)

import time
from functools import partial
from http import HTTPStatus

import lazy_object_proxy
from bson.objectid import ObjectId
from pydantic import Field
from typeguard import typechecked

from featurebyte.config import Configurations
from featurebyte.exception import (
    DuplicatedRecordException,
    ObjectHasBeenSavedError,
    RecordCreationException,
    RecordRetrievalException,
    RecordUpdateException,
)
from featurebyte.logger import logger
from featurebyte.models.base import FeatureByteBaseDocumentModel, FeatureByteBaseModel
from featurebyte.schema.task import TaskStatus

ApiObjectT = TypeVar("ApiObjectT", bound="ApiGetObject")
ConflictResolution = Literal["raise", "retrieve"]


class ApiGetObject(FeatureByteBaseDocumentModel):
    """
    ApiGetObject contains common methods used to retrieve data
    """

    # class variables
    _route: ClassVar[str] = ""

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
    def _get_object_dict_by_name(cls: Type[ApiObjectT], name: str) -> dict[str, Any]:
        client = Configurations().get_client()
        response = client.get(url=cls._route, params={"name": name})
        if response.status_code == HTTPStatus.OK:
            response_dict = response.json()
            if response_dict["data"]:
                return dict(response_dict["data"][0])
            class_name = cls.__name__
            raise RecordRetrievalException(
                response,
                f'{class_name} (name: "{name}") not found. Please save the {class_name} object first.',
            )
        raise RecordRetrievalException(response, "Failed to retrieve the specified object.")

    @classmethod
    def _get(cls: Type[ApiObjectT], name: str) -> ApiObjectT:
        return cls(**cls._get_object_dict_by_name(name=name), **cls._get_init_params(), saved=True)

    @classmethod
    def get(cls: Type[ApiObjectT], name: str) -> ApiObjectT:
        """
        Retrieve lazy object from the persistent given object name

        Parameters
        ----------
        name: str
            Object name

        Returns
        -------
        ApiObjectT
            ApiObject object of the given event data name
        """
        return cast(ApiObjectT, lazy_object_proxy.Proxy(partial(cls._get, name)))

    @classmethod
    def from_persistent_object_dict(
        cls: Type[ApiObjectT], object_dict: dict[str, Any]
    ) -> ApiObjectT:
        """
        Construct the object from dictionary stored at the persistent

        Parameters
        ----------
        object_dict: dict[str, Any]
            Record in dictionary format

        Returns
        -------
        ApiObjectT
            Deserialized object
        """
        return cls(**object_dict, **cls._get_init_params(), saved=True)

    @classmethod
    def _get_by_id(
        cls: Type[ApiObjectT], id: ObjectId  # pylint: disable=redefined-builtin,invalid-name
    ) -> ApiObjectT:
        client = Configurations().get_client()
        response = client.get(url=f"{cls._route}/{id}")
        if response.status_code == HTTPStatus.OK:
            return cls.from_persistent_object_dict(object_dict=response.json())
        raise RecordRetrievalException(response, "Failed to retrieve specified object.")

    @classmethod
    def get_by_id(
        cls: Type[ApiObjectT], id: ObjectId  # pylint: disable=redefined-builtin,invalid-name
    ) -> ApiObjectT:
        """
        Get the lazy object from the persistent given the object ID

        Parameters
        ----------
        id: ObjectId
            Object ID value

        Returns
        -------
        ApiObjectT
            ApiGetObject object of the given object ID
        """
        return cast(ApiObjectT, lazy_object_proxy.Proxy(partial(cls._get_by_id, id)))

    @staticmethod
    def _default_to_request_func(response_dict: dict[str, Any], page: int) -> bool:
        """
        Default helper function to check whether to continue calling list route

        Parameters
        ----------
        response_dict: dict[str, Any]
            Response data
        page: int
            Page number

        Returns
        -------
        Flag to indicate whether to continue calling list route
        """
        return bool(response_dict["total"] > (page * response_dict["page_size"]))

    @classmethod
    def _iterate_paginated_routes(
        cls,
        route: str,
        params: dict[str, Any] | None = None,
        to_request_func: Callable[[dict[str, Any], int], bool] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """
        List route response generator

        Parameters
        ----------
        route: str
            List route
        params: dict[str, Any] | None
            Route parameters
        to_request_func: Callable[[dict[str, Any], int], bool] = None,
            Function used to check whether to continue calling the route

        Yields
        -------
        Iterator[dict[str, Any]]
            List route response

        Raises
        ------
        RecordRetrievalException
            When failed to retrieve from list route
        """
        client = Configurations().get_client()
        to_request, page = True, 1
        params = params or {}
        if to_request_func is None:
            to_request_func = cls._default_to_request_func
        while to_request:
            params = params.copy()
            params["page"] = page
            response = client.get(url=route, params=params)
            if response.status_code == HTTPStatus.OK:
                response_dict = response.json()
                to_request = to_request_func(response_dict, page)
                page += 1
                yield response_dict
            else:
                raise RecordRetrievalException(response, "Failed to list object names.")

    @classmethod
    def list(cls) -> list[str]:
        """
        List the object name store at the persistent

        Returns
        -------
        list[str]
            List of object name
        """
        output = []
        for response_dict in cls._iterate_paginated_routes(route=cls._route):
            for item in response_dict["data"]:
                output.append(item["name"])
        return output

    def audit(self) -> dict[str, Any]:
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
            return dict(response.json())
        raise RecordRetrievalException(response, "Failed to list object audit log.")

    @typechecked
    def _get_audit_history(self, field_name: str) -> List[Dict[str, Any]]:
        """
        Retrieve field audit history

        Parameters
        ----------
        field_name: str
            Field name

        Returns
        -------
        List of history

        Raises
        ------
        RecordRetrievalException
            When unexpected retrieval failure
        """
        client = Configurations().get_client()
        response = client.get(url=f"{self._route}/history/{field_name}/{self.id}")
        if response.status_code == HTTPStatus.OK:
            history: list[dict[str, Any]] = response.json()
            return history
        raise RecordRetrievalException(response)

    @typechecked
    def info(self, verbose: bool = False) -> Dict[str, Any]:
        """
        Construct summary info of the API object

        Parameters
        ----------
        verbose: bool
            Control verbose level of the summary

        Returns
        -------
        Dict[str, Any]

        Raises
        ------
        RecordRetrievalException
            When the object not found or unexpected response status code
        """
        client = Configurations().get_client()
        response = client.get(url=f"{self._route}/{self.id}/info", params={"verbose": verbose})
        if response.status_code == HTTPStatus.OK:
            return dict(response.json())
        raise RecordRetrievalException(response, "Failed to retrieve object info.")


class ApiObject(ApiGetObject):
    """
    ApiObject contains common methods used to interact with API routes
    """

    # class variables
    _update_schema_class: ClassVar[Optional[Type[FeatureByteBaseModel]]] = None

    def _get_create_payload(self) -> dict[str, Any]:
        """
        Construct payload used for post route

        Returns
        -------
        dict[str, Any]
        """
        return self.json_dict(exclude_none=True)

    def _get_init_params_from_object(self) -> dict[str, Any]:
        """
        Additional parameters pass to constructor from object of the same class
        (other than those parameters from response)

        Returns
        -------
        dict[str, Any]
        """
        return {}

    def _pre_save_operations(self, conflict_resolution: ConflictResolution) -> None:
        """
        Operations to be executed before saving the api object

        Parameters
        ----------
        conflict_resolution: ConflictResolution
            "raise" raises error when then counters conflict error (default)
            "retrieve" handle conflict error by retrieving object with the same name
        """
        _ = conflict_resolution

    @typechecked
    def update(self, update_payload: Dict[str, Any]) -> None:
        """
        Update object in the persistent

        Parameters
        ----------
        update_payload: dict[str, Any]
            Fields to update in dictionary format

        Raises
        ------
        NotImplementedError
            If there _update_schema is not set
        DuplicatedRecordException
            If the update causes record conflict
        RecordUpdateException
            When unexpected record update failure
        """
        if self._update_schema_class is None:
            raise NotImplementedError

        data = self._update_schema_class(  # pylint: disable=not-callable
            **{**self.dict(), **update_payload}
        )
        client = Configurations().get_client()
        response = client.patch(url=f"{self._route}/{self.id}", json=data.json_dict())
        if response.status_code == HTTPStatus.OK:
            type(self).__init__(
                self,
                **response.json(),
                **self._get_init_params_from_object(),
                saved=True,
            )
        elif response.status_code == HTTPStatus.NOT_FOUND:
            for key, value in update_payload.items():
                setattr(self, key, value)
        elif response.status_code == HTTPStatus.CONFLICT:
            raise DuplicatedRecordException(response=response)
        else:
            raise RecordUpdateException(response=response)

    @typechecked
    def save(self, conflict_resolution: ConflictResolution = "raise") -> None:
        """
        Save object to the persistent. Conflict could be triggered when the object
        being saved has violated uniqueness check at the persistent (for example,
        same ID has been used by another record stored at the persistent).

        Parameters
        ----------
        conflict_resolution: ConflictResolution
            "raise" raises error when then counters conflict error (default)
            "retrieve" handle conflict error by retrieving the object with the same name

        Raises
        ------
        ObjectHasBeenSavedError
            If the object has been saved before
        DuplicatedRecordException
            When record with the same key exists at the persistent
        RecordCreationException
            When fail to save the new object (general failure)
        """
        if self.saved:
            raise ObjectHasBeenSavedError(
                f'{type(self).__name__} (id: "{self.id}") has been saved before.'
            )

        self._pre_save_operations(conflict_resolution=conflict_resolution)
        client = Configurations().get_client()
        response = client.post(url=self._route, json=self._get_create_payload())
        retrieve_object = False
        if response.status_code != HTTPStatus.CREATED:
            if response.status_code == HTTPStatus.CONFLICT:
                if conflict_resolution == "retrieve":
                    retrieve_object = True
                else:
                    raise DuplicatedRecordException(response=response)
            if not retrieve_object:
                raise RecordCreationException(response=response)

        if retrieve_object:
            assert self.name is not None
            object_dict = self._get_object_dict_by_name(name=self.name)
        else:
            object_dict = response.json()
        type(self).__init__(self, **object_dict, **self._get_init_params_from_object(), saved=True)

    @staticmethod
    def post_async_task(route: str, payload: dict[str, Any], delay: float = 3.0) -> dict[str, Any]:
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
            while status in [TaskStatus.STARTED, TaskStatus.PENDING]:
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
            output_url = create_response_dict.get("output_path")
            if output_url is None:
                if task_get_response:
                    output_url = task_get_response.json().get("output_path")
            if output_url is None:
                raise RecordRetrievalException(response=task_get_response or create_response)

            logger.debug("Retrieving task result", extra={"output_url": output_url})
            result_response = client.get(url=output_url)
            if result_response.status_code == HTTPStatus.OK:
                return dict(result_response.json())
            raise RecordRetrievalException(response=result_response)
        raise RecordCreationException(response=create_response)
