"""
ApiObject class
"""
from __future__ import annotations

from typing import Any, ClassVar, Dict, Iterator, List, Literal, Optional, Type, TypeVar, Union

import operator
import time
from dataclasses import dataclass
from functools import partial
from http import HTTPStatus

import pandas as pd
from bson.objectid import ObjectId
from cachetools import TTLCache, cachedmethod
from cachetools.keys import hashkey
from pandas import DataFrame
from pydantic import Field
from rich.pretty import pretty_repr
from typeguard import typechecked

from featurebyte.common.utils import construct_repr_string
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

ApiObjectT = TypeVar("ApiObjectT", bound="ApiObject")
ModelT = TypeVar("ModelT", bound=FeatureByteBaseDocumentModel)
ConflictResolution = Literal["raise", "retrieve"]
PAGINATED_CALL_PAGE_SIZE = 100


class PrettyDict(Dict[str, Any]):
    """
    Dict with prettified representation
    """

    def __repr__(self) -> str:
        return pretty_repr(dict(self), expand_all=True, indent_size=2)


def get_api_object_cache_key(
    obj: Union[ApiObjectT, FeatureByteBaseDocumentModel], *args: Any, **kwargs: Any
) -> Any:
    """
    Construct cache key for a given document model object

    Parameters
    ----------
    obj: Union[ApiObjectT, FeatureByteBaseDocumentModel]
        Api object or document model object
    args: Any
        Additional positional arguments
    kwargs: Any
        Additional keywords arguments

    Returns
    -------
    Any
    """
    # Return a cache key for _cache key retrieval (only collection name & object ID are used)
    if hasattr(obj, "_get_schema"):
        collection_name = (
            obj._get_schema.Settings.collection_name  # type: ignore # pylint: disable=protected-access
        )
    else:
        collection_name = obj.Settings.collection_name
    return hashkey(collection_name, obj.id, *args, **kwargs)


@dataclass
class ForeignKeyMapping:
    """
    ForeignKeyMapping contains information about a foreign key field mapping that we can use to map
    IDs to their names in the list API response.
    """

    # Field name of the existing ID field in the list API response.
    foreign_key_field: str
    # Object class that we will be trying to retrieve the data from.
    object_class: Any
    # New field name that we want to display in the list API response
    new_field_name: str
    # Field to display instead of `name` from the retrieved list API response.
    # By default, we will pull the `name` from the retrieved values. This will override that behaviour
    # to pull a different field.
    display_field_override: Optional[str] = None


class ApiObject(FeatureByteBaseDocumentModel):
    """
    ApiObject contains common methods used to retrieve data
    """

    # class variables
    _route: ClassVar[str] = ""
    _update_schema_class: ClassVar[Optional[Type[FeatureByteBaseModel]]] = None
    _list_schema = FeatureByteBaseDocumentModel
    _get_schema = FeatureByteBaseDocumentModel
    _list_fields = ["name", "created_at"]
    _list_foreign_keys: List[ForeignKeyMapping] = []

    # global api object cache shared by all the ApiObject class & its child classes
    _cache: Any = TTLCache(maxsize=1024, ttl=1)

    # other ApiObject attributes
    saved: bool = Field(default=False, allow_mutation=False, exclude=True)

    def __repr__(self) -> str:
        info_repr = ""
        try:
            info_repr = repr(self.info())
        except RecordCreationException:
            # object has not been saved yet
            pass

        return construct_repr_string(self, info_repr)

    @property  # type: ignore
    @cachedmethod(cache=operator.attrgetter("_cache"), key=get_api_object_cache_key)
    def cached_model(self: ModelT) -> ModelT:
        """
        Retrieve the model stored the persistent (result of this property will be cached within the time-to-live
        period specified during _cache attribution construction). If the cached expired, calling this property
        will make an API call to retrieve the most recent result stored at persistent.

        Returns
        -------
        FeatureByteBaseDocumentModel
        """
        return self._get_schema(**self._get_object_dict_by_id(id_value=self.id))  # type: ignore

    @classmethod
    def _update_cache(cls, object_dict: dict[str, Any]) -> None:
        """
        Override existing model stored in the cache

        Parameters
        ----------
        object_dict: dict[str, Any]
            model object in dictionary format
        """
        model = cls._get_schema(**object_dict)
        cls._cache[get_api_object_cache_key(model)] = model

    @classmethod
    def _get_init_params(cls) -> dict[str, Any]:
        """
        Additional parameters pass to constructor (without referencing any object)

        Returns
        -------
        dict[str, Any]
        """
        return {}

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
    def _get_object_dict_by_name(
        cls: Type[ApiObjectT], name: str, other_params: Optional[dict[str, Any]] = None
    ) -> dict[str, Any]:
        client = Configurations().get_client()
        other_params = other_params or {}
        response = client.get(url=cls._route, params={"name": name, **other_params})
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
    def _get_object_dict_by_id(cls: Type[ApiObjectT], id_value: ObjectId) -> dict[str, Any]:
        client = Configurations().get_client()
        response = client.get(url=f"{cls._route}/{id_value}")
        if response.status_code == HTTPStatus.OK:
            object_dict = dict(response.json())
            cls._update_cache(object_dict)
            return object_dict
        raise RecordRetrievalException(response, "Failed to retrieve specified object.")

    @classmethod
    def _get(
        cls: Type[ApiObjectT], name: str, other_params: Optional[dict[str, Any]] = None
    ) -> ApiObjectT:
        return cls(
            **cls._get_object_dict_by_name(name=name, other_params=other_params),
            **cls._get_init_params(),
            saved=True,
            _validate_schema=True,
        )

    @classmethod
    def get(cls: Type[ApiObjectT], name: str) -> ApiObjectT:
        """
        Retrieve object from the persistent given object name

        Parameters
        ----------
        name: str
            Object name

        Returns
        -------
        ApiObjectT
            Retrieved object with the specified name
        """
        return cls._get(name)

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
        return cls(**object_dict, **cls._get_init_params(), saved=True, _validate_schema=True)

    @classmethod
    def _get_by_id(
        cls: Type[ApiObjectT], id: ObjectId  # pylint: disable=redefined-builtin,invalid-name
    ) -> ApiObjectT:
        return cls.from_persistent_object_dict(cls._get_object_dict_by_id(id_value=id))

    @classmethod
    def get_by_id(
        cls: Type[ApiObjectT], id: ObjectId  # pylint: disable=redefined-builtin,invalid-name
    ) -> ApiObjectT:
        """
        Get the object from the persistent given the object ID

        Parameters
        ----------
        id: ObjectId
            Object ID value

        Returns
        -------
        ApiObjectT
            ApiObject object of the given object ID
        """
        return cls._get_by_id(id)

    @staticmethod
    def _to_request_func(response_dict: dict[str, Any], page: int) -> bool:
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
    def _iterate_api_object_using_paginated_routes(
        cls, route: str, params: dict[str, Any] | None = None
    ) -> Iterator[dict[str, Any]]:
        """
        Api object generator by iterating listing route

        Parameters
        ----------
        route: str
            List route
        params: dict[str, Any] | None
            Route parameters

        Yields
        -------
        Iterator[dict[str, Any]]
            Iterator of api object records

        Raises
        ------
        RecordRetrievalException
            When failed to retrieve from list route
        """
        client = Configurations().get_client()
        to_request, page = True, 1
        params = params or {}
        while to_request:
            params = params.copy()
            params["page"] = page
            response = client.get(url=route, params=params)
            if response.status_code == HTTPStatus.OK:
                response_dict = response.json()
                to_request = cls._to_request_func(response_dict, page)
                page += 1
                for obj_dict in response_dict["data"]:
                    yield obj_dict
            else:
                raise RecordRetrievalException(response, f"Failed to list {route}.")

    @classmethod
    def list(cls, include_id: Optional[bool] = False) -> DataFrame:
        """
        List the object name store at the persistent

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list

        Returns
        -------
        DataFrame
            Table of objects
        """
        return cls._list(include_id=include_id)

    @classmethod
    def _list(
        cls, include_id: Optional[bool] = False, params: Optional[Dict[str, Any]] = None
    ) -> DataFrame:
        """
        List the object name store at the persistent

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list
        params: Optional[Dict[str, Any]]
            Additional parameters to include in request

        Returns
        -------
        DataFrame
            Table of objects
        """
        params = params or {}
        output = []
        for item_dict in cls._iterate_api_object_using_paginated_routes(
            route=cls._route, params={"page_size": PAGINATED_CALL_PAGE_SIZE, **params}
        ):
            output.append(cls._list_schema(**item_dict).dict())

        fields = cls._list_fields
        if include_id:
            fields = ["id"] + fields

        if not output:
            return DataFrame(columns=fields)

        # apply post-processing on object listing
        return cls._post_process_list(DataFrame.from_records(output))[fields]

    @staticmethod
    def map_dict_list_to_name(
        object_map: Dict[Optional[ObjectId], str],
        object_id_field: str,
        object_dict: Union[Dict[str, ObjectId], List[Dict[str, ObjectId]]],
    ) -> Union[Optional[str], List[Optional[str]]]:
        """
        Map list of object dict to object names

        Parameters
        ----------
        object_map: Dict[Optional[ObjectId], str],
            Dict that maps ObjectId to name
        object_id_field: str
            Name of field in object dict to get object id from
        object_dict: Union[Dict[str, ObjectId], List[Dict[str, ObjectId]]]
            List of dict to map

        Returns
        -------
        Union[Optional[str], List[Optional[str]]]
        """
        if isinstance(object_dict, list):
            return [
                object_map.get(_obj_dict.get(object_id_field))
                for _obj_dict in object_dict
                if _obj_dict.get(object_id_field)
            ]
        return object_map.get(object_dict.get(object_id_field))

    @staticmethod
    def map_object_id_to_name(
        object_map: Dict[Optional[ObjectId], str], object_id: Union[ObjectId, List[ObjectId]]
    ) -> Union[Optional[str], List[Optional[str]]]:
        """
        Map list of object ids object names

        Parameters
        ----------
        object_map: Dict[Optional[ObjectId], str],
            Dict that maps ObjectId to name
        object_id: Union[ObjectId, List[ObjectId]]
            List of object ids to map, or object id to map

        Returns
        -------
        Union[Optional[str], List[Optional[str]]]
        """
        if isinstance(object_id, list):
            return [object_map.get(_id) for _id in object_id]
        return object_map.get(object_id)

    @classmethod
    def _post_process_list(cls, item_list: DataFrame) -> DataFrame:
        """
        Post process list output

        Parameters
        ----------
        item_list: DataFrame
            List of documents

        Returns
        -------
        DataFrame
        """
        # populate object names using foreign keys
        for foreign_key_mapping in cls._list_foreign_keys:
            object_list = foreign_key_mapping.object_class.list(include_id=True)
            if object_list.shape[0] > 0:
                object_list.index = object_list.id
                field_to_pull = (
                    foreign_key_mapping.display_field_override
                    if foreign_key_mapping.display_field_override
                    else "name"
                )
                object_map = object_list[field_to_pull].to_dict()
                foreign_key_field = foreign_key_mapping.foreign_key_field
                if "." in foreign_key_field:
                    # foreign_key is a dict
                    foreign_key_field, object_id_field = foreign_key_field.split(".")
                    mapping_function = partial(
                        cls.map_dict_list_to_name, object_map, object_id_field
                    )
                else:
                    # foreign_key is an objectid
                    mapping_function = partial(cls.map_object_id_to_name, object_map)
                new_field_values = item_list[foreign_key_field].apply(mapping_function)
            else:
                new_field_values = [[]] * item_list.shape[0]
            item_list[foreign_key_mapping.new_field_name] = new_field_values

        return item_list

    @typechecked
    def update(
        self,
        update_payload: Dict[str, Any],
        allow_update_local: bool,
        add_internal_prefix: bool = False,
    ) -> None:
        """
        Update object in the persistent

        Parameters
        ----------
        update_payload: dict[str, Any]
            Fields to update in dictionary format
        allow_update_local: bool
            Whether to allow update load object if the object has not been saved
        add_internal_prefix: bool
            Whether to add internal prefix (`internal_`) to the update key (used when the attribute to be updated
            starts with `internal_`). This flag only affects local update behavior (no effect if `allow_update_local`
            is disabled).

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
            **{**self.json_dict(), **update_payload}
        )
        client = Configurations().get_client()
        response = client.patch(url=f"{self._route}/{self.id}", json=data.json_dict())
        if response.status_code == HTTPStatus.OK:
            object_dict = response.json()
            self._update_cache(object_dict)  # update object cache
            type(self).__init__(
                self,
                **object_dict,
                **self._get_init_params_from_object(),
                saved=True,
            )
        elif response.status_code == HTTPStatus.NOT_FOUND and allow_update_local:
            for key, value in update_payload.items():
                key = f"internal_{key}" if add_internal_prefix else key
                setattr(self, key, value)
        elif response.status_code == HTTPStatus.CONFLICT:
            raise DuplicatedRecordException(response=response)
        else:
            raise RecordUpdateException(response=response)

    @staticmethod
    def _prepare_audit_record(record: Dict[str, Any]) -> pd.DataFrame:
        field_name = "field_name"
        previous = pd.json_normalize(record["previous_values"]).melt(var_name=field_name)
        current = pd.json_normalize(record["current_values"]).melt(var_name=field_name)
        record_df = pd.DataFrame(
            {
                "action_at": record["action_at"],
                "action_type": record["action_type"],
                "name": record["name"],
                "old_value": previous.set_index(field_name)["value"],
                "new_value": current.set_index(field_name)["value"],
            }
        ).reset_index()
        column_order = [
            "action_at",
            "action_type",
            "name",
            field_name,
            "old_value",
            "new_value",
        ]
        return record_df[column_order]  # pylint: disable=unsubscriptable-object

    def audit(self) -> pd.DataFrame:
        """
        Get list of persistent audit logs which records the object update history

        Returns
        -------
        pd.DataFrame
            List of audit log
        """
        audit_records = []
        for audit_record in self._iterate_api_object_using_paginated_routes(
            route=f"{self._route}/audit/{self.id}", params={"page_size": PAGINATED_CALL_PAGE_SIZE}
        ):
            audit_records.append(self._prepare_audit_record(audit_record))
        return pd.concat(audit_records).reset_index(drop=True)

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
        Construct summary info of the object.

        Parameters
        ----------
        verbose: bool
            Control verbose level of the summary.

        Returns
        -------
        Dict[str, Any]
            Key-value mapping of properties of the object.

        Raises
        ------
        RecordRetrievalException
            When the object not found or unexpected response status code.
        """
        client = Configurations().get_client()
        response = client.get(url=f"{self._route}/{self.id}/info", params={"verbose": verbose})
        if response.status_code == HTTPStatus.OK:
            return PrettyDict(response.json())
        raise RecordRetrievalException(response, "Failed to retrieve object info.")

    @staticmethod
    def post_async_task(
        route: str, payload: dict[str, Any], delay: float = 3.0, retrieve_result: bool = True
    ) -> dict[str, Any]:
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
        retrieve_result: bool
            Whether to retrieve result from output_url

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

            if not retrieve_result:
                return {"output_url": output_url}

            logger.debug("Retrieving task result", extra={"output_url": output_url})
            result_response = client.get(url=output_url)
            if result_response.status_code == HTTPStatus.OK:
                return dict(result_response.json())
            raise RecordRetrievalException(response=result_response)
        raise RecordCreationException(response=create_response)


class SavableApiObject(ApiObject):
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
        return self.json_dict(exclude_none=True)

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
        if self.saved and conflict_resolution == "raise":
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

        self._update_cache(object_dict)  # update api object cache store
        type(self).__init__(
            self,
            **object_dict,
            **self._get_init_params_from_object(),
            saved=True,
        )
