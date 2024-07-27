"""
ApiObject class
"""

from __future__ import annotations

import operator
from http import HTTPStatus
from typing import Any, Callable, ClassVar, Dict, List, Optional, Type, TypeVar, Union

import pandas as pd
from bson import ObjectId
from cachetools import TTLCache, cachedmethod
from cachetools.keys import hashkey
from pandas import DataFrame
from typeguard import typechecked

from featurebyte.api.api_handler.base import ListHandler
from featurebyte.api.api_object_util import (
    PAGINATED_CALL_PAGE_SIZE,
    ForeignKeyMapping,
    get_api_object_by_id,
    iterate_api_object_using_paginated_routes,
)
from featurebyte.api.mixin import AsyncMixin
from featurebyte.common.formatting_util import InfoDict
from featurebyte.common.utils import construct_repr_string
from featurebyte.config import Configurations
from featurebyte.exception import (
    DuplicatedRecordException,
    RecordCreationException,
    RecordRetrievalException,
    RecordUpdateException,
)
from featurebyte.logging import get_logger
from featurebyte.models.base import FeatureByteBaseDocumentModel, FeatureByteBaseModel

ApiObjectT = TypeVar("ApiObjectT", bound="ApiObject")
ModelT = TypeVar("ModelT", bound=FeatureByteBaseDocumentModel)

CacheKeyNotFound = object()

logger = get_logger(__name__)


def _get_cache_collection_name(
    obj: Union[ApiObjectT, FeatureByteBaseDocumentModel, Type[ApiObjectT]],
) -> str:
    if hasattr(obj, "_get_schema"):
        collection_name = obj._get_schema.Settings.collection_name
    else:
        collection_name = obj.Settings.collection_name
    return str(collection_name)


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
    collection_name = _get_cache_collection_name(obj)
    return hashkey(collection_name, obj.id, *args, **kwargs)


class ApiObject(FeatureByteBaseDocumentModel, AsyncMixin):
    """
    ApiObject contains common methods used to retrieve data
    """

    # class variables
    _route: ClassVar[str] = ""
    _update_schema_class: ClassVar[Optional[Type[FeatureByteBaseModel]]] = None
    _list_schema: ClassVar[Any] = FeatureByteBaseDocumentModel
    _get_schema: ClassVar[Any] = FeatureByteBaseDocumentModel
    _list_fields: ClassVar[Any] = ["name", "created_at"]
    _list_foreign_keys: ClassVar[List[ForeignKeyMapping]] = []
    _cache: ClassVar[Any] = TTLCache(maxsize=1024, ttl=1)  # share cache across all the subclasses

    def _repr_html_(self) -> str:
        """
        HTML representation of the object

        Returns
        -------
        str
        """
        try:
            return InfoDict(self.info()).to_html()
        except (RecordCreationException, RecordRetrievalException):
            # object has not been saved yet
            return repr(self)

    def __repr__(self) -> str:
        try:
            info_repr = repr(self.info())
        except (RecordCreationException, RecordRetrievalException):
            # object has not been saved yet
            info_repr = super().__repr__()

        return construct_repr_string(self, info_repr)

    @property
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

    @property
    def saved(self) -> bool:
        """
        Flag to indicate whether the object has been saved to persistent or not.

        Returns
        -------
        bool
        """
        try:
            # Do not use cached_model here as it may use the cached value.
            # Explicitly call _get_object_dict_by_id to retrieve the latest value from persistent.
            _ = self._get_object_dict_by_id(id_value=self.id)
            return True
        except RecordRetrievalException:
            return False

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
        cls: Type[ApiObjectT],
        name: str,
        other_params: Optional[dict[str, Any]] = None,
        select_func: Optional[Callable[[list[dict[str, Any]]], dict[str, Any]]] = None,
    ) -> dict[str, Any]:
        client = Configurations().get_client()
        other_params = other_params or {}
        response = client.get(url=cls._route, params={"name": name, **other_params})
        if response.status_code == HTTPStatus.OK:
            response_dict = response.json()
            if response_dict["data"]:
                if select_func:
                    return select_func(response_dict["data"])
                return dict(response_dict["data"][0])
            class_name = cls.__name__
            raise RecordRetrievalException(
                response,
                f'{class_name} (name: "{name}") not found. Please save the {class_name} object first.',
            )
        raise RecordRetrievalException(response, "Failed to retrieve the specified object.")

    @classmethod
    def _get_object_dict_by_id(cls: Type[ApiObjectT], id_value: ObjectId) -> dict[str, Any]:
        object_dict = get_api_object_by_id(route=cls._route, id_value=id_value)
        cls._update_cache(object_dict)
        return object_dict

    @classmethod
    def _get(
        cls: Type[ApiObjectT], name: str, other_params: Optional[dict[str, Any]] = None
    ) -> ApiObjectT:
        return cls(
            **cls._get_object_dict_by_name(name=name, other_params=other_params),
            **cls._get_init_params(),
            _validate_schema=True,
        )

    @classmethod
    def get(cls: Type[ApiObjectT], name: str) -> ApiObjectT:
        """
        Retrieve the object from the persistent data store given the object's name.

        This assumes that the object has been saved to the persistent data store. If the object has not been saved,
        an exception will be raised and you should create and save the object first.

        Parameters
        ----------
        name: str
            Name of the object to retrieve.

        Returns
        -------
        ApiObjectT
            Retrieved object with the specified name.

        Examples
        --------
        Note that the examples below are not exhaustive.

        Get an Entity object that is already saved.

        >>> grocery_customer_entity = fb.Entity.get("grocerycustomer")  # doctest: +SKIP
        Entity(name="grocerycustomer", serving_names=["GROCERYCUSTOMERGUID"])

        Get an Entity object that is not saved.

        >>> grocery_customer_entity = fb.Entity.get("random_entity")  # doctest: +SKIP
        'Entity (name: "random_entity") not found. Please save the Entity object first.'

        Get a Feature object that is already saved.

        >>> feature = fb.Feature.get("InvoiceCount_60days")


        Get a Feature Store that is already saved.

        >>> feature_store = fb.FeatureStore.get("playground")
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
        return cls(**object_dict, **cls._get_init_params(), _validate_schema=True)

    @classmethod
    def _get_by_id(
        cls: Type[ApiObjectT],
        id: ObjectId,
        use_cache: bool = True,
    ) -> ApiObjectT:
        if use_cache:
            collection_name = _get_cache_collection_name(cls)
            key = hashkey(collection_name, id)
            cached_value = cls._cache.get(key, CacheKeyNotFound)
            if cached_value is not CacheKeyNotFound:
                return cls.from_persistent_object_dict(cached_value.model_dump(by_alias=True))
        return cls.from_persistent_object_dict(cls._get_object_dict_by_id(id_value=id))

    @classmethod
    def get_by_id(
        cls: Type[ApiObjectT],
        id: ObjectId,
    ) -> ApiObjectT:
        """
        Retrieve the object from the persistent data store given the object's ID.

        This assumes that the object has been saved to the persistent data store. If the object has not been saved,
        an exception will be raised and you should create and save the object first.

        Parameters
        ----------
        id: ObjectId
            Object ID value.

        Returns
        -------
        ApiObjectT
            ApiObject object of the given object ID.

        Examples
        --------
        Note that the examples below are not exhaustive.

        Get an Entity object that is already saved.

        >>> fb.Entity.get_by_id(grocery_customer_entity_id)  # doctest: +SKIP
        <featurebyte.api.entity.Entity at 0x7f511dda3bb0>
        {
          'name': 'grocerycustomer',
          'created_at': '2023-03-22T04:08:21.668000',
          'updated_at': '2023-03-22T04:08:22.050000',
          'serving_names': [
            'GROCERYCUSTOMERGUID'
          ],
          'catalog_name': 'grocery'
        }

        Get an Entity object that is not saved.

        >>> random_object_id = ObjectId()  # doctest: +SKIP
        >>> grocery_customer_entity = fb.Entity.get_by_id(random_object_id)  # doctest: +SKIP
        'Entity (id: <random_object_id.id>) not found. Please save the Entity object first.'

        Get a Feature object that is already saved.

        >>> feature_from_id = fb.Feature.get_by_id(invoice_count_60_days_feature_id)
        """
        return cls._get_by_id(id)

    @classmethod
    def list(cls, include_id: Optional[bool] = True) -> DataFrame:
        """
        List objects name stored in the persistent data store.

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list.

        Returns
        -------
        DataFrame
            Table of objects.

        Examples
        --------
        Note that the examples below are not exhaustive.

        List all feature stores.

        >>> feature_stores = fb.FeatureStore.list()
        """
        return cls._list(include_id=include_id)

    @classmethod
    def _list_handler(cls) -> ListHandler:
        """
        Get list handler.

        Returns
        -------
        ListHandler
            List handler
        """
        return ListHandler(
            route=cls._route,
            list_schema=cls._list_schema,
            list_fields=cls._list_fields,
            list_foreign_keys=cls._list_foreign_keys,
        )

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
        return cls._list_handler().list(include_id=include_id, params=params)

    @typechecked
    def update(
        self,
        update_payload: Dict[str, Any],
        allow_update_local: bool,
        add_internal_prefix: bool = False,
        url: Optional[str] = None,
        skip_update_schema_check: bool = False,
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
        url: Optional[str]
            URL to update the object
        skip_update_schema_check: bool
            Whether to skip update schema check

        Raises
        ------
        NotImplementedError
            If there _update_schema is not set
        DuplicatedRecordException
            If the update causes record conflict
        RecordUpdateException
            When unexpected record update failure
        """
        if skip_update_schema_check:
            data = update_payload
        else:
            if self._update_schema_class is None:
                raise NotImplementedError
            data = self._update_schema_class(**{
                **self.model_dump(by_alias=True),
                **update_payload,
            }).json_dict()

        url = url or f"{self._route}/{self.id}"
        client = Configurations().get_client()
        response = client.patch(url=url, json=data)
        if response.status_code == HTTPStatus.OK:
            object_dict = response.json()
            self._update_cache(object_dict)  # update object cache
            type(self).__init__(
                self,
                **object_dict,
                **self._get_init_params_from_object(),
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
        record_df = pd.DataFrame({
            "action_at": record["action_at"],
            "action_type": record["action_type"],
            "name": record["name"],
            "old_value": previous.set_index(field_name)["value"],
            "new_value": current.set_index(field_name)["value"],
        }).reset_index()
        column_order = [
            "action_at",
            "action_type",
            "name",
            field_name,
            "old_value",
            "new_value",
        ]
        return record_df[column_order]

    def audit(self) -> pd.DataFrame:
        """
        Get list of persistent audit logs which records the object update history

        Returns
        -------
        pd.DataFrame
            List of audit log
        """
        audit_records = []
        for audit_record in iterate_api_object_using_paginated_routes(
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
        Construct summary info of the object. This method is only available for objects that are saved in the
        persistent data store.

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
            When the object cannot be found, or we have received an unexpected response status code.
        """
        client = Configurations().get_client()
        response = client.get(url=f"{self._route}/{self.id}/info", params={"verbose": verbose})
        if response.status_code == HTTPStatus.OK:
            info = response.json()
            info["class_name"] = self.__class__.__name__
            return InfoDict(info)
        raise RecordRetrievalException(response, "Failed to retrieve object info.")

    @typechecked
    def update_description(self, description: Optional[str]) -> None:
        """
        Update description of an object

        Parameters
        ----------
        description: Optional[str]
            Description of the object
        """
        self.update(
            update_payload={"description": description},
            allow_update_local=False,
            url=f"{self._route}/{self.id}/description",
            skip_update_schema_check=True,
        )
