"""
API Object Util
"""

from __future__ import annotations

import ctypes
import threading
from dataclasses import dataclass
from http import HTTPStatus
from typing import Any, Dict, Iterator, List, Optional, Union

from bson import ObjectId

from featurebyte.config import Configurations
from featurebyte.exception import RecordDeletionException, RecordRetrievalException
from featurebyte.logging import get_logger

logger = get_logger(__name__)


PAGINATED_CALL_PAGE_SIZE = 100


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
    # Whether to use list or list_versions to retrieve the data. By default, we will use list.
    # This will override that behaviour to use list_versions.
    use_list_versions: bool = False


class ProgressThread(threading.Thread):
    """
    Thread to get progress updates from websocket
    """

    def __init__(self, task_id: str, progress_bar: Any) -> None:
        self.task_id = task_id
        self.progress_bar = progress_bar
        threading.Thread.__init__(self)

    def run(self) -> None:
        """
        Check progress updates from websocket
        """
        # receive message from websocket
        with Configurations().get_websocket_client(task_id=self.task_id) as websocket_client:
            try:
                while True:
                    message = websocket_client.receive_json()
                    # socket closed
                    if not message:
                        break
                    # update progress bar
                    description = message.get("message")
                    if description:
                        self.progress_bar.text(description)
                    percent = message.get("percent")
                    if percent:
                        # end of stream
                        if percent == -1:
                            break
                        self.progress_bar(percent / 100)
            finally:
                pass

    def get_id(self) -> Optional[int]:
        """
        Returns id of the respective thread

        Returns
        -------
        Optional[int]
            thread id
        """
        # returns id of the respective thread
        if hasattr(self, "_thread_id"):
            return int(getattr(self, "_thread_id"))
        active_threads = getattr(threading, "_active", {})
        for thread_id, thread in active_threads.items():
            if thread is self:
                return int(thread_id)
        return None

    def raise_exception(self) -> None:
        """
        Raises SystemExit exception in the context of the given thread, which should
        cause the thread to exit silently (unless caught).
        """
        thread_id = self.get_id()
        if thread_id:
            res = ctypes.pythonapi.PyThreadState_SetAsyncExc(
                ctypes.c_long(thread_id), ctypes.py_object(SystemExit)
            )
            if res > 1:
                ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, 0)
                logger.warning("Exception raise failure")


class NameAttributeUpdatableMixin:
    """
    This mixin is used to handle the case when name of the api object is updatable.
    """

    def __getattribute__(self, item: str) -> Any:
        """
        Custom __getattribute__ method to handle the case when name of the model is updated.

        Parameters
        ----------
        item: str
            Attribute name.

        Returns
        -------
        Any
            Attribute value.
        """
        if item == "name":
            # Special handling for name attribute is required because name is a common attribute for all
            # FeaturebyteBaseDocumentModel objects. To override the parent name attribute, we need to use
            # __getattribute__ method as property has no effect to override the parent pydantic model attribute.
            try:
                # Retrieve the name from the cached model first. Cached model is used to store the latest
                # model retrieved from the server. If the model has not been saved to the server, name attribute
                # of this model will be used.
                return self.cached_model.name
            except RecordRetrievalException:
                pass
        return super().__getattribute__(item)


def map_object_id_to_name(
    object_map: Dict[Optional[str], str], object_id: Union[str, List[str]]
) -> Union[Optional[str], List[Optional[str]]]:
    """
    Map list of object ids object names

    Parameters
    ----------
    object_map: Dict[Optional[str], str],
        Dict that maps ObjectId string to name
    object_id: Union[str, List[str]]
        List of object ids to map, or object id to map

    Returns
    -------
    Union[Optional[str], List[Optional[str]]]
    """
    if isinstance(object_id, list):
        return [object_map.get(_id) for _id in object_id]
    return object_map.get(object_id)


def map_dict_list_to_name(
    object_map: Dict[Optional[str], str],
    object_id_field: str,
    object_dict: Union[Dict[str, str], List[Dict[str, str]]],
) -> Union[Optional[str], List[Optional[str]]]:
    """
    Map list of object dict to object names

    Parameters
    ----------
    object_map: Dict[Optional[str], str],
        Dict that maps ObjectId to name
    object_id_field: str
        Name of field in object dict to get object id from
    object_dict: Union[Dict[str, str], List[Dict[str, str]]]
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


def to_request_func(response_dict: dict[str, Any], page: int) -> bool:
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


def get_api_object_by_id(
    route: str, id_value: ObjectId, resource_url: Optional[str] = None
) -> dict[str, Any]:
    """
    Retrieve api object by given route & id

    Parameters
    ----------
    route: str
        Route to retrieve object
    id_value: ObjectId
        Id of object to retrieve
    resource_url: Optional[str]
        extrac resource url for the object

    Returns
    -------
    dict[str, Any]
        Retrieved object in dict

    Raises
    ------
    RecordRetrievalException
        When failed to retrieve from list route
    """
    client = Configurations().get_client()

    url = f"{route}/{id_value}"
    if resource_url:
        url = url + "/" + resource_url

    response = client.get(url=url)
    if response.status_code == HTTPStatus.OK:
        object_dict = dict(response.json())
        return object_dict
    raise RecordRetrievalException(response, "Failed to retrieve specified object.")


def delete_api_object_by_id(route: str, id_value: ObjectId) -> None:
    """
    Delete api object by given route & id

    Parameters
    ----------
    route: str
        Route to delete object
    id_value: ObjectId
        Id of object to delete

    Raises
    ------
    RecordDeletionException
        When failed to retrieve from list route
    """
    client = Configurations().get_client()
    response = client.delete(url=f"{route}/{id_value}")
    if response.status_code != HTTPStatus.OK:
        raise RecordDeletionException(response, "Failed to delete specified object.")


def iterate_api_object_using_paginated_routes(
    route: str, params: Optional[dict[str, Any]] = None
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
            to_request = to_request_func(response_dict, page)
            page += 1
            for obj_dict in response_dict["data"]:
                yield obj_dict
        else:
            raise RecordRetrievalException(response, f"Failed to list {route}.")
