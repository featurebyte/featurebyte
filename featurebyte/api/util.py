"""
This module contains utility function used in api directory
"""
from __future__ import annotations

from typing import Any, Type

from http import HTTPStatus

from requests.models import Response

from featurebyte.config import Configurations
from featurebyte.exception import RecordRetrievalException, ResponseException


def _get_response(
    response: Response, response_exception_class: Type[ResponseException], success_status_code: int
) -> dict[str, Any]:
    """
    Retrieve response data into a dictionary

    Parameters
    ----------
    response: Response
        API response
    response_exception_class: type
        ResponseException class for response failure
    success_status_code: int
        HTTP status code for success case

    Returns
    -------
    dict[str, Any]
        Response in dictionary format

    Raises
    ------
    response_exception_class
        When unexpected failure to create/retrieve/update resource
    """
    if response.status_code == success_status_code:
        response_dict: dict[str, Any] = response.json()
        return response_dict
    raise response_exception_class(response)


def get_entity(entity_name: str, page_size: int = 10) -> dict[str, Any] | None:
    """
    Get entity dictionary given entity name

    Parameters
    ----------
    entity_name: str
        Entity name
    page_size: int
        Number of max items to retrieve per API request

    Returns
    -------
    dict[str, Any]
        Entity dictionary object if found
    None
        Entity not found
    """
    client = Configurations().get_client()
    response = client.get("/entity", params={"page_size": page_size})
    if response.status_code == HTTPStatus.OK:
        response_dict: dict[str, Any] = _get_response(
            response=response,
            response_exception_class=RecordRetrievalException,
            success_status_code=HTTPStatus.OK,
        )
        item: dict[str, Any]
        for item in response_dict["data"]:
            if item["name"] == entity_name:
                return item
        page = response_dict["page"]
        page_size = response_dict["page_size"]
        total = response_dict["total"]
        while total > page * page_size:
            page += 1
            response = client.get("/entity", params={"page": page, "page_size": page_size})
            response_dict = _get_response(
                response=response,
                response_exception_class=RecordRetrievalException,
                success_status_code=HTTPStatus.OK,
            )
            for item in response_dict["data"]:
                if item["name"] == entity_name:
                    return item
    return None
