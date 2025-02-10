"""
List handler
"""

from __future__ import annotations

import json
from functools import partial
from itertools import groupby
from typing import Any, Dict, List, Optional, Type

import pandas as pd
from pandas import DataFrame
from pydantic import ValidationError

from featurebyte.api.api_object_util import (
    PAGINATED_CALL_PAGE_SIZE,
    ForeignKeyMapping,
    iterate_api_object_using_paginated_routes,
    map_dict_list_to_name,
    map_object_id_to_name,
)
from featurebyte.logging import get_logger
from featurebyte.models.base import FeatureByteBaseDocumentModel

logger = get_logger(__name__)


class ListHandler:
    """
    ListHandler is a class that handles listing of objects. ApiObject classes can use this class to configure how
    to list objects.
    """

    def __init__(
        self,
        route: str,
        list_schema: Type[FeatureByteBaseDocumentModel],
        list_fields: Optional[List[str]] = None,
        list_foreign_keys: Optional[List[ForeignKeyMapping]] = None,
    ):
        """
        Creates a new list handler.

        Parameters
        ----------
        route: str
            Route to list objects
        list_schema: Type[FeatureByteBaseDocumentModel]
            Schema to use to parse list response
        list_fields: List[str]
            Fields to include in list
        list_foreign_keys: List[ForeignKeyMapping]
            List of foreign keys that can be mapped to an object name
        """
        self.route = route
        self.list_schema = list_schema
        self.list_fields = list_fields or ["name", "created_at"]
        self.list_foreign_keys = list_foreign_keys or []

    def list(
        self, include_id: Optional[bool] = False, params: Optional[Dict[str, Any]] = None
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
        for item_dict in iterate_api_object_using_paginated_routes(
            route=self.route, params={"page_size": PAGINATED_CALL_PAGE_SIZE, **params}
        ):
            try:
                output.append(json.loads(self.list_schema(**item_dict).model_dump_json()))
            except ValidationError as exc_info:
                logger.warning(
                    f"Failed to parse list item: {item_dict} with schema: {self.list_schema}. "
                    f"Error: {exc_info}"
                )

        fields = self.list_fields
        if include_id:
            fields = ["id"] + fields

        if not output:
            return DataFrame(columns=fields)

        # apply post-processing on object listing
        return self._post_process_list(DataFrame.from_records(output))[fields]

    def _post_process_list(self, item_list: DataFrame) -> DataFrame:
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

        def _key_func(mapping: ForeignKeyMapping) -> tuple[str, Any, bool]:
            return (
                mapping.foreign_key_field,
                mapping.object_class,
                mapping.use_list_versions,
            )

        list_foreign_keys = sorted(self.list_foreign_keys, key=_key_func)
        for (
            foreign_key_field,
            object_class,
            use_list_version,
        ), foreign_key_mapping_group in groupby(list_foreign_keys, key=_key_func):
            if use_list_version:
                object_list = object_class.list_versions(include_id=True)
            else:
                object_list = object_class.list(include_id=True)

            for foreign_key_mapping in foreign_key_mapping_group:
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
                            map_dict_list_to_name, object_map, object_id_field
                        )
                    else:
                        # foreign_key is an objectid
                        mapping_function = partial(map_object_id_to_name, object_map)
                    new_field_values = item_list[foreign_key_field].apply(mapping_function)
                else:
                    new_field_values = [None] * item_list.shape[0]
                item_list[foreign_key_mapping.new_field_name] = new_field_values
        additionally_processed_list = self.additional_post_processing(item_list)
        return additionally_processed_list

    def additional_post_processing(self, item_list: pd.DataFrame) -> pd.DataFrame:
        """
        Additional post processing to apply to list output.

        Parameters
        ----------
        item_list: pd.DataFrame
            List of items

        Returns
        -------
        pd.DataFrame
        """
        return item_list
