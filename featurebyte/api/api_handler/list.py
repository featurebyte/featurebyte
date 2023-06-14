"""
List handler
"""
from typing import Any, Dict, List, Optional, Type

from functools import partial
from itertools import groupby

from pandas import DataFrame

from featurebyte.api.api_object_util import (
    ForeignKeyMapping,
    iterate_api_object_using_paginated_routes,
    map_dict_list_to_name,
    map_object_id_to_name,
)
from featurebyte.models.base import FeatureByteBaseDocumentModel

# TODO: consolidate this with the one in api_object.py
PAGINATED_CALL_PAGE_SIZE = 100


class ListHandler:
    def __init__(
        self,
        route: str,
        list_schema: Type[FeatureByteBaseDocumentModel],
        list_fields: List[str] = ("name", "created_at"),
        list_foreign_keys: List[ForeignKeyMapping] = (),
    ):
        self.route = route
        self.list_schema = list_schema
        self.list_fields = list_fields
        self.list_foreign_keys = list_foreign_keys

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
            output.append(self.list_schema(**item_dict).dict())

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
                    new_field_values = [[]] * item_list.shape[0]
                item_list[foreign_key_mapping.new_field_name] = new_field_values
        return item_list
