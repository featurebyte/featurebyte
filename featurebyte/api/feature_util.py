"""
Feature util module.
"""

from typing import List, Optional, Union

import pandas as pd

from featurebyte.api.api_object_util import ForeignKeyMapping
from featurebyte.api.base_table import TableApiObject
from featurebyte.api.entity import Entity
from featurebyte.common.utils import convert_to_list_of_strings

FEATURE_COMMON_LIST_FIELDS = [
    "dtype",
    "readiness",
    "online_enabled",
    "tables",
    "primary_tables",
    "entities",
    "primary_entities",
    "created_at",
]

FEATURE_LIST_FOREIGN_KEYS = [
    ForeignKeyMapping("entity_ids", Entity, "entities"),
    ForeignKeyMapping("table_ids", TableApiObject, "tables"),
    ForeignKeyMapping("primary_entity_ids", Entity, "primary_entities"),
    ForeignKeyMapping("primary_table_ids", TableApiObject, "primary_tables"),
]


def filter_feature_list(
    feature_list: pd.DataFrame,
    primary_entity: Optional[Union[str, List[str]]] = None,
    primary_table: Optional[Union[str, List[str]]] = None,
) -> pd.DataFrame:
    """
    Filter a feature list based on a primary entity and/or primary table.

    Parameters
    ----------
    feature_list: pd.DataFrame
        Feature list to filter
    primary_entity: Optional[Union[str, List[str]]]
        Primary entity to filter on
    primary_table: Optional[Union[str, List[str]]]
        Primary table to filter on

    Returns
    -------
    pd.DataFrame
        Filtered feature list
    """
    target_entities = convert_to_list_of_strings(primary_entity)
    target_tables = convert_to_list_of_strings(primary_table)
    if target_entities:
        feature_list = feature_list[
            feature_list.primary_entities.apply(
                lambda entities: all(entity in entities for entity in target_entities)
            )
        ]
    if target_tables:
        feature_list = feature_list[
            feature_list.primary_tables.apply(
                lambda tables: all(table in tables for table in target_tables)
            )
        ]
    return feature_list
