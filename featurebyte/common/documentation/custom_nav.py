"""
Custom nav module
"""

from typing import Any, Iterable, List, Mapping

from mkdocs_gen_files import Nav  # type: ignore[attr-defined]

from featurebyte.common.documentation.constants import (
    ADD_METADATA,
    BATCH_FEATURE_TABLE,
    BATCH_REQUEST_TABLE,
    CATALOG,
    CLASS_METHODS,
    CLEANING_OPERATION,
    CONSTRUCTOR,
    CONTEXT,
    CREATE,
    CREATE_FEATURE,
    CREATE_FEATURE_GROUP,
    CREATE_TABLE,
    CREATE_TARGET,
    CREDENTIAL,
    DATA_SOURCE,
    DEPLOY,
    DEPLOYMENT,
    ENTITY,
    ENUMS,
    EXPLORE,
    FEATURE,
    FEATURE_GROUP,
    FEATURE_JOB_SETTING,
    FEATURE_LIST,
    FEATURE_STORE,
    GET,
    GET_VIEW,
    GROUPBY,
    HISTORICAL_FEATURE_TABLE,
    INFO,
    JOIN,
    LAGS,
    LINEAGE,
    LIST,
    MANAGE,
    OBSERVATION_TABLE,
    ONLINE_STORE,
    ONLINE_STORE_DETAILS,
    RELATIONSHIP,
    REQUEST_COLUMN,
    SAVE,
    SERVE,
    SET_FEATURE_JOB,
    SOURCE_TABLE,
    TABLE,
    TABLE_COLUMN,
    TARGET,
    TRANSFORM,
    TYPE,
    USE_CASE,
    USER_DEFINED_FUNCTION,
    UTILITY_CLASSES,
    UTILITY_METHODS,
    VIEW,
    VIEW_COLUMN,
    WAREHOUSE,
)


class BetaWave3Nav(Nav):
    """
    CustomItemsNav class overrides the mkdocs Nav to provide some custom ordering of menu items.

    In particular, we have a specific order for menu items provided via the _custom_root_level_order. Menu items not
    provided in this list will be placed after these items, and sorted alphabetically.
    """

    _custom_root_level_order = [
        FEATURE_STORE,
        ONLINE_STORE,
        CATALOG,
        DATA_SOURCE,
        SOURCE_TABLE,
        TABLE,
        TABLE_COLUMN,
        ENTITY,
        RELATIONSHIP,
        VIEW,
        VIEW_COLUMN,
        CONTEXT,
        USE_CASE,
        TARGET,
        FEATURE,
        FEATURE_GROUP,
        FEATURE_LIST,
        OBSERVATION_TABLE,
        HISTORICAL_FEATURE_TABLE,
        DEPLOYMENT,
        BATCH_REQUEST_TABLE,
        BATCH_FEATURE_TABLE,
        USER_DEFINED_FUNCTION,
        UTILITY_CLASSES,
        UTILITY_METHODS,
    ]

    _custom_second_level_order = [
        WAREHOUSE,
        ONLINE_STORE_DETAILS,
        CREDENTIAL,
        CLASS_METHODS,
        TYPE,
        LIST,
        GET,
        GET_VIEW,
        CREATE,
        CREATE_FEATURE,
        CREATE_FEATURE_GROUP,
        CREATE_TABLE,
        CREATE_TARGET,
        CONSTRUCTOR,
        ADD_METADATA,
        SAVE,
        MANAGE,
        JOIN,
        TRANSFORM,
        LAGS,
        SERVE,
        FEATURE,
        FEATURE_JOB_SETTING,
        SET_FEATURE_JOB,
        GROUPBY,
        DEPLOY,
        EXPLORE,
        INFO,
        LINEAGE,
        ENUMS,
        CLEANING_OPERATION,
        REQUEST_COLUMN,
        USER_DEFINED_FUNCTION,
    ]

    _custom_order_mapping = {
        0: _custom_root_level_order,
        1: _custom_second_level_order,
    }

    @classmethod
    def _get_items_for_level(cls, data: Mapping, custom_order: List[str]) -> Any:  # type: ignore[type-arg]
        """
        Helper method to get items sorted by a custom ordering.

        Parameters
        ----------
        data: Mapping
            The data to be sorted
        custom_order: List[str]
            The custom order

        Returns
        -------
        Any
            The sorted items
        """
        available_keys = set([item[0] for item in data.items() if item[0]])
        customized_keys = [key for key in custom_order if key in available_keys]
        extra_keys = sorted(available_keys - set(customized_keys))
        return ({key: data[key] for key in customized_keys + extra_keys}).items()

    @classmethod
    def _items(cls, data: Mapping, level: int) -> Iterable[Nav.Item]:  # type: ignore[type-arg]
        """
        Return nav section items sorted by title in alphabetical order

        Parameters
        ----------
        data: Mapping
            The data to be sorted
        level: int
            The level of the data

        Yields
        ------
        Iterable[Nav.Item]
            The sorted items
        """
        if level in cls._custom_order_mapping:
            items = cls._get_items_for_level(data, cls._custom_order_mapping[level])
        else:
            # sort by alphabetical order for other levels
            items_with_key = [item for item in data.items() if item[0]]
            items = sorted(items_with_key, key=lambda item: item[0])  # type: ignore[no-any-return]

        for key, value in items:
            yield cls.Item(level=level, title=key, filename=value.get(None))
            yield from cls._items(value, level + 1)
