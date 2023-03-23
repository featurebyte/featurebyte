"""
Custom nav module
"""
from typing import Any, Iterable, List, Mapping

from mkdocs_gen_files import Nav  # type: ignore[attr-defined]


class BetaWave3Nav(Nav):
    """
    CustomItemsNav class overrides the mkdocs Nav to provide some custom ordering of menu items.

    In particular, we have a specific order for menu items provided via the _custom_root_level_order. Menu items not
    provided in this list will be placed after these items, and sorted alphabetically.
    """

    _custom_root_level_order = [
        "FeatureStore",
        "Catalog",
        "DataSource",
        "Table",
        "TableColumn",
        "Entity",
        "Relationship",
        "View",
        "ViewColumn",
        "Feature",
        "FeatureGroup",
        "FeatureList",
    ]

    _custom_second_level_order = [
        "Type",
        "Activate",
        "List",
        "Get",
        "Create",
        "Add Metadata",
        "Join",
        "Transform",
        "Serve",
        "Explore",
        "Info",
        "Lineage",
        "Update",
        "Version",
    ]

    _custom_order_mapping = {
        0: _custom_root_level_order,
        1: _custom_second_level_order,
    }

    @classmethod
    def _get_items_for_level(cls, data: Mapping, custom_order: List[str]) -> Any:
        """
        Helper method to get items sorted by a custom ordering.
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
            items = sorted(items_with_key, key=lambda item: item[0])  # type: ignore[assignment, no-any-return]

        for key, value in items:
            yield cls.Item(level=level, title=key, filename=value.get(None))
            yield from cls._items(value, level + 1)
