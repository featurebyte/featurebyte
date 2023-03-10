"""
Custom nav module
"""
from typing import Iterable, Mapping

from mkdocs_gen_files import Nav


class BetaWave3Nav(Nav):
    """
    CustomItemsNav class overrides the mkdocs Nav to provide some custom ordering of menu items.

    In particular, we have a specific order for menu items provided via the _custom_root_level_order. Menu items not
    provided in this list will be placed after these items, and sorted alphabetically.
    """

    _custom_root_level_order = [
        "Data",
        "DataColumn",
        "Entity",
        "Feature",
        "FeatureGroup",
        "FeatureList",
        "FeatureStore",
        "Relationship",
        "View",
        "ViewColumn",
        "Workspace",
    ]

    @classmethod
    def _items(cls, data: Mapping, level: int) -> Iterable[Nav.Item]:
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
        if level == 0:
            # use customized order for root level
            available_keys = set(data.keys())
            customized_keys = [key for key in cls._custom_root_level_order if key in available_keys]
            extra_keys = sorted(available_keys - set(customized_keys))
            items = ({key: data[key] for key in customized_keys + extra_keys}).items()
        else:
            # sort by alphabetical order for other levels
            items_with_key = [item for item in data.items() if item[0]]
            items = sorted(items_with_key, key=lambda item: item[0])

        for key, value in items:
            yield cls.Item(level=level, title=key, filename=value.get(None))
            yield from cls._items(value, level + 1)
