"""
Feature and FeatureList classes
"""
from __future__ import annotations

from featurebyte.core.frame import Frame
from featurebyte.core.generic import ProtectedColumnsQueryObject
from featurebyte.core.series import Series


class FeatureQueryObject(ProtectedColumnsQueryObject):
    """
    FeatureMixin contains common properties & operations shared between FeatureList & Feature
    """

    def __repr__(self) -> str:
        return f"{type(self).__name__}(node.name={self.node.name}, entity_identifiers={self.entity_identifiers})"

    @property
    def protected_attributes(self) -> list[str]:
        """
        List of protected attributes used to extract protected_columns

        Returns
        -------
        list[str]
        """
        return ["entity_identifiers"]

    @property
    def entity_identifiers(self) -> list[str] | None:
        """
        Entity identifiers column names

        Returns
        -------
        list[str]
        """
        return self.inception_node.parameters.get("keys")


class Feature(FeatureQueryObject, Series):
    """
    Feature class
    """


class FeatureList(FeatureQueryObject, Frame):
    """
    FeatureList class
    """

    series_class = Feature

    def __getitem__(self, item: str | list[str] | Series) -> Series | Frame:
        # pylint: disable=R0801 (duplicate-code)
        if isinstance(item, list) and all(isinstance(elem, str) for elem in item):
            item = sorted(self.protected_columns.union(item))
        return super().__getitem__(item)

    def __setitem__(self, key: str, value: int | float | str | bool | Series) -> None:
        if key in self.protected_columns:
            raise ValueError(f"Not allow to override entity identifier column '{key}'!")
        super().__setitem__(key, value)
