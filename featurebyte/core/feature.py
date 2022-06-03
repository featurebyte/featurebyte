"""
Feature and FeatureList classes
"""
from __future__ import annotations

from featurebyte.core.frame import Frame
from featurebyte.core.mixin import EventSourceFeatureOpsMixin
from featurebyte.core.protocol import HasInceptionNodeProtocol
from featurebyte.core.series import Series


class FeatureMixin(EventSourceFeatureOpsMixin):
    """
    FeatureMixin contains common properties & operations shared between FeatureList & Feature
    """

    @property
    def entity_identifiers(self: HasInceptionNodeProtocol) -> list[str] | None:
        """
        Entity identifiers column names
        """
        return self.inception_node.parameters.get("keys")

    @property
    def protected_columns(self) -> set[str]:
        """
        Special columns where its value should not be overridden

        Returns
        -------
        set[str]
        """
        columns = []
        if self.entity_identifiers:
            columns.extend(self.entity_identifiers)
        return set(columns)

    def publish(self) -> None:
        """
        Publish feature or feature list
        """
        raise NotImplementedError


class Feature(FeatureMixin, Series):
    """
    Feature class
    """

    def publish(self) -> None:
        """
        Publish feature
        """


class FeatureList(Frame, FeatureMixin):
    """
    FeatureList class
    """

    series_class = Feature

    def __getitem__(self, item: str | list[str] | Series) -> Series | Frame:
        if isinstance(item, list) and all(isinstance(elem, str) for elem in item):
            item = sorted(self.protected_columns.union(item))
        return super().__getitem__(item)

    def __setitem__(self, key: str, value: int | float | str | bool | Series) -> None:
        if key in self.protected_columns:
            raise ValueError("Not allow to override entity identifiers!")
        super().__setitem__(key, value)

    def publish(self) -> None:
        """
        Publish feature list
        """
