"""
Feature and FeatureList classes
"""
from __future__ import annotations

from featurebyte.core.frame import Frame
from featurebyte.core.mixin import EventSourceFeatureOpsMixin, WithProtectedColumnsFrameMixin
from featurebyte.core.protocol import ProtectedPropertiesProtocol
from featurebyte.core.series import Series


class FeatureMixin(EventSourceFeatureOpsMixin):
    """
    FeatureMixin contains common properties & operations shared between FeatureList & Feature
    """

    @property
    def entity_identifiers(self: ProtectedPropertiesProtocol) -> list[str] | None:
        """
        Entity identifiers column names
        """
        return self.inception_node.parameters.get("keys")

    @property
    def protected_columns(self) -> set[str]:
        """
        Special columns set where values of these columns should not be overridden

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


class FeatureList(Frame, FeatureMixin, WithProtectedColumnsFrameMixin):
    """
    FeatureList class
    """

    series_class = Feature

    def publish(self) -> None:
        """
        Publish feature list
        """
