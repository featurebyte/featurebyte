"""
Feature and FeatureList classes
"""
from __future__ import annotations

from featurebyte.core.frame import Frame
from featurebyte.core.mixin import WithProtectedColumnsFrameMixin, WithProtectedColumnsMixin
from featurebyte.core.series import Series


class FeatureMixin(WithProtectedColumnsMixin):
    """
    FeatureMixin contains common properties & operations shared between FeatureList & Feature
    """

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
    def entity_identifiers(self: WithProtectedColumnsMixin) -> list[str] | None:
        """
        Entity identifiers column names
        """
        return self.inception_node.parameters.get("keys")

    def publish(self) -> None:
        """
        Publish feature or feature list
        """
        raise NotImplementedError


class Feature(Series, FeatureMixin, WithProtectedColumnsMixin):
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
