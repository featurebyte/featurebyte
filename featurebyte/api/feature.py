"""
Feature and FeatureList classes
"""
from __future__ import annotations

import logging
import time

import pandas as pd

from featurebyte.core.frame import Frame
from featurebyte.core.generic import ProtectedColumnsQueryObject
from featurebyte.core.series import Series
from featurebyte.query_graph.feature_compute import get_feature_preview_sql

logger = logging.getLogger("featurebyte")


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
    def entity_identifiers(self) -> list[str]:
        """
        Entity identifiers column names

        Returns
        -------
        list[str]
        """
        entity_ids: list[str] = self.inception_node.parameters["keys"]
        return entity_ids

    @property
    def inherited_columns(self) -> set[str]:
        """
        Special columns set which will be automatically added to the object of same class
        derived from current object

        Returns
        -------
        set[str]
        """
        return set(self.entity_identifiers)

    def preview(self, *args, **kwargs) -> pd.DataFrame | None:
        """
        Preview a FeatureGroup

        Parameters
        ----------
        point_in_time_and_entity_id : dict
            Dictionary consisting the point in time and entity ids based on which the feature
            preview will be computed
        credentials: Credentials | None
            credentials to create a database session
        """
        tic = time.time()
        point_in_time_and_entity_id = args[0]
        preview_sql = get_feature_preview_sql(
            graph=self.graph,
            node=self.node,
            entity_columns=self.entity_identifiers,
            point_in_time_and_entity_id=point_in_time_and_entity_id,
        )
        session = self.get_session(kwargs.get("credentials"))
        result = session.execute_query(preview_sql)
        elapsed = time.time() - tic
        logger.debug(f"Preview took {elapsed:.2f}s")
        return result


class Feature(FeatureQueryObject, Series):
    """
    Feature class
    """


class FeatureGroup(FeatureQueryObject, Frame):
    """
    FeatureList class
    """

    _series_class = Feature

    def __getitem__(self, item: str | list[str] | Series) -> Series | Frame:
        if isinstance(item, list) and all(isinstance(elem, str) for elem in item):
            item = sorted(self.protected_columns.union(item))
        return super().__getitem__(item)

    def __setitem__(self, key: str, value: int | float | str | bool | Series) -> None:
        if key in self.protected_columns:
            raise ValueError(f"Entity identifier column '{key}' cannot be modified!")
        super().__setitem__(key, value)
