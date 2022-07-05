"""
Feature and FeatureList classes
"""
from __future__ import annotations

from typing import Any

import time

import pandas as pd

from featurebyte.config import Credentials
from featurebyte.core.frame import Frame
from featurebyte.core.generic import ProtectedColumnsQueryObject
from featurebyte.core.series import Series
from featurebyte.enum import SpecialColumnName
from featurebyte.logger import logger
from featurebyte.models.feature import FeatureModel
from featurebyte.query_graph.feature_preview import get_feature_preview_sql


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

    def preview(  # type: ignore[override]  # pylint: disable=arguments-renamed
        self,
        point_in_time_and_entity_id: dict[str, Any],
        credentials: Credentials | None = None,
    ) -> pd.DataFrame:
        """
        Preview a FeatureGroup

        Parameters
        ----------
        point_in_time_and_entity_id : dict
            Dictionary consisting the point in time and entity ids based on which the feature
            preview will be computed
        credentials: Credentials | None
            credentials to create a database session

        Returns
        -------
        pd.DataFrame
        """
        tic = time.time()
        self._validate_point_in_time_and_entity_id(point_in_time_and_entity_id)
        preview_sql = get_feature_preview_sql(
            graph=self.graph,
            node=self.node,
            point_in_time_and_entity_id=point_in_time_and_entity_id,
        )
        session = self.get_session(credentials)
        result = session.execute_query(preview_sql)
        elapsed = time.time() - tic
        logger.debug(f"Preview took {elapsed:.2f}s")
        return result

    def _validate_point_in_time_and_entity_id(
        self, point_in_time_and_entity_id: dict[str, Any]
    ) -> None:

        if not isinstance(point_in_time_and_entity_id, dict):
            raise ValueError("point_in_time_and_entity_id should be a dict")

        if SpecialColumnName.POINT_IN_TIME not in point_in_time_and_entity_id:
            raise KeyError(f"Point in time column not provided: {SpecialColumnName.POINT_IN_TIME}")

        if self.entity_identifiers is not None:
            for col in self.entity_identifiers:
                if col not in point_in_time_and_entity_id:
                    raise KeyError(f"Entity column not provided: {col}")


class Feature(FeatureQueryObject, Series, FeatureModel):
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
