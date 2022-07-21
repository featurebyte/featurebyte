"""
Feature and FeatureList classes
"""
from __future__ import annotations

from typing import Any, List, Tuple

import json
import time
from http import HTTPStatus

import pandas as pd
from beanie import PydanticObjectId

from featurebyte.api.feature_store import FeatureStore
from featurebyte.config import Configurations, Credentials
from featurebyte.core.frame import Frame
from featurebyte.core.generic import ProtectedColumnsQueryObject
from featurebyte.core.series import Series
from featurebyte.enum import SpecialColumnName
from featurebyte.exception import DuplicatedRecordException, RecordCreationException
from featurebyte.logger import logger
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_store import TableDetails
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
    def serving_names(self) -> list[str]:
        """
        Serving name columns

        Returns
        -------
        list[str]
        """
        serving_names: list[str] = self.inception_node.parameters["serving_names"]
        return serving_names

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
        point_in_time_and_serving_name: dict[str, Any],
        credentials: Credentials | None = None,
    ) -> pd.DataFrame:
        """
        Preview a FeatureGroup

        Parameters
        ----------
        point_in_time_and_serving_name : dict
            Dictionary consisting the point in time and serving names based on which the feature
            preview will be computed
        credentials: Credentials | None
            credentials to create a database session

        Returns
        -------
        pd.DataFrame
        """
        tic = time.time()
        self._validate_point_in_time_and_serving_name(point_in_time_and_serving_name)
        preview_sql = get_feature_preview_sql(
            graph=self.graph,
            node=self.node,
            point_in_time_and_serving_name=point_in_time_and_serving_name,
        )
        session = self.get_session(credentials)
        result = session.execute_query(preview_sql)
        elapsed = time.time() - tic
        logger.debug(f"Preview took {elapsed:.2f}s")
        return result

    def _validate_point_in_time_and_serving_name(
        self, point_in_time_and_serving_name: dict[str, Any]
    ) -> None:

        if not isinstance(point_in_time_and_serving_name, dict):
            raise ValueError("point_in_time_and_serving_name should be a dict")

        if SpecialColumnName.POINT_IN_TIME not in point_in_time_and_serving_name:
            raise KeyError(f"Point in time column not provided: {SpecialColumnName.POINT_IN_TIME}")

        if self.serving_names is not None:
            for col in self.serving_names:
                if col not in point_in_time_and_serving_name:
                    raise KeyError(f"Serving name not provided: {col}")


class Feature(FeatureQueryObject, Series, FeatureModel):
    """
    Feature class
    """

    # Although tabular_source is already defined in FeatureModel, here it is redefined so that
    # pydantic knows to deserialize the first element as a FeatureStore instead of a
    # FeatureStoreModel
    tabular_source: Tuple[FeatureStore, TableDetails]

    def _binary_op_series_params(self, other: Series | None = None) -> dict[str, Any]:
        """
        Parameters that will be passed to series-like constructor in _binary_op method


        Parameters
        ----------
        other: Series
            Other Series object

        Returns
        -------
        dict[str, Any]
        """
        event_data_ids = list(self.event_data_ids)
        if other is not None:
            event_data_ids.extend(getattr(other, "event_data_ids", []))
        return {"event_data_ids": list(set(event_data_ids))}

    def save(self) -> None:
        """
        Save feature to persistent as draft

        Raises
        ------
        DuplicatedRecordException
            When record with the same key exists at the persistent layer
        RecordCreationException
            When fail to save the event data (general failure)
        """
        client = Configurations().get_client()
        response = client.post(url="/feature", json=json.loads(self.json(by_alias=True)))
        if response.status_code != HTTPStatus.CREATED:
            if response.status_code == HTTPStatus.CONFLICT:
                raise DuplicatedRecordException(response)
            raise RecordCreationException(response)
        type(self).__init__(self, **response.json())


class FeatureGroup(FeatureQueryObject, Frame):
    """
    FeatureList class
    """

    _series_class = Feature

    event_data_ids: List[PydanticObjectId]

    @property
    def _getitem_frame_params(self) -> dict[str, Any]:
        """
        Parameters that will be passed to frame-like class constructor in __getitem__

        Returns
        -------
        dict[str, Any]
        """
        return {"event_data_ids": self.event_data_ids}

    @property
    def _getitem_series_params(self) -> dict[str, Any]:
        """
        Parameters that will be passed to series-like class constructor in __getitem__

        Returns
        -------
        dict[str, Any]
        """
        return {"event_data_ids": self.event_data_ids}

    def __getitem__(self, item: str | list[str] | Series) -> Series | Frame:
        if isinstance(item, list) and all(isinstance(elem, str) for elem in item):
            item = sorted(self.protected_columns.union(item))
        return super().__getitem__(item)

    def __setitem__(self, key: str, value: int | float | str | bool | Series) -> None:
        if key in self.protected_columns:
            raise ValueError(f"Entity identifier column '{key}' cannot be modified!")
        super().__setitem__(key, value)
