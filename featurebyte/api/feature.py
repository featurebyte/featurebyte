"""
Feature and FeatureList classes
"""
from __future__ import annotations

from typing import Any

import time
from http import HTTPStatus

import pandas as pd
from pydantic import Field

from featurebyte.config import Configurations, Credentials
from featurebyte.core.generic import ExtendedFeatureStoreModel, ProtectedColumnsQueryObject
from featurebyte.core.series import Series
from featurebyte.enum import SpecialColumnName
from featurebyte.exception import (
    DuplicatedRecordException,
    RecordCreationException,
    RecordRetrievalException,
)
from featurebyte.logger import logger
from featurebyte.models.feature import FeatureModel
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.feature_preview import get_feature_preview_sql


class Feature(ProtectedColumnsQueryObject, Series, FeatureModel):
    """
    Feature class
    """

    feature_store: ExtendedFeatureStoreModel = Field(exclude=True, allow_mutation=False)

    def __setattr__(self, key: str, value: Any) -> Any:
        """
        Custom __setattr__ to handle setting of special attributes such as name

        Parameters
        ----------
        key : str
            Key
        value : Any
            Value

        Raises
        ------
        ValueError
            if the name parameter is invalid

        Returns
        -------
        Any
        """
        if key != "name":
            return super().__setattr__(key, value)

        if value is None:
            raise ValueError("None is not a valid feature name")

        # For now, only allow updating name if the feature is unnamed (i.e. created on-the-fly by
        # combining different features)
        name = value
        node = self.node
        if node.type in {NodeType.PROJECT, NodeType.ALIAS}:
            if node.type == NodeType.PROJECT:
                existing_name = node.parameters["columns"][0]
            else:
                existing_name = node.parameters["name"]
            if name != existing_name:
                raise ValueError(f'Feature "{existing_name}" cannot be renamed to "{name}"')
            # FeatureGroup sets name unconditionally, so we allow this here
            return super().__setattr__(key, value)

        # Here, node could be any node resulting from series operations, e.g. DIV. This
        # validation was triggered by setting the name attribute of a Feature object
        new_node = self.graph.add_operation(
            node_type=NodeType.ALIAS,
            node_params={"name": name},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[node],
        )
        self.node = new_node
        return super().__setattr__(key, value)

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

    def preview(  # type: ignore[override]  # pylint: disable=arguments-renamed
        self,
        point_in_time_and_serving_name: dict[str, Any],
        credentials: Credentials | None = None,
    ) -> pd.DataFrame:
        """
        Preview a Feature

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
            nodes=[self.node],
            point_in_time_and_serving_name=point_in_time_and_serving_name,
        )
        session = self.get_session(credentials)
        result = session.execute_query(preview_sql)
        elapsed = time.time() - tic
        logger.debug(f"Preview took {elapsed:.2f}s")
        return result

    @classmethod
    def get(cls, name: str) -> Feature:
        """
        Retrieve feature from the persistent given feature name

        Parameters
        ----------
        name: str
            Feature name

        Returns
        -------
        Feature
            Feature object of the given feature name

        Raises
        ------
        RecordRetrievalException
            When the feature not found
        """
        client = Configurations().get_client()
        response = client.get(url="/feature", params={"name": name})
        if response.status_code == HTTPStatus.OK:
            response_dict = response.json()
            if response_dict["data"]:
                feature_dict = response_dict["data"][0]
                return Feature(**feature_dict)
        raise RecordRetrievalException(response, f'Feature (feature.name: "{name}") not found!')

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
        response = client.post(url="/feature", json=self.json_dict())
        if response.status_code != HTTPStatus.CREATED:
            if response.status_code == HTTPStatus.CONFLICT:
                raise DuplicatedRecordException(response)
            raise RecordCreationException(response)
        type(self).__init__(self, **response.json(), feature_store=self.feature_store)
