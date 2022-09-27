"""
Feature and FeatureList classes
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import time
from http import HTTPStatus

import pandas as pd
from pydantic import Field, root_validator
from typeguard import typechecked

from featurebyte.api.api_object import ApiGetObject, ApiObject
from featurebyte.config import Configurations, Credentials
from featurebyte.core.accessor.count_dict import CdAccessorMixin
from featurebyte.core.generic import ExtendedFeatureStoreModel, ProtectedColumnsQueryObject
from featurebyte.core.series import Series
from featurebyte.enum import SpecialColumnName
from featurebyte.logger import logger
from featurebyte.models.feature import (
    DefaultVersionMode,
    FeatureModel,
    FeatureNamespaceModel,
    FeatureReadiness,
)
from featurebyte.models.feature_store import TabularSource
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.feature_preview import get_feature_preview_sql
from featurebyte.query_graph.node.generic import AliasNode, GroupbyNode, ProjectNode
from featurebyte.schema.feature import FeatureCreate


class FeatureNamespace(FeatureNamespaceModel, ApiGetObject):
    """
    FeatureNamespace class
    """

    # class variables
    _route = "/feature_namespace"


class Feature(
    ProtectedColumnsQueryObject,
    Series,
    FeatureModel,
    ApiObject,
    CdAccessorMixin,
):
    """
    Feature class
    """

    feature_store: ExtendedFeatureStoreModel = Field(exclude=True, allow_mutation=False)

    # class variables
    _route = "/feature"

    def _get_init_params_from_object(self) -> dict[str, Any]:
        return {"feature_store": self.feature_store}

    def _get_create_payload(self) -> dict[str, Any]:
        data = FeatureCreate(**self.json_dict())
        return data.json_dict()

    @root_validator(pre=True)
    @classmethod
    def _set_feature_store(cls, values: dict[str, Any]) -> dict[str, Any]:
        if "feature_store" not in values:
            tabular_source = values.get("tabular_source")
            if isinstance(tabular_source, dict):
                tabular_source_obj = TabularSource(**tabular_source)
                client = Configurations().get_client()
                feature_store_id = tabular_source_obj.feature_store_id
                feature_store_response = client.get(url=f"/feature_store/{feature_store_id}")
                if feature_store_response.status_code == HTTPStatus.OK:
                    values["feature_store"] = ExtendedFeatureStoreModel(
                        **feature_store_response.json()
                    )
        return values

    @classmethod
    def list(cls) -> List[str]:
        return FeatureNamespace.list()

    @typechecked
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
            if isinstance(node, ProjectNode):
                existing_name = node.parameters.columns[0]
            else:
                assert isinstance(node, AliasNode)
                existing_name = node.parameters.name  # type: ignore
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
        self.node_name = new_node.name
        return super().__setattr__(key, value)

    @property
    def protected_attributes(self) -> List[str]:
        """
        List of protected attributes used to extract protected_columns

        Returns
        -------
        List[str]
        """
        return ["entity_identifiers"]

    @property
    def entity_identifiers(self) -> List[str]:
        """
        Entity identifiers column names

        Returns
        -------
        List[str]
        """
        assert isinstance(self.inception_node, GroupbyNode)
        entity_ids: list[str] = self.inception_node.parameters.keys  # type: ignore
        return entity_ids

    @property
    def serving_names(self) -> List[str]:
        """
        Serving name columns

        Returns
        -------
        List[str]
        """
        assert isinstance(self.inception_node, GroupbyNode)
        serving_names: list[str] = self.inception_node.parameters.serving_names
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

    @property
    def feature_namespace(self) -> FeatureNamespace:
        """
        FeatureNamespace object of current feature

        Returns
        -------
        FeatureNamespace
        """
        return FeatureNamespace.get_by_id(id=self.feature_namespace_id)

    @property
    def is_default(self) -> bool:
        """
        Check whether current feature version is the default one or not

        Returns
        -------
        bool
        """
        return self.id == self.feature_namespace.default_feature_id

    @property
    def default_version_mode(self) -> DefaultVersionMode:
        """
        Retrieve default version mode of current feature namespace

        Returns
        -------
        DefaultVersionMode
        """
        return self.feature_namespace.default_version_mode

    @property
    def default_readiness(self) -> FeatureReadiness:
        """
        Feature readiness of the default feature version

        Returns
        -------
        FeatureReadiness
        """
        return self.feature_namespace.readiness

    def binary_op_series_params(self, other: Series | None = None) -> dict[str, Any]:
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
        event_data_ids = set(self.event_data_ids)
        entity_ids = set(self.entity_ids)
        if other is not None:
            event_data_ids = event_data_ids.union(getattr(other, "event_data_ids", []))
            entity_ids = entity_ids.union(getattr(other, "entity_ids", []))
        return {"event_data_ids": sorted(event_data_ids), "entity_ids": sorted(entity_ids)}

    def unary_op_series_params(self) -> dict[str, Any]:
        return {"event_data_ids": self.event_data_ids, "entity_ids": self.entity_ids}

    def _validate_point_in_time_and_serving_name(
        self, point_in_time_and_serving_name: dict[str, Any]
    ) -> None:

        if SpecialColumnName.POINT_IN_TIME not in point_in_time_and_serving_name:
            raise KeyError(f"Point in time column not provided: {SpecialColumnName.POINT_IN_TIME}")

        if self.serving_names is not None:
            for col in self.serving_names:
                if col not in point_in_time_and_serving_name:
                    raise KeyError(f"Serving name not provided: {col}")

    @typechecked
    def preview_sql(  # type: ignore[override]  # pylint: disable=arguments-renamed
        self,
        point_in_time_and_serving_name: Dict[str, Any],
    ) -> str:
        """
        Generate SQL query to preview a feature

        Parameters
        ----------
        point_in_time_and_serving_name : Dict[str, Any]
            Dictionary consisting the point in time and serving names based on which the feature
            preview will be computed

        Returns
        -------
        str
        """
        self._validate_point_in_time_and_serving_name(point_in_time_and_serving_name)
        pruned_graph, mapped_node = self.extract_pruned_graph_and_node()
        return get_feature_preview_sql(
            graph=pruned_graph,
            nodes=[mapped_node],
            point_in_time_and_serving_name=point_in_time_and_serving_name,
        )

    @typechecked
    def preview(  # type: ignore[override]  # pylint: disable=arguments-renamed
        self,
        point_in_time_and_serving_name: Dict[str, Any],
        credentials: Optional[Credentials] = None,
    ) -> pd.DataFrame:
        """
        Preview a Feature

        Parameters
        ----------
        point_in_time_and_serving_name : Dict[str, Any]
            Dictionary consisting the point in time and serving names based on which the feature
            preview will be computed
        credentials: Optional[Credentials]
            credentials to create a database session

        Returns
        -------
        pd.DataFrame
        """
        tic = time.time()
        session = self.get_session(credentials)
        result = session.execute_query(
            self.preview_sql(point_in_time_and_serving_name=point_in_time_and_serving_name)
        )
        elapsed = time.time() - tic
        logger.debug(f"Preview took {elapsed:.2f}s")
        return result
