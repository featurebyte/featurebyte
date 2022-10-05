"""
Feature and FeatureList classes
"""
from __future__ import annotations

from typing import Any, Dict, List

import time
from http import HTTPStatus

import pandas as pd
from pydantic import Field, root_validator
from typeguard import typechecked

from featurebyte.api.api_object import ApiGetObject, ApiObject
from featurebyte.config import Configurations
from featurebyte.core.accessor.count_dict import CdAccessorMixin
from featurebyte.core.generic import ExtendedFeatureStoreModel, ProtectedColumnsQueryObject
from featurebyte.core.series import Series
from featurebyte.exception import RecordCreationException, RecordRetrievalException
from featurebyte.logger import logger
from featurebyte.models.event_data import FeatureJobSetting
from featurebyte.models.feature import (
    DefaultVersionMode,
    FeatureModel,
    FeatureNamespaceModel,
    FeatureReadiness,
)
from featurebyte.models.feature_store import TabularSource
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.node.generic import AliasNode, GroupbyNode, ProjectNode
from featurebyte.schema.feature import FeatureCreate, FeaturePreview
from featurebyte.service.preview import PreviewService
from featurebyte.utils.credential import get_credential


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

    @typechecked
    def preview(  # type: ignore[override]  # pylint: disable=arguments-renamed
        self,
        point_in_time_and_serving_name: Dict[str, Any],
    ) -> pd.DataFrame:
        """
        Preview a Feature

        Parameters
        ----------
        point_in_time_and_serving_name : Dict[str, Any]
            Dictionary consisting the point in time and serving names based on which the feature
            preview will be computed

        Returns
        -------
        pd.DataFrame

        Raises
        ------
        RecordRetrievalException
            Failed to preview feature
        """
        tic = time.time()

        pruned_graph, mapped_node = self.extract_pruned_graph_and_node()
        feature_dict = self.dict()
        feature_dict["graph"] = pruned_graph
        feature_dict["node"] = mapped_node

        payload = FeaturePreview(
            feature_store_name=self.feature_store.name,
            feature=FeatureModel(**feature_dict),
            point_in_time_and_serving_name=point_in_time_and_serving_name,
        )

        if self.feature_store.details.is_local_source:
            result = PreviewService(user=None, persistent=None).preview_feature(
                feature_preview=payload, get_credential=get_credential
            )
        else:
            client = Configurations().get_client()
            response = client.post(url="/feature/preview", json=payload.json_dict())
            if response.status_code != HTTPStatus.OK:
                raise RecordRetrievalException(response)
            result = response.json()

        elapsed = time.time() - tic
        logger.debug(f"Preview took {elapsed:.2f}s")
        return pd.read_json(result, orient="table", convert_dates=False)

    @typechecked
    def create_new_version(self, feature_job_setting: FeatureJobSetting) -> Feature:
        """
        Create new feature version

        Parameters
        ----------
        feature_job_setting: FeatureJobSetting
            New feature job setting

        Returns
        -------
        Feature

        Raises
        ------
        RecordCreationException
            When failed to save a new version
        """
        client = Configurations().get_client()
        response = client.post(
            url=self._route,
            json={
                "source_feature_id": str(self.id),
                "feature_job_setting": feature_job_setting.dict(),
            },
        )
        if response.status_code != HTTPStatus.CREATED:
            raise RecordCreationException(response=response)
        return Feature(**response.json(), **self._get_init_params_from_object(), saved=True)
