"""
Feature and FeatureList classes
"""
from __future__ import annotations

from typing import Any, ClassVar, Dict, List, Literal, Optional, Sequence, Tuple, Union, cast

import time
from http import HTTPStatus

import pandas as pd
from bson import ObjectId
from pydantic import Field, root_validator
from typeguard import typechecked

from featurebyte.api.api_object import ApiObject, SavableApiObject
from featurebyte.api.base_data import DataApiObject
from featurebyte.api.entity import Entity
from featurebyte.api.feature_job import FeatureJobMixin
from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.feature_validation_util import assert_is_lookup_feature
from featurebyte.common.descriptor import ClassInstanceMethodDescriptor
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.typing import Scalar, ScalarSequence
from featurebyte.common.utils import dataframe_from_json
from featurebyte.config import Configurations
from featurebyte.core.accessor.count_dict import CdAccessorMixin
from featurebyte.core.generic import ProtectedColumnsQueryObject
from featurebyte.core.series import Series
from featurebyte.exception import RecordCreationException, RecordRetrievalException
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.logger import logger
from featurebyte.models.base import PydanticObjectId, VersionIdentifier
from featurebyte.models.event_data import FeatureJobSetting
from featurebyte.models.feature import (
    DefaultVersionMode,
    FeatureModel,
    FeatureNamespaceModel,
    FeatureReadiness,
    FrozenFeatureModel,
    FrozenFeatureNamespaceModel,
)
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.tile import TileSpec
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.generic import (
    AliasNode,
    GroupbyNode,
    ItemGroupbyNode,
    ProjectNode,
)
from featurebyte.schema.feature import FeatureCreate, FeaturePreview, FeatureSQL, FeatureUpdate
from featurebyte.schema.feature_namespace import FeatureNamespaceUpdate


class FeatureNamespace(FrozenFeatureNamespaceModel, ApiObject):
    """
    FeatureNamespace class
    """

    # class variables
    _route = "/feature_namespace"
    _update_schema_class = FeatureNamespaceUpdate
    _list_schema = FeatureNamespaceModel
    _get_schema = FeatureNamespaceModel
    _list_fields = [
        "name",
        "dtype",
        "readiness",
        "online_enabled",
        "data",
        "entities",
        "created_at",
    ]
    _list_foreign_keys = [
        ("entity_ids", Entity, "entities"),
        ("tabular_data_ids", DataApiObject, "data"),
    ]

    @property
    def feature_ids(self) -> List[PydanticObjectId]:
        """
        List of feature IDs from the same feature namespace

        Returns
        -------
        List[PydanticObjectId]
        """
        return self.cached_model.feature_ids

    @property
    def online_enabled_feature_ids(self) -> List[PydanticObjectId]:
        """
        List of online-enabled feature IDs from the same feature namespace

        Returns
        -------
        List[PydanticObjectId]
        """
        return self.cached_model.online_enabled_feature_ids

    @property
    def readiness(self) -> FeatureReadiness:
        """
        Feature readiness of the default feature of this feature namespace

        Returns
        -------
        FeatureReadiness
        """
        return self.cached_model.readiness

    @property
    def default_feature_id(self) -> PydanticObjectId:
        """
        Default feature ID of this feature namespace

        Returns
        -------
        PydanticObjectId
        """
        return self.cached_model.default_feature_id

    @property
    def default_version_mode(self) -> DefaultVersionMode:
        """
        Default feature namespace version mode of this feature namespace

        Returns
        -------
        DefaultVersionMode
        """
        return self.cached_model.default_version_mode

    @classmethod
    def _post_process_list(cls, item_list: pd.DataFrame) -> pd.DataFrame:
        features = super()._post_process_list(item_list)
        # add online_enabled
        features["online_enabled"] = features[
            ["default_feature_id", "online_enabled_feature_ids"]
        ].apply(lambda row: row[0] in row[1], axis=1)
        return features

    @classmethod
    def list(
        cls,
        include_id: Optional[bool] = False,
        entity: Optional[str] = None,
        data: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        List saved features

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list
        entity: Optional[str]
            Name of entity used to filter results
        data: Optional[str]
            Name of data used to filter results

        Returns
        -------
        pd.DataFrame
            Table of features
        """
        feature_list = super().list(include_id=include_id)
        if entity:
            feature_list = feature_list[
                feature_list.entities.apply(lambda entities: entity in entities)
            ]
        if data:
            feature_list = feature_list[
                feature_list.data.apply(lambda data_list: data in data_list)
            ]
        return feature_list


class Feature(
    ProtectedColumnsQueryObject,
    Series,
    FrozenFeatureModel,
    SavableApiObject,
    CdAccessorMixin,
    FeatureJobMixin,
):  # pylint: disable=too-many-public-methods
    """
    Feature class
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["Feature"], proxy_class="featurebyte.Feature")

    # pydantic instance variable (public)
    feature_store: FeatureStoreModel = Field(exclude=True, allow_mutation=False)

    # class variables
    _route = "/feature"
    _update_schema_class = FeatureUpdate
    _list_schema = FeatureModel
    _get_schema = FeatureModel
    _list_fields = [
        "name",
        "version",
        "dtype",
        "readiness",
        "online_enabled",
        "data",
        "entities",
        "created_at",
    ]
    _list_foreign_keys = [
        ("entity_ids", Entity, "entities"),
        ("tabular_data_ids", DataApiObject, "data"),
    ]

    def _get_init_params_from_object(self) -> dict[str, Any]:
        return {"feature_store": self.feature_store}

    def _get_create_payload(self) -> dict[str, Any]:
        data = FeatureCreate(**self.json_dict())
        return data.json_dict()

    def _get_feature_tiles_specs(self) -> List[Tuple[str, List[TileSpec]]]:
        tile_specs = ExtendedFeatureModel(**self.dict()).tile_specs
        return [(str(self.name), tile_specs)] if tile_specs else []

    @root_validator(pre=True)
    @classmethod
    def _set_feature_store(cls, values: dict[str, Any]) -> dict[str, Any]:
        if "feature_store" not in values:
            tabular_source = values.get("tabular_source")
            if isinstance(tabular_source, dict):
                feature_store_id = TabularSource(**tabular_source).feature_store_id
                values["feature_store"] = FeatureStore.get_by_id(id=feature_store_id)
        return values

    @classmethod
    def list(
        cls,
        include_id: Optional[bool] = False,
        entity: Optional[str] = None,
        data: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        List saved features

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list
        entity: Optional[str]
            Name of entity used to filter results
        data: Optional[str]
            Name of data used to filter results

        Returns
        -------
        pd.DataFrame
            Table of features
        """
        return FeatureNamespace.list(include_id=include_id, entity=entity, data=data)

    @classmethod
    def _post_process_list(cls, item_list: pd.DataFrame) -> pd.DataFrame:
        features = super()._post_process_list(item_list)
        # convert version strings
        features["version"] = features["version"].apply(
            lambda version: VersionIdentifier(**version).to_str()
        )
        return features

    @classmethod
    def _list_versions(
        cls,
        include_id: Optional[bool] = False,
        feature_list_id: Optional[ObjectId] = None,
        entity: Optional[str] = None,
        data: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        List saved features versions

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list
        feature_list_id: Optional[ObjectId] = None,
            Include only features in specified feature list
        entity: Optional[str]
            Name of entity used to filter results
        data: Optional[str]
            Name of data used to filter results

        Returns
        -------
        pd.DataFrame
            Table of features
        """
        params = {}
        if feature_list_id:
            params = {"feature_list_id": str(feature_list_id)}
        feature_list = cls._list(include_id=include_id, params=params)
        if entity:
            feature_list = feature_list[
                feature_list.entities.apply(lambda entities: entity in entities)
            ]
        if data:
            feature_list = feature_list[
                feature_list.data.apply(lambda data_list: data in data_list)
            ]
        return feature_list

    def _list_versions_with_same_name(self, include_id: bool = False) -> pd.DataFrame:
        """
        List feature versions with the same name

        Parameters
        ----------
        include_id: bool
            Whether to include id in the list

        Returns
        -------
        pd.DataFrame
            Table of features with the same name
        """
        return self._list(include_id=include_id, params={"name": self.name})

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
        entity_ids: list[str] = []
        for node in self.graph.iterate_nodes(target_node=self.node, node_type=NodeType.GROUPBY):
            entity_ids.extend(cast(GroupbyNode, node).parameters.keys)
        for node in self.graph.iterate_nodes(
            target_node=self.node, node_type=NodeType.ITEM_GROUPBY
        ):
            entity_ids.extend(cast(ItemGroupbyNode, node).parameters.keys)
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
    def readiness(self) -> FeatureReadiness:
        """
        Feature readiness of this feature

        Returns
        -------
        FeatureReadiness
        """
        try:
            return self.cached_model.readiness
        except RecordRetrievalException:
            return FeatureReadiness.DRAFT

    @property
    def online_enabled(self) -> bool:
        """
        Whether this feature is oneline-enabled or not, an online-enabled feature is a feature used by at least
        one deployed feature list.

        Returns
        -------
        bool
        """
        try:
            return self.cached_model.online_enabled
        except RecordRetrievalException:
            return False

    @property
    def deployed_feature_list_ids(self) -> List[PydanticObjectId]:
        """
        IDs of deployed feature lists which use this feature

        Returns
        -------
        List[PydanticObjectId]
        """
        try:
            return self.cached_model.deployed_feature_list_ids
        except RecordRetrievalException:
            return []

    @property
    def is_default(self) -> bool:
        """
        Check whether current feature is the default one or not

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

    @property
    def is_time_based(self) -> bool:
        """
        Whether the feature is a time based one.

        We check for this by looking to see by looking at the operation structure to see if it's time-based.

        Returns
        -------
        bool
            True if the feature is time based, False otherwise.
        """
        operation_structure = self.extract_operation_structure()
        return operation_structure.is_time_based

    def binary_op_series_params(self, other: Scalar | Series | ScalarSequence) -> dict[str, Any]:
        """
        Parameters that will be passed to series-like constructor in _binary_op method


        Parameters
        ----------
        other: other: Scalar | Series | ScalarSequence
            Other object

        Returns
        -------
        dict[str, Any]
        """
        tabular_data_ids = set(self.tabular_data_ids)
        entity_ids = set(self.entity_ids)
        if isinstance(other, Series):
            tabular_data_ids = tabular_data_ids.union(getattr(other, "tabular_data_ids", []))
            entity_ids = entity_ids.union(getattr(other, "entity_ids", []))
        return {"tabular_data_ids": sorted(tabular_data_ids), "entity_ids": sorted(entity_ids)}

    def unary_op_series_params(self) -> dict[str, Any]:
        return {"tabular_data_ids": self.tabular_data_ids, "entity_ids": self.entity_ids}

    def _get_pruned_feature_model(self) -> FeatureModel:
        """
        Get pruned model of feature

        Returns
        -------
        FeatureModel
        """
        pruned_graph, mapped_node = self.extract_pruned_graph_and_node()
        feature_dict = self.dict()
        feature_dict["graph"] = pruned_graph
        feature_dict["node_name"] = mapped_node.name
        return FeatureModel(**feature_dict)

    @typechecked
    def preview(
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
            Materialized historical features.

            **Note**: `POINT_IN_TIME` values will be converted to UTC time.

        Raises
        ------
        RecordRetrievalException
            Failed to preview feature
        """
        tic = time.time()

        feature = self._get_pruned_feature_model()

        payload = FeaturePreview(
            feature_store_name=self.feature_store.name,
            graph=feature.graph,
            node_name=feature.node_name,
            point_in_time_and_serving_name=point_in_time_and_serving_name,
        )

        client = Configurations().get_client()
        response = client.post(url="/feature/preview", json=payload.json_dict())
        if response.status_code != HTTPStatus.OK:
            raise RecordRetrievalException(response)
        result = response.json()

        elapsed = time.time() - tic
        logger.debug(f"Preview took {elapsed:.2f}s")
        return dataframe_from_json(result)

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

        object_dict = response.json()
        self._update_cache(object_dict)  # update object cache store
        return Feature(**object_dict, **self._get_init_params_from_object(), saved=True)

    @typechecked
    def update_readiness(
        self, readiness: Literal[tuple(FeatureReadiness)]  # type: ignore[misc]
    ) -> None:
        """
        Update feature readiness

        Parameters
        ----------
        readiness: Literal[tuple(FeatureReadiness)]
            Feature readiness level
        """
        self.update(update_payload={"readiness": str(readiness)}, allow_update_local=False)

    @typechecked
    def update_default_version_mode(
        self, default_version_mode: Literal[tuple(DefaultVersionMode)]  # type: ignore[misc]
    ) -> None:
        """
        Update feature default version mode

        Parameters
        ----------
        default_version_mode: Literal[tuple(DefaultVersionMode)]
            Feature default version mode
        """
        self.feature_namespace.update(
            update_payload={"default_version_mode": DefaultVersionMode(default_version_mode).value},
            allow_update_local=False,
        )

    @property
    def sql(self) -> str:
        """
        Get Feature SQL

        Returns
        -------
        str
            Feature SQL

        Raises
        ------
        RecordRetrievalException
            Failed to get feature SQL
        """
        feature = self._get_pruned_feature_model()

        payload = FeatureSQL(
            graph=feature.graph,
            node_name=feature.node_name,
        )

        client = Configurations().get_client()
        response = client.post("/feature/sql", json=payload.json_dict())
        if response.status_code != HTTPStatus.OK:
            raise RecordRetrievalException(response)

        return cast(
            str,
            response.json(),
        )

    def validate_isin_operation(
        self, other: Union[Series, Sequence[Union[bool, int, float, str]]]
    ) -> None:
        """
        Validates whether a feature is a lookup feature

        Parameters
        ----------
        other: Union[Series, Sequence[Union[bool, int, float, str]]]
            other
        """
        assert_is_lookup_feature(self.node_types_lineage)

    # descriptors
    list_versions: ClassVar[ClassInstanceMethodDescriptor] = ClassInstanceMethodDescriptor(
        class_method=_list_versions,
        instance_method=_list_versions_with_same_name,
    )
