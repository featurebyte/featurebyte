"""
Feature and FeatureList classes
"""
from __future__ import annotations

from typing import Any, ClassVar, List, Literal, Optional, Sequence, Tuple, Union, cast

import time
from http import HTTPStatus

import pandas as pd
from bson import ObjectId
from pydantic import Field, root_validator
from typeguard import typechecked

from featurebyte.api.api_object import ApiObject, ForeignKeyMapping, SavableApiObject
from featurebyte.api.base_table import TableApiObject
from featurebyte.api.entity import Entity
from featurebyte.api.feature_job import FeatureJobMixin
from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.feature_validation_util import assert_is_lookup_feature
from featurebyte.api.table import Table
from featurebyte.common.descriptor import ClassInstanceMethodDescriptor
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.typing import Scalar, ScalarSequence
from featurebyte.common.utils import (
    CodeStr,
    convert_to_list_of_strings,
    dataframe_from_json,
    enforce_observation_set_row_order,
)
from featurebyte.config import Configurations
from featurebyte.core.accessor.count_dict import CdAccessorMixin
from featurebyte.core.generic import ProtectedColumnsQueryObject
from featurebyte.core.series import FrozenSeries, Series
from featurebyte.exception import RecordCreationException, RecordRetrievalException
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.logger import logger
from featurebyte.models.base import PydanticObjectId, VersionIdentifier
from featurebyte.models.feature import (
    DefaultVersionMode,
    FeatureModel,
    FeatureReadiness,
    FrozenFeatureModel,
    FrozenFeatureNamespaceModel,
)
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.relationship_analysis import derive_primary_entity
from featurebyte.models.tile import TileSpec
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.model.feature_job_setting import TableFeatureJobSetting
from featurebyte.query_graph.node.cleaning_operation import TableCleaningOperation
from featurebyte.query_graph.node.generic import (
    AliasNode,
    GroupByNode,
    ItemGroupbyNode,
    ProjectNode,
)
from featurebyte.schema.feature import (
    FeatureCreate,
    FeatureModelResponse,
    FeaturePreview,
    FeatureSQL,
    FeatureUpdate,
)
from featurebyte.schema.feature_namespace import (
    FeatureNamespaceModelResponse,
    FeatureNamespaceUpdate,
)

# pylint: disable=too-many-lines


class FeatureNamespace(FrozenFeatureNamespaceModel, ApiObject):
    """
    FeatureNamespace represents a Feature set, in which all the features in the set have the same name. The different
    elements typically refer to different versions of a Feature.
    """

    # class variables
    _route = "/feature_namespace"
    _update_schema_class = FeatureNamespaceUpdate
    _list_schema = FeatureNamespaceModelResponse
    _get_schema = FeatureNamespaceModelResponse
    _list_fields = [
        "name",
        "dtype",
        "readiness",
        "online_enabled",
        "tables",
        "primary_tables",
        "entities",
        "primary_entities",
        "created_at",
    ]
    _list_foreign_keys = [
        ForeignKeyMapping("entity_ids", Entity, "entities"),
        ForeignKeyMapping("tabular_data_ids", TableApiObject, "tables"),
        ForeignKeyMapping("primary_entity_ids", Entity, "primary_entities"),
        ForeignKeyMapping("primary_table_ids", TableApiObject, "primary_tables"),
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
        primary_entity: Optional[Union[str, List[str]]] = None,
        primary_table: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """
        List saved features

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list
        primary_entity: Optional[Union[str, List[str]]]
            Name of entity used to filter results. If multiple entities are provided, the filtered results will
            contain features that are associated with all the entities.
        primary_table: Optional[Union[str, List[str]]]
            Name of table used to filter results. If multiple tables are provided, the filtered results will
            contain features that are associated with all the tables.

        Returns
        -------
        pd.DataFrame
            Table of features
        """
        feature_list = super().list(include_id=include_id)
        target_entities = convert_to_list_of_strings(primary_entity)
        target_tables = convert_to_list_of_strings(primary_table)

        if target_entities:
            feature_list = feature_list[
                feature_list.primary_entities.apply(
                    lambda entities: all(entity in entities for entity in target_entities)
                )
            ]
        if target_tables:
            feature_list = feature_list[
                feature_list.primary_tables.apply(
                    lambda tables: all(table in tables for table in target_tables)
                )
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
    A feature is input data that is used to train Machine Learning models and compute predictions.

    These features can sometimes be derived from attributes already present in the source tables. For instance, a
    customer churn model can use features obtained directly from a customer profile table, such as age, gender,
    income, and location.

    However, in most cases, features are created by applying a series of row transformations, joins, filters, and
    aggregates. For instance, a customer churn model may utilize aggregate features that reflect the customer's
    account details over a given period, such as the entropy of product types purchased over the past 12 weeks,
    the count of canceled orders over the past 56 weeks, and the amount of money spent over the past 7 days.
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["Feature"], proxy_class="featurebyte.Feature")

    # pydantic instance variable (public)
    feature_store: FeatureStoreModel = Field(exclude=True, allow_mutation=False)

    # class variables
    _route = "/feature"
    _update_schema_class = FeatureUpdate
    _list_schema = FeatureModelResponse
    _get_schema = FeatureModelResponse
    _list_fields = [
        "name",
        "version",
        "dtype",
        "readiness",
        "online_enabled",
        "tables",
        "primary_tables",
        "entities",
        "primary_entities",
        "created_at",
    ]
    _list_foreign_keys = [
        ForeignKeyMapping("entity_ids", Entity, "entities"),
        ForeignKeyMapping("tabular_data_ids", TableApiObject, "tables"),
        ForeignKeyMapping("primary_entity_ids", Entity, "primary_entities"),
        ForeignKeyMapping("primary_table_ids", TableApiObject, "primary_tables"),
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
    def get(cls, name: str, version: Optional[str] = None) -> Feature:
        """
        Retrieve the Feature from the persistent data store given the object's name, and version.

        This assumes that the object has been saved to the persistent data store. If the object has not been saved,
        an exception will be raised and you should create and save the object first.

        Parameters
        ----------
        name: str
            Name of the Feature to retrieve.
        version: Optional[str]
            Feature version, if None, the default version will be returned.

        Returns
        -------
        Feature
            Feature object.

        Examples
        --------
        Get a Feature object that is already saved.

        >>> feature = fb.Feature.get("InvoiceCount_60days")
        """
        if version is None:
            feature_namespace = FeatureNamespace.get(name=name)
            return cls.get_by_id(id=feature_namespace.default_feature_id)
        return cls._get(name=name, other_params={"version": version})

    @classmethod
    def list(
        cls,
        include_id: Optional[bool] = False,
        primary_entity: Optional[Union[str, List[str]]] = None,
        primary_table: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """
        List saved features

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list
        primary_entity: Optional[Union[str, List[str]]]
            Name of entity used to filter results. If multiple entities are provided, the filtered results will
            contain features that are associated with all the entities.
        primary_table: Optional[Union[str, List[str]]]
            Name of table used to filter results. If multiple tables are provided, the filtered results will
            contain features that are associated with all the tables.

        Returns
        -------
        pd.DataFrame
            Table of features
        """
        return FeatureNamespace.list(
            include_id=include_id, primary_entity=primary_entity, primary_table=primary_table
        )

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
        primary_entity: Optional[Union[str, List[str]]] = None,
        primary_table: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """
        List saved features versions

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list
        feature_list_id: Optional[ObjectId] = None,
            Include only features in specified feature list
        primary_entity: Optional[str]
            Name of entity used to filter results. If multiple entities are provided, the filtered results will
            contain features that are associated with all the entities.
        primary_table: Optional[str]
            Name of table used to filter results. If multiple tables are provided, the filtered results will
            contain features that are associated with all the tables.

        Returns
        -------
        pd.DataFrame
            Table of features

        Examples
        --------
        List saved Feature versions

        >>> Feature.list_versions()  # doctest: +SKIP
            name        version  dtype readiness  online_enabled             table    entities              created_at
        0  new_feat2  V230323  FLOAT     DRAFT           False      [sf_event_table]  [customer] 2023-03-23 07:16:21.244
        1  new_feat1  V230323  FLOAT     DRAFT           False      [sf_event_table]  [customer] 2023-03-23 07:16:21.166
        2     sum_1d  V230323  FLOAT     DRAFT           False      [sf_event_table]  [customer] 2023-03-23 07:16:21.009

        List Feature versions with the same name

        >>> feature = catalog.get_feature("InvoiceCount_60days")
        >>> feature.list_versions()  # doctest: +SKIP
                name  version  dtype readiness  online_enabled             table    entities              created_at
            0  sum_1d  V230323  FLOAT     DRAFT           False  [sf_event_table]  [customer] 2023-03-23 06:19:35.838
        """
        params = {}
        if feature_list_id:
            params = {"feature_list_id": str(feature_list_id)}

        feature_list = cls._list(include_id=include_id, params=params)
        target_entities = convert_to_list_of_strings(primary_entity)
        target_tables = convert_to_list_of_strings(primary_table)
        if target_entities:
            feature_list = feature_list[
                feature_list.primary_entities.apply(
                    lambda entities: all(entity in entities for entity in target_entities)
                )
            ]
        if target_tables:
            feature_list = feature_list[
                feature_list.primary_tables.apply(
                    lambda tables: all(table in tables for table in target_tables)
                )
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

        Examples
        --------
        >>> feature = catalog.get_feature("InvoiceCount_60days")
        >>> feature.list_versions()  # doctest: +SKIP
                name  version  dtype readiness  online_enabled             table    entities              created_at
            0  sum_1d  V230323  FLOAT     DRAFT           False  [sf_event_table]  [customer] 2023-03-23 06:19:35.838
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
            entity_ids.extend(cast(GroupByNode, node).parameters.keys)
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

    @property
    def definition(self) -> str:
        """
        Display feature definition string of this feature.

        Returns
        -------
        str
        """
        try:
            # retrieve all the table used to construct this feature
            data_id_to_doc = {
                data_id: Table.get_by_id(data_id).dict() for data_id in self.tabular_data_ids
            }
        except RecordRetrievalException:
            # table used to construct this feature has not been saved
            data_id_to_doc = {}
        return CodeStr(
            self._generate_code(
                to_format=True, to_use_saved_data=True, table_id_to_info=data_id_to_doc
            )
        )

    @property
    def primary_entity(self) -> List[Entity]:
        """
        Primary entity of this feature

        The primary entity of a feature defines the level of analysis for that feature.

        Returns
        -------
        list[Entity]
            Primary entity
        """
        entities = []
        for entity_id in self.entity_ids:
            entities.append(Entity.get_by_id(entity_id))
        primary_entity = derive_primary_entity(entities)
        return primary_entity

    def binary_op_series_params(
        self, other: Scalar | FrozenSeries | ScalarSequence
    ) -> dict[str, Any]:
        """
        Parameters that will be passed to series-like constructor in _binary_op method


        Parameters
        ----------
        other: other: Scalar | FrozenSeries | ScalarSequence
            Other object

        Returns
        -------
        dict[str, Any]
        """
        entity_ids = set(self.entity_ids)
        if isinstance(other, FrozenSeries):
            entity_ids = entity_ids.union(getattr(other, "entity_ids", []))
        return {"entity_ids": sorted(entity_ids)}

    def unary_op_series_params(self) -> dict[str, Any]:
        return {"entity_ids": self.entity_ids}

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

    @enforce_observation_set_row_order
    @typechecked
    def preview(
        self,
        observation_set: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Materialize feature using a small observation set of up to 50 rows.

        Unlike get_historical_features, this method does not store partial aggregations (tiles) to
        speed up future computation. Instead, it computes the feature on the fly, and should be used
        only for small observation sets for debugging or prototyping unsaved features.

        Tiles are a method of storing partial aggregations in the feature store,
        which helps to minimize the resources required to fulfill historical, batch and online requests.

        Parameters
        ----------
        observation_set : pd.DataFrame
            Observation set DataFrame, which should contain the `POINT_IN_TIME` column,
            as well as columns with serving names for all entities used by the feature.

        Returns
        -------
        pd.DataFrame
            Materialized feature values.
            The returned DataFrame will have the same number of rows, and include all columns from the observation set.

            **Note**: `POINT_IN_TIME` values will be converted to UTC time.

        Raises
        ------
        RecordRetrievalException
            Failed to materialize feature preview.

        Examples
        --------
        Preview feature with a small observation set.

        >>> catalog.get_feature("InvoiceCount_60days").preview(
        ...     observation_set=pd.DataFrame({
        ...         "POINT_IN_TIME": ["2022-06-01 00:00:00", "2022-06-02 00:00:00"],
        ...         "GROCERYCUSTOMERGUID": [
        ...             "a2828c3b-036c-4e2e-9bd6-30c9ee9a20e3",
        ...             "ac479f28-e0ff-41a4-8e60-8678e670e80b",
        ...         ],
        ...     })
        ... )
          POINT_IN_TIME                   GROCERYCUSTOMERGUID  InvoiceCount_60days
        0    2022-06-01  a2828c3b-036c-4e2e-9bd6-30c9ee9a20e3                 10.0
        1    2022-06-02  ac479f28-e0ff-41a4-8e60-8678e670e80b                  6.0

        See Also
        --------
        - [FeatureGroup.preview](/reference/featurebyte.api.feature_list.FeatureGroup.preview/):
          Preview feature group.
        - [FeatureList.get_historical_features](/reference/featurebyte.api.feature_list.FeatureList.get_historical_features/):
          Get historical features from a feature list.
        """
        tic = time.time()

        feature = self._get_pruned_feature_model()
        payload = FeaturePreview(
            feature_store_name=self.feature_store.name,
            graph=feature.graph,
            node_name=feature.node_name,
            point_in_time_and_serving_name_list=observation_set.to_dict(orient="records"),
        )

        client = Configurations().get_client()
        response = client.post(url="/feature/preview", json=payload.json_dict())
        if response.status_code != HTTPStatus.OK:
            raise RecordRetrievalException(response)
        result = response.json()

        elapsed = time.time() - tic
        logger.debug(f"Preview took {elapsed:.2f}s")
        return dataframe_from_json(result)  # pylint: disable=no-member

    @typechecked
    def create_new_version(
        self,
        table_feature_job_settings: Optional[List[TableFeatureJobSetting]] = None,
        table_cleaning_operations: Optional[List[TableCleaningOperation]] = None,
    ) -> Feature:
        """
        Create new feature version from the current one.

        Parameters
        ----------
        table_feature_job_settings: Optional[List[TableFeatureJobSetting]]
            List of table feature job settings to be applied to the feature
        table_cleaning_operations: Optional[List[DataCleaningOperation]]
            List of table cleaning operations to be applied to the feature

        Returns
        -------
        Feature

        Raises
        ------
        RecordCreationException
            When failed to save a new version, e.g. when the feature is exactly the same as the current one

        Examples
        --------

        Create a new version of a feature with different feature job setting

        >>> import featurebyte as fb
        >>> feature = catalog.get_feature("my_magic_feature")  # doctest: +SKIP
        >>> feature.create_new_version(
        ...   table_feature_job_settings=[
        ...     fb.TableFeatureJobSetting(
        ...       table_name="some_event_table_name",
        ...       feature_job_setting=fb.FeatureJobSetting(
        ...         blind_spot="10m",
        ...         frequency="30m",
        ...         time_modulo_frequency="5m",
        ...       )
        ...     )
        ...   ]
        ... )  # doctest: +SKIP


        Create a new version of a feature with table cleaning operations

        >>> import featurebyte as fb
        >>> feature = catalog.get_feature("my_magic_feature")  # doctest: +SKIP
        >>> feature.create_new_version(
        ...   table_cleaning_operations=[
        ...     fb.TableCleaningOperation(
        ...       table_name="some_event_table_name",
        ...       column_cleaning_operations=[
        ...         fb.ColumnCleaningOperation(
        ...           column_name="some_column_name",
        ...           cleaning_operations=[fb.MissingValueImputation(imputed_value=0.0)],
        ...         )
        ...       ],
        ...     )
        ...   ]
        ... )  # doctest: +SKIP

        """
        client = Configurations().get_client()
        response = client.post(
            url=self._route,
            json={
                "source_feature_id": str(self.id),
                "table_feature_job_settings": [
                    table_feature_job_setting.dict()
                    for table_feature_job_setting in table_feature_job_settings
                ]
                if table_feature_job_settings
                else None,
                "table_cleaning_operations": [
                    clean_ops.dict() for clean_ops in table_cleaning_operations
                ]
                if table_cleaning_operations
                else None,
            },
        )
        if response.status_code != HTTPStatus.CREATED:
            raise RecordCreationException(response=response)

        object_dict = response.json()
        self._update_cache(object_dict)  # update object cache store
        return Feature(**object_dict, **self._get_init_params_from_object(), saved=True)

    @typechecked
    def update_readiness(
        self,
        readiness: Literal[tuple(FeatureReadiness)],  # type: ignore[misc]
        ignore_guardrails: bool = False,
    ) -> None:
        """
        Update feature readiness

        Parameters
        ----------
        readiness: Literal[tuple(FeatureReadiness)]
            Feature readiness level
        ignore_guardrails: bool
            Allow a user to specify if they want to  ignore any guardrails when updating this feature. This should
            currently only apply of the FeatureReadiness value is being updated to PRODUCTION_READY. This should
            be a no-op for all other scenarios.

        Examples
        --------

        >>> feature = catalog.get_feature("InvoiceCount_60days")
        >>> feature.update_readiness(readiness="DEPRECATED")
        """
        self.update(
            update_payload={"readiness": str(readiness), "ignore_guardrails": ignore_guardrails},
            allow_update_local=False,
        )

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

        Examples
        --------

        >>> feature = catalog.get_feature("InvoiceCount_60days")
        >>> feature.update_default_version_mode("MANUAL")
        """
        self.feature_namespace.update(
            update_payload={"default_version_mode": DefaultVersionMode(default_version_mode).value},
            allow_update_local=False,
        )

    def as_default_version(self) -> None:
        """
        Set the feature as default version

        Examples
        --------

        >>> feature = catalog.get_feature("InvoiceCount_60days")
        >>> feature.update_default_version_mode(DefaultVersionMode.MANUAL)
        >>> feature.as_default_version()
        """
        self.feature_namespace.update(
            update_payload={"default_feature_id": self.id},
            allow_update_local=False,
        )
        assert self.feature_namespace.default_feature_id == self.id

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
        self, other: Union[FrozenSeries, Sequence[Union[bool, int, float, str]]]
    ) -> None:
        """
        Validates whether a feature is a lookup feature

        Parameters
        ----------
        other: Union[FrozenSeries, Sequence[Union[bool, int, float, str]]]
            other
        """
        assert_is_lookup_feature(self.node_types_lineage)

    # descriptors
    list_versions: ClassVar[ClassInstanceMethodDescriptor] = ClassInstanceMethodDescriptor(
        class_method=_list_versions,
        instance_method=_list_versions_with_same_name,
    )
