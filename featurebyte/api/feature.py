"""
Feature and FeatureList classes
"""
# pylint: disable=too-many-lines
from __future__ import annotations

from typing import Any, ClassVar, Dict, List, Literal, Optional, Sequence, Tuple, Type, Union, cast

import time
from http import HTTPStatus

import pandas as pd
from bson import ObjectId
from pydantic import Field, root_validator
from typeguard import typechecked

from featurebyte.api.api_object import ConflictResolution, DeletableApiObject, SavableApiObject
from featurebyte.api.entity import Entity
from featurebyte.api.feature_job import FeatureJobMixin
from featurebyte.api.feature_namespace import FeatureNamespace
from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.feature_util import FEATURE_COMMON_LIST_FIELDS, FEATURE_LIST_FOREIGN_KEYS
from featurebyte.api.feature_validation_util import assert_is_lookup_feature
from featurebyte.common.descriptor import ClassInstanceMethodDescriptor
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.typing import Scalar, ScalarSequence
from featurebyte.common.utils import CodeStr, dataframe_from_json, enforce_observation_set_row_order
from featurebyte.config import Configurations
from featurebyte.core.accessor.count_dict import CdAccessorMixin
from featurebyte.core.generic import ProtectedColumnsQueryObject
from featurebyte.core.series import FrozenSeries, FrozenSeriesT, Series
from featurebyte.exception import RecordCreationException, RecordRetrievalException
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.logging import get_logger
from featurebyte.models.base import PydanticObjectId, VersionIdentifier
from featurebyte.models.feature import (
    DefaultVersionMode,
    FeatureModel,
    FeatureReadiness,
    FrozenFeatureModel,
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

logger = get_logger(__name__)


class Feature(
    ProtectedColumnsQueryObject,
    Series,
    FrozenFeatureModel,
    DeletableApiObject,
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
    __fbautodoc__ = FBAutoDoc(proxy_class="featurebyte.Feature")

    # pydantic instance variable (public)
    feature_store: FeatureStoreModel = Field(
        exclude=True,
        allow_mutation=False,
        description="Provides information about the feature store that the feature is connected to.",
    )

    # class variables
    _route = "/feature"
    _update_schema_class = FeatureUpdate
    _list_schema = FeatureModelResponse
    _get_schema = FeatureModelResponse
    _list_fields = [
        "name",
        "version",
        *FEATURE_COMMON_LIST_FIELDS,
    ]
    _list_foreign_keys = FEATURE_LIST_FOREIGN_KEYS

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

    @typechecked
    def isin(  # pylint: disable=useless-parent-delegation
        self: FrozenSeriesT, other: Union[FrozenSeries, ScalarSequence]
    ) -> FrozenSeriesT:
        """
        Identifies if each element is contained in a sequence of values represented by the `other` parameter.

        Parameters
        ----------
        other: Union[FrozenSeries, ScalarSequence]
            The sequence of values to check for membership. `other` can be a predefined list of values, or a Cross
            Aggregate feature. If `other` is a Cross Aggregate feature, the keys of the Cross Aggregate feature will
            be used to check for membership.

        Returns
        -------
        FrozenSeriesT
            Feature with boolean values

        Examples
        --------
        Create a new feature that checks whether a lookup feature is contained in the keys of a
        dictionary feature:

        >>> lookup_feature = catalog.get_feature("ProductGroupLookup")
        >>> dictionary_feature = catalog.get_feature("CustomerProductGroupCounts_7d")
        >>> new_feature = lookup_feature.isin(dictionary_feature)
        >>> new_feature.name = "CustomerHasProductGroup_7d"
        """
        return super().isin(other)  # type: ignore[no-any-return,misc]

    def info(  # pylint: disable=useless-parent-delegation
        self, verbose: bool = False
    ) -> Dict[str, Any]:
        """
        Returns a dictionary that summarizes the essential information of an Feature object. The dictionary contains
        the following keys:

        - `created_at`: The timestamp indicating when the Feature object was created.
        - `name`: The name of the Feature object.
        - `updated_at`: The timestamp indicating when the Feature object was last updated.
        - `catalog_name`: The catalog name of the Feature object.
        - `primary_entity`: Details about the primary entity of the Feature object.
        - `entities`: List of entities involved in the computation of the Feature object.
        - `primary_table`: Details about the primary table of the Feature object.
        - `tables`: List of tables involved in the computation of the Feature object.
        - `default_version_mode`: Indicates whether the default version mode is 'auto' or 'manual'.
        - `version_count`: The number of versions with the same feature namespace.
        - `dtype`: the data type of the Feature object.
        - `default_feature_id`: The unique identifier of the default version with the same feature namespace.
            This is the version returned by the catalog when only the object's feature namespace is used.
        - `metadata`: Summary of key operations.

        Some information is provided for both the Feature object and the default version with the same
        feature namespace:

        - `version`: The version name.
        - `readiness`: The readiness state.
        - `table_feature_job_setting`: Details of the Feature Job setting of tables used by the feature.
        - `table_cleaning_operation`: Details of cleaning operations of table columns used by the feature.

        This method is only available for Feature objects that are saved in the catalog.

        Parameters
        ----------
        verbose: bool
            Control verbose level of the summary.

        Returns
        -------
        Dict[str, Any]
            Key-value mapping of properties of the object.
        """
        return super().info(verbose)

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
        include_id: Optional[bool] = True,
        feature_list_id: Optional[ObjectId] = None,
    ) -> pd.DataFrame:
        """
        Returns a DataFrame that presents a summary of the feature versions belonging to the namespace of the
        Feature object. The DataFrame contains multiple attributes of the feature versions, such as their version
        names, readiness states, online availability, and creation dates.

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list.
        feature_list_id: Optional[ObjectId] = None,
            Feature list ID used to filter results and only include versions that are used in the specified feature
            list.

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

        return cls._list(include_id=include_id, params=params)

    def _list_versions_with_same_name(self, include_id: bool = True) -> pd.DataFrame:
        """
        List feature versions with the same name

        Parameters
        ----------
        include_id: bool
            Whether to include id in the list.

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

    def delete(self) -> None:
        """
        Delete a feature. A feature can only be deleted if
        * the feature readiness is DRAFT
        * the feature is not used in any feature list
        * the feature is not a default feature with manual version mode

        Examples
        --------
        Delete a feature.

        >>> feature = catalog.get_feature("InvoiceCount_60days")
        >>> feature.delete()  # doctest: +SKIP

        See Also
        --------
        - [Feature.update_default_version_mode](/reference/featurebyte.api.feature.Feature.update_default_version_mode/)
        """
        self._delete()

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
        Returns the readiness level of the feature object.

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
    def is_datetime(self) -> bool:  # pylint: disable=useless-parent-delegation
        """
        Returns whether the feature has a datetime data type.

        Returns
        -------
        bool
        """
        return super().is_datetime

    @property
    def is_numeric(self) -> bool:  # pylint: disable=useless-parent-delegation
        """
        Returns whether the feature has a numeric data type.

        Returns
        -------
        bool
        """
        return super().is_numeric

    @property
    def saved(self) -> bool:  # pylint: disable=useless-parent-delegation
        """
        Returns whether the Feature object is saved and part of the catalog.

        Returns
        -------
        bool
        """
        return super().saved

    @property
    def definition(self) -> str:
        """
        Displays the feature definition file of the feature.

        The file is the single source of truth for a feature version. The file is generated automatically after a
        feature is declared in the SDK and is stored in the FeatureByte Service.

        This file uses the same SDK syntax as the feature declaration and provides an explicit outline of the intended
        operations of the feature declaration, including those that are inherited but not explicitly declared by the
        user. These operations may include feature job settings and cleaning operations inherited from tables metadata.

        The feature definition file serves as the basis for generating the final logical execution graph, which is
        then transpiled into platform-specific SQL (e.g. SnowSQL, SparkSQL) for feature materialization."

        Returns
        -------
        str
        """
        try:
            definition = self.cached_model.definition
            assert definition is not None, "Saved feature's definition should not be None."
        except RecordRetrievalException:
            definition = self._generate_code(to_format=True, to_use_saved_data=True)
        return CodeStr(definition)

    @property
    def primary_entity(self) -> List[Entity]:
        """
        Returns the primary entity of the Feature object.

        Returns
        -------
        list[Entity]
            Primary entity
        """
        entities = []
        for entity_id in self.entity_ids:
            entities.append(Entity.get_by_id(entity_id))
        primary_entity = derive_primary_entity(entities)  # type: ignore
        return primary_entity

    @typechecked
    def save(  # pylint: disable=useless-parent-delegation
        self, conflict_resolution: ConflictResolution = "raise"
    ) -> None:
        """
        Adds a Feature object to the catalog.

        A conflict could be triggered when the object being saved has violated a uniqueness check at the catalog.
        If uniqueness is violated, you can either raise an error or retrieve the object with the same name, depending
        on the conflict resolution parameter passed in. The default behavior is to raise an error.

        Parameters
        ----------
        conflict_resolution: ConflictResolution
            "raise" will raise an error when we encounter a conflict error.
            "retrieve" will handle the conflict error by retrieving the object with the same name.
        """
        super().save(conflict_resolution=conflict_resolution)

    @typechecked
    def astype(  # pylint: disable=useless-parent-delegation
        self: FrozenSeriesT,
        new_type: Union[Type[int], Type[float], Type[str], Literal["int", "float", "str"]],
    ) -> FrozenSeriesT:
        """
        Modifies the data type of a feature. It is useful when you need to convert feature values between numerical
        and string formats, or the other way around.

        Parameters
        ----------
        new_type : Union[Type[int], Type[float], Type[str], Literal["int", "float", "str"]])
            Desired type after conversion. Type can be provided directly, or as a string.

        Returns
        -------
        FrozenSeriesT
            A new Series with converted variable type.
        """
        return super().astype(new_type=new_type)  # type: ignore[no-any-return,misc]

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
        Materializes a Feature object using a small observation set of up to 50 rows. Unlike compute_historical_features,
        this method does not store partial aggregations (tiles) to speed up future computation. Instead, it computes
        the feature values on the fly, and should be used only for small observation sets for debugging or prototyping
        unsaved features.

        The small observation set should combine historical points-in-time and key values of the primary entity from
        the feature. Associated serving entities can also be utilized.

        Parameters
        ----------
        observation_set : pd.DataFrame
            Observation set DataFrame which combines historical points-in-time and values of the feature primary entity
            or its descendant (serving entities). The column containing the point-in-time values should be named
            `POINT_IN_TIME`, while the columns representing entity values should be named using accepted serving
            names for the entity.

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
        - [FeatureGroup.preview](/reference/featurebyte.api.feature_group.FeatureGroup.preview/):
          Preview feature group.
        - [FeatureList.compute_historical_features](/reference/featurebyte.api.feature_list.FeatureList.compute_historical_features/):
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
        Creates a new feature version from a Feature object. The new version is created by replacing the current
        feature's feature job settings (if provided) and the table cleaning operations (if provided).

        Parameters
        ----------
        table_feature_job_settings: Optional[List[TableFeatureJobSetting]]
            List of table feature job settings to apply to the feature. Each item in the list represents a specific
            feature job setting for a table, which is created using the TableFeatureJobSetting constructor. This
            constructor takes the table name and the desired feature job setting as input. The setting should only be
            applied to tables that originally contained the timestamp column used in the GroupBy.aggregate_over
            operation for the feature. If the operation was performed on an item table, use the name of the related
            event table, as the event timestamp is sourced from there.
        table_cleaning_operations: Optional[List[TableCleaningOperation]]
            List of table cleaning operations to apply to the feature. Each item in the list represents the cleaning
            operations for a specific table, which is created using the TableCleaningOperation constructor. This
            constructor takes the table name and the cleaning operations for that table as input. The cleaning
            operations for each table are represented as a list, where each item defines the cleaning operations
            for a specific column. The association between a column and its cleaning operations is established using
            the ColumnCleaningOperation constructor.

        Returns
        -------
        Feature
            New feature version created based on provided feature settings and table cleaning operations.

        Raises
        ------
        RecordCreationException
            When failed to save a new version, e.g. when the created feature is exactly the same as the current one.
            This could happen when the provided feature settings and table cleaning operations are irrelevant to the
            current feature.

        Examples
        --------
        Check feature job setting of this feature first:

        >>> feature = catalog.get_feature("InvoiceAmountAvg_60days")
        >>> feature.info()["table_feature_job_setting"]
        {'this': [{'table_name': 'GROCERYINVOICE',
           'feature_job_setting': {'blind_spot': '0s',
            'frequency': '3600s',
            'time_modulo_frequency': '90s'}}],
         'default': [{'table_name': 'GROCERYINVOICE',
           'feature_job_setting': {'blind_spot': '0s',
            'frequency': '3600s',
            'time_modulo_frequency': '90s'}}]}

        Create a new feature with a different feature job setting:

        >>> new_feature = feature.create_new_version(
        ...   table_feature_job_settings=[
        ...     fb.TableFeatureJobSetting(
        ...       table_name="GROCERYINVOICE",
        ...       feature_job_setting=fb.FeatureJobSetting(
        ...         blind_spot="60s",
        ...         frequency="3600s",
        ...         time_modulo_frequency="90s",
        ...       )
        ...     )
        ...   ]
        ... )
        >>> new_feature.info()["table_feature_job_setting"]
        {'this': [{'table_name': 'GROCERYINVOICE',
           'feature_job_setting': {'blind_spot': '60s',
            'frequency': '3600s',
            'time_modulo_frequency': '90s'}}],
         'default': [{'table_name': 'GROCERYINVOICE',
           'feature_job_setting': {'blind_spot': '0s',
            'frequency': '3600s',
            'time_modulo_frequency': '90s'}}]}

        Check table cleaning operation of this feature first:

        >>> feature = catalog.get_feature("InvoiceAmountAvg_60days")
        >>> feature.info()["table_cleaning_operation"]
        {'this': [], 'default': []}

        Create a new version of a feature with different table cleaning operations:

        >>> new_feature = feature.create_new_version(
        ...   table_cleaning_operations=[
        ...     fb.TableCleaningOperation(
        ...       table_name="GROCERYINVOICE",
        ...       column_cleaning_operations=[
        ...         fb.ColumnCleaningOperation(
        ...           column_name="Amount",
        ...           cleaning_operations=[fb.MissingValueImputation(imputed_value=0.0)],
        ...         )
        ...       ],
        ...     )
        ...   ]
        ... )
        >>> new_feature.info()["table_cleaning_operation"]
        {'this': [{'table_name': 'GROCERYINVOICE',
           'column_cleaning_operations': [{'column_name': 'Amount',
             'cleaning_operations': [{'imputed_value': 0.0, 'type': 'missing'}]}]}],
         'default': []}

        Check the tables used by this feature first:

        >>> feature = catalog.get_feature("InvoiceAmountAvg_60days")
        >>> feature.info()["tables"]
        [{'name': 'GROCERYINVOICE', 'status': 'PUBLIC_DRAFT', 'catalog_name': 'grocery'}]

        Create a new version of a feature with irrelevant table cleaning operations (for example, the specified
        table name or column name is not used by the feature):

        >>> feature.create_new_version(
        ...   table_cleaning_operations=[
        ...     fb.TableCleaningOperation(
        ...       table_name="GROCERYPRODUCT",
        ...       column_cleaning_operations=[
        ...         fb.ColumnCleaningOperation(
        ...           column_name="GroceryProductGuid",
        ...           cleaning_operations=[fb.MissingValueImputation(imputed_value=None)],
        ...         )
        ...       ],
        ...     )
        ...   ]
        ... )
        Traceback (most recent call last):
        ...
        featurebyte.exception.RecordCreationException:
        Table cleaning operation(s) does not result a new feature version.
        This is because the new feature version is the same as the source feature.

        See Also
        --------
        - [GroupBy.aggregate_over](/reference/featurebyte.api.groupby.GroupBy.aggregate_over/)
        - [Table.column_cleaning_operations](/reference/featurebyte.api.base_table.TableApiObject.column_cleaning_operations)
        - [Feature.info](/reference/featurebyte.api.feature.Feature.info/)
        - [FeatureList.create_new_version](/reference/featurebyte.api.feature_list.FeatureList.create_new_version/)
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
        return Feature(**object_dict, **self._get_init_params_from_object())

    @typechecked
    def update_readiness(
        self,
        readiness: Literal[tuple(FeatureReadiness)],  # type: ignore[misc]
        ignore_guardrails: bool = False,
    ) -> None:
        """
        Updates readiness of a feature version.

        A Feature version can have one of four readiness levels:

        1. PRODUCTION_READY: Assigned to Feature versions that are ready for deployment in production environments.
        2. PUBLIC_DRAFT: For Feature versions shared for feedback purposes.
        3. DRAFT: For Feature versions in the prototype stage.
        4. DEPRECATED: For feature versions not advised for use in either training or online serving.

        When a new feature version is created, its status is DRAFT.
        Only a Draft feature version can be deleted. Feature versions with other status cannot be reverted to DRAFT.
        Only one version of a feature can be labeled as PRODUCTION_READY.

        Once a feature version is promoted as PRODUCTION_READY, guardrails are automatically applied to ensure that
        the cleaning operations and FeatureJob settings are consistent with the defaults defined at the table level.
        These guardrails can be disregarded by setting the ignore_guardrails as True if the user is confident that
        the settings of the promoted feature version adhere to equally robust practices.

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
        >>> feature.update_readiness(readiness="PRODUCTION_READY")  # doctest: +SKIP
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
        Sets the default version mode of a feature.

        By default, the feature default version mode is auto and the feature's version with the highest level of
        readiness is considered as the default version. In cases where multiple versions share the highest level of
        readiness, the most recent version is automatically chosen as the default.

        If the default version mode is set as manual, you can choose to manually set any version as the default version
        for the feature.

        Parameters
        ----------
        default_version_mode: Literal[tuple(DefaultVersionMode)]
            Feature default version mode.

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
        When a feature has its default version mode set to manual, this method designates the Feature object as the
        default version for that specific feature.

        Each feature is recognized by its name and can possess numerous versions, though only a single default
        version is allowed.

        The default version streamlines feature reuse by supplying the most suitable version when none is explicitly
        indicated. By default, the feature's default version mode is automatic, selecting the version with the highest
        readiness level as the default. If several versions share the same readiness level, the most recent one
        becomes the default.

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
        Get Feature SQL.

        Returns
        -------
        str
            Retrieved Feature SQL string.

        Raises
        ------
        RecordRetrievalException
            Failed to get feature SQL.
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
        Validates whether a feature is a lookup feature.

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
