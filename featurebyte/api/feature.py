"""
Feature and FeatureList classes
"""

from __future__ import annotations

from http import HTTPStatus
from typing import Any, ClassVar, Dict, List, Optional, Sequence, Tuple, Type, Union, cast

import pandas as pd
from bson import ObjectId
from pydantic import BaseModel, Field, model_validator
from typeguard import typechecked
from typing_extensions import Literal

from featurebyte.api.api_handler.base import ListHandler
from featurebyte.api.api_handler.feature import FeatureListHandler
from featurebyte.api.api_object_util import ForeignKeyMapping
from featurebyte.api.entity import Entity
from featurebyte.api.feature_job import FeatureJobMixin
from featurebyte.api.feature_namespace import FeatureNamespace
from featurebyte.api.feature_or_target_mixin import FeatureOrTargetMixin
from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.feature_util import FEATURE_COMMON_LIST_FIELDS, FEATURE_LIST_FOREIGN_KEYS
from featurebyte.api.observation_table import ObservationTable
from featurebyte.api.savable_api_object import DeletableApiObject, SavableApiObject
from featurebyte.api.templates.doc_util import substitute_docstring
from featurebyte.api.templates.entity_doc import (
    ENTITY_DOC,
    ENTITY_IDS_DOC,
    PRIMARY_ENTITY_DOC,
    PRIMARY_ENTITY_IDS_DOC,
)
from featurebyte.api.templates.feature_or_target_doc import (
    CATALOG_ID_DOC,
    DEFINITION_DOC,
    PREVIEW_DOC,
    TABLE_IDS_DOC,
    VERSION_DOC,
)
from featurebyte.api.templates.series_doc import ISNULL_DOC, NOTNULL_DOC
from featurebyte.common.descriptor import ClassInstanceMethodDescriptor
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.utils import enforce_observation_set_row_order, is_server_mode
from featurebyte.config import Configurations
from featurebyte.core.accessor.count_dict import CdAccessorMixin
from featurebyte.core.accessor.feature_datetime import FeatureDtAccessorMixin
from featurebyte.core.accessor.feature_string import FeatureStrAccessorMixin
from featurebyte.core.series import FrozenSeries, FrozenSeriesT, Series
from featurebyte.enum import ConflictResolution, DBVarType, FeatureType
from featurebyte.exception import RecordCreationException, RecordRetrievalException
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.logging import get_logger
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_namespace import DefaultVersionMode, FeatureReadiness
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.tile import TileSpec
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph, QueryGraph
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.model.feature_job_setting import (
    TableFeatureJobSetting,
    TableIdFeatureJobSetting,
)
from featurebyte.query_graph.node.cleaning_operation import TableCleaningOperation
from featurebyte.schema.feature import (
    BatchFeatureCreatePayload,
    BatchFeatureItem,
    FeatureCreate,
    FeatureModelResponse,
    FeatureSQL,
    FeatureUpdate,
)
from featurebyte.typing import ScalarSequence

logger = get_logger(__name__)

DOCSTRING_FORMAT_PARAMS = {"class_name": "Feature"}


class Feature(
    Series,
    DeletableApiObject,
    SavableApiObject,
    FeatureOrTargetMixin,
    CdAccessorMixin,
    FeatureJobMixin,
    FeatureDtAccessorMixin,
    FeatureStrAccessorMixin,
):
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

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.Feature")
    _route: ClassVar[str] = "/feature"
    _update_schema_class: ClassVar[Any] = FeatureUpdate
    _list_schema: ClassVar[Any] = FeatureModelResponse
    _get_schema: ClassVar[Any] = FeatureModelResponse
    _list_fields: ClassVar[List[str]] = [
        "name",
        "version",
        *FEATURE_COMMON_LIST_FIELDS,
        "is_default",
    ]
    _list_foreign_keys: ClassVar[List[ForeignKeyMapping]] = FEATURE_LIST_FOREIGN_KEYS

    # pydantic instance variable (public)
    feature_store: FeatureStoreModel = Field(
        exclude=True,
        frozen=True,
        description="Provides information about the feature store that the feature is connected to.",
    )

    def _get_create_payload(self) -> dict[str, Any]:
        data = FeatureCreate(**self.model_dump(by_alias=True))
        return data.json_dict()

    def _get_init_params_from_object(self) -> dict[str, Any]:
        return {"feature_store": self.feature_store}

    def _get_feature_tiles_specs(self) -> List[Tuple[str, List[TileSpec]]]:
        tile_specs = ExtendedFeatureModel(**self.model_dump(by_alias=True)).tile_specs
        return [(str(self.name), tile_specs)] if tile_specs else []

    @property
    def row_index_lineage(self) -> Tuple[str, ...]:
        # Override the row_index_lineage property to return a constant value. This is so that the
        # check for row_lineage_index alignment for Feature objects always passes. The check is not
        # applicable to Feature objects because they can freely interact with each other regardless
        # of their lineage.
        return tuple()

    @model_validator(mode="before")
    @classmethod
    def _set_feature_store(cls, values: Any) -> Any:
        if isinstance(values, BaseModel):
            values = values.model_dump(by_alias=True)

        if isinstance(values, dict) and "feature_store" not in values:
            tabular_source = values.get("tabular_source")
            if isinstance(tabular_source, dict):
                feature_store_id = TabularSource(**tabular_source).feature_store_id
                values["feature_store"] = FeatureStore.get_by_id(id=feature_store_id)
        return values

    @property
    @substitute_docstring(
        doc_template=VERSION_DOC,
        examples=(
            """
            >>> feature = catalog.get_feature("CustomerProductGroupCounts_7d")
            >>> feature.version  # doctest: +SKIP
            'V230323'
            """
        ),
        format_kwargs=DOCSTRING_FORMAT_PARAMS,
    )
    def version(self) -> str:
        return self._get_version()

    @property
    @substitute_docstring(doc_template=CATALOG_ID_DOC, format_kwargs=DOCSTRING_FORMAT_PARAMS)
    def catalog_id(self) -> ObjectId:
        return self._get_catalog_id()

    @property
    @substitute_docstring(doc_template=ENTITY_IDS_DOC, format_kwargs=DOCSTRING_FORMAT_PARAMS)
    def entity_ids(self) -> Sequence[ObjectId]:
        return self._get_entity_ids()

    @property
    @substitute_docstring(
        doc_template=PRIMARY_ENTITY_IDS_DOC, format_kwargs=DOCSTRING_FORMAT_PARAMS
    )
    def primary_entity_ids(
        self,
    ) -> Sequence[ObjectId]:
        return self._get_primary_entity_ids()

    @property
    @substitute_docstring(doc_template=ENTITY_DOC, format_kwargs=DOCSTRING_FORMAT_PARAMS)
    def entities(self) -> List[Entity]:
        return self._get_entities()

    @property
    @substitute_docstring(doc_template=PRIMARY_ENTITY_DOC, format_kwargs=DOCSTRING_FORMAT_PARAMS)
    def primary_entity(self) -> List[Entity]:
        return self._get_primary_entity()

    @property
    @substitute_docstring(doc_template=TABLE_IDS_DOC, format_kwargs=DOCSTRING_FORMAT_PARAMS)
    def table_ids(self) -> Sequence[ObjectId]:
        return self._get_table_ids()

    @property
    def feature_list_ids(self) -> Sequence[ObjectId]:
        """
        Returns the feature list IDs that use the Feature object.

        Returns
        -------
        Sequence[ObjectId]
        """
        return cast(FeatureModel, self.cached_model).feature_list_ids

    @property
    @substitute_docstring(
        doc_template=DEFINITION_DOC,
        examples=(
            """
            >>> feature = catalog.get_feature("InvoiceCount_60days")
            >>> feature_definition = feature.definition
            """
        ),
        format_kwargs={"object_type": "feature"},
    )
    def definition(self) -> str:
        return self._generate_definition()

    @property
    def feature_type(self) -> FeatureType:
        """
        Returns the feature type of the Feature object.

        Returns
        -------
        FeatureType
        """
        return self.feature_namespace.feature_type

    @typechecked
    def isin(self: FrozenSeriesT, other: Union[FrozenSeries, ScalarSequence]) -> FrozenSeriesT:
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
        return super().isin(other)

    def info(self, verbose: bool = False) -> Dict[str, Any]:
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

        Examples
        --------
        >>> feature = catalog.get_feature("InvoiceCount_60days")
        >>> feature.info()  # doctest: +SKIP
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
    def _list_handler(cls) -> ListHandler:
        return FeatureListHandler(
            route=cls._route,
            list_schema=cls._list_schema,
            list_fields=cls._list_fields,
            list_foreign_keys=cls._list_foreign_keys,
        )

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

        >>> Feature.list_versions(include_id=False)  # doctest: +ELLIPSIS
                                       name  version      dtype         readiness  online_enabled                                          tables    primary_tables           entities   primary_entities  created_at  is_default
        0    CustomerLatestInvoiceTimestamp      ...  TIMESTAMP  PRODUCTION_READY           False                                [GROCERYINVOICE]  [GROCERYINVOICE]  [grocerycustomer]  [grocerycustomer]         ...        True
        1                      InvoiceCount      ...        INT             DRAFT           False                  [GROCERYINVOICE, INVOICEITEMS]    [INVOICEITEMS]   [groceryinvoice]   [groceryinvoice]         ...        True
        2                ProductGroupLookup      ...    VARCHAR             DRAFT           False                                [GROCERYPRODUCT]  [GROCERYPRODUCT]   [groceryproduct]   [groceryproduct]         ...        True
        3  CustomerProductGroupTotalCost_7d      ...     OBJECT             DRAFT           False  [GROCERYINVOICE, INVOICEITEMS, GROCERYPRODUCT]    [INVOICEITEMS]  [grocerycustomer]  [grocerycustomer]         ...        True
        4    CustomerProductGroupCounts_90d      ...     OBJECT             DRAFT           False  [GROCERYINVOICE, INVOICEITEMS, GROCERYPRODUCT]    [INVOICEITEMS]  [grocerycustomer]  [grocerycustomer]         ...        True
        5     CustomerProductGroupCounts_7d      ...     OBJECT             DRAFT           False  [GROCERYINVOICE, INVOICEITEMS, GROCERYPRODUCT]    [INVOICEITEMS]  [grocerycustomer]  [grocerycustomer]         ...        True
        6           InvoiceAmountAvg_60days      ...      FLOAT  PRODUCTION_READY           False                                [GROCERYINVOICE]  [GROCERYINVOICE]  [grocerycustomer]  [grocerycustomer]         ...        True
        7               InvoiceCount_60days      ...      FLOAT  PRODUCTION_READY           False                                [GROCERYINVOICE]  [GROCERYINVOICE]  [grocerycustomer]  [grocerycustomer]         ...        True

        List Feature versions with the same name

        >>> feature = catalog.get_feature("InvoiceCount_60days")
        >>> feature.list_versions(include_id=False)  # doctest: +ELLIPSIS
                              name  version  dtype         readiness  online_enabled            tables    primary_tables           entities   primary_entities  created_at  is_default
            0  InvoiceCount_60days      ...  FLOAT  PRODUCTION_READY           False  [GROCERYINVOICE]  [GROCERYINVOICE]  [grocerycustomer]  [grocerycustomer]         ...  True
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
        >>> feature.list_versions(include_id=False)  # doctest: +ELLIPSIS
                              name  version  dtype         readiness  online_enabled            tables    primary_tables           entities   primary_entities  created_at  is_default
            0  InvoiceCount_60days      ...  FLOAT  PRODUCTION_READY           False  [GROCERYINVOICE]  [GROCERYINVOICE]  [grocerycustomer]  [grocerycustomer]         ...        True
        """
        output = self._list(include_id=True, params={"name": self.name})
        default_feature_id = self.feature_namespace.default_feature_id
        output["is_default"] = output["id"] == str(default_feature_id)
        columns = output.columns
        if not include_id:
            columns = [column for column in columns if column != "id"]
        return output[columns]

    def delete(self) -> None:
        """
        Deletes a feature from the persistent data store. A feature can only be deleted from the persistent data
        store if:

        - the feature readiness is DRAFT
        - the feature is not used in any feature list
        - the feature is not a default feature with manual version mode

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

    @property
    def feature_namespace(self) -> FeatureNamespace:
        """
        FeatureNamespace object of current feature

        Returns
        -------
        FeatureNamespace
        """
        feature_namespace_id = cast(FeatureModel, self.cached_model).feature_namespace_id
        return FeatureNamespace.get_by_id(id=feature_namespace_id)

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
        Whether this feature is online-enabled. An online-enabled feature is a feature that is used by at least
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
        operation_structure = self.graph.extract_operation_structure(
            self.node, keep_all_source_columns=True
        )
        return operation_structure.is_time_based

    @property
    def is_datetime(self) -> bool:
        """
        Returns whether the feature has a datetime data type.

        Returns
        -------
        bool
        """
        return super().is_datetime

    @property
    def is_numeric(self) -> bool:
        """
        Returns whether the feature has a numeric data type.

        Returns
        -------
        bool
        """
        return super().is_numeric

    @property
    def saved(self) -> bool:
        """
        Returns whether the Feature object is saved and part of the catalog.

        Returns
        -------
        bool
        """
        return super().saved

    @property
    def used_request_column(self) -> bool:
        """
        Returns whether the Feature object uses request column(s) in the computation.

        Returns
        -------
        bool
        """
        try:
            return self.cached_model.used_request_column
        except RecordRetrievalException:
            return self.graph.has_node_type(
                target_node=self.node, node_type=NodeType.REQUEST_COLUMN
            )

    @property
    def table_id_feature_job_settings(self) -> List[TableIdFeatureJobSetting]:
        """
        Return table feature job settings of tables used by the feature

        Returns
        -------
        List[TableFeatureJobSetting]
        """
        return self.graph.extract_table_id_feature_job_settings(target_node=self.node)

    @typechecked
    def save(
        self, conflict_resolution: ConflictResolution = "raise", _id: Optional[ObjectId] = None
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
        _id: Optional[ObjectId]
            The object ID to be used when saving the object. If not provided, a new object ID will be generated.

        Raises
        ------
        RecordCreationException
            When the feature object cannot be saved using feature definition.
        NotImplementedError
            When the feature data type is not supported for saving.

        Examples
        --------
        >>> grocery_invoice_view = catalog.get_view("GROCERYINVOICE")
        >>> invoice_amount_avg_60days = grocery_invoice_view.groupby(
        ...     "GroceryCustomerGuid"
        ... ).aggregate_over(
        ...     value_column="Amount",
        ...     method="avg",
        ...     feature_names=["InvoiceAmountAvg_60days"],
        ...     windows=["60d"],
        ... )["InvoiceAmountAvg_60days"]
        >>> invoice_amount_avg_60days.save()  # doctest: +SKIP
        """
        if self.dtype in DBVarType.not_supported_feature_save_types():
            raise NotImplementedError(
                f"Feature with data type {self.dtype} is not supported for saving."
            )

        if is_server_mode():
            # server mode save a feature by POST /feature/ endpoint directly without running the feature definition.
            super().save(conflict_resolution=conflict_resolution, _id=_id)
        else:
            # For non-server mode, a feature is saved by POST /feature/batch endpoint. A task is created to run the
            # feature definition and save the feature. The task is executed asynchronously. The feature definition is
            # validated before saving the feature.
            self._check_object_not_been_saved(conflict_resolution=conflict_resolution)
            pruned_graph, node_name_map = GlobalQueryGraph().quick_prune(
                target_node_names=[self.node_name]
            )
            feature_item = BatchFeatureItem(
                id=self.id,
                name=self.name,
                node_name=node_name_map[self.node_name],
                tabular_source=self.tabular_source,
            )
            try:
                self.post_async_task(
                    route="/feature/batch",
                    payload=BatchFeatureCreatePayload(
                        graph=pruned_graph,
                        features=[feature_item],
                    ).json_dict(),
                    retrieve_result=False,
                    has_output_url=False,
                )
                object_dict = self._get_object_dict_by_id(id_value=feature_item.id)
            except RecordCreationException as exc:
                traceback_message = exc.response.json()["traceback"]
                has_dup_exception = False
                if traceback_message:
                    has_dup_exception = "DuplicatedRecordException" in traceback_message
                if conflict_resolution == "retrieve" and has_dup_exception:
                    object_dict = self._get_object_dict_by_name(name=feature_item.name)
                else:
                    raise exc

            type(self).__init__(self, **object_dict, **self._get_init_params_from_object())

    @typechecked
    def astype(
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

        Examples
        --------
        >>> feature = catalog.get_feature("InvoiceCount_60days")
        >>> string_invoice_count_feature = feature.astype(str)
        >>> float_invoice_count_feature = feature.astype(float)
        """
        return super().astype(new_type=new_type)

    @substitute_docstring(
        doc_template=PREVIEW_DOC,
        description=(
            """
            Materializes a Feature object using a small observation set of up to 50 rows. Unlike compute_historical_features,
            this method does not store partial aggregations (tiles) to speed up future computation. Instead, it computes
            the feature values on the fly, and should be used only for small observation sets for debugging or prototyping
            unsaved features.
            """
        ),
        examples=(
            """
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
            """
        ),
        see_also=(
            """
            See Also
            --------
            - [FeatureGroup.preview](/reference/featurebyte.api.feature_group.FeatureGroup.preview/):
              Preview feature group.
            - [FeatureList.compute_historical_features](/reference/featurebyte.api.feature_list.FeatureList.compute_historical_features/):
              Get historical features from a feature list.
            """
        ),
        format_kwargs={"object_type": "feature"},
    )
    @enforce_observation_set_row_order
    @typechecked
    def preview(
        self,
        observation_set: Union[ObservationTable, pd.DataFrame],
    ) -> pd.DataFrame:
        return self._preview(observation_set=observation_set, url="/feature/preview")

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
            'period': '3600s',
            'offset': '90s',
            'execution_buffer': '0s'}}],
         'default': [{'table_name': 'GROCERYINVOICE',
           'feature_job_setting': {'blind_spot': '0s',
            'period': '3600s',
            'offset': '90s',
            'execution_buffer': '0s'}}]}

        Create a new feature with a different feature job setting:

        >>> new_feature = feature.create_new_version(
        ...     table_feature_job_settings=[
        ...         fb.TableFeatureJobSetting(
        ...             table_name="GROCERYINVOICE",
        ...             feature_job_setting=fb.FeatureJobSetting(
        ...                 blind_spot="60s",
        ...                 period="3600s",
        ...                 offset="90s",
        ...             ),
        ...         )
        ...     ]
        ... )
        >>> new_feature.info()["table_feature_job_setting"]
        {'this': [{'table_name': 'GROCERYINVOICE',
           'feature_job_setting': {'blind_spot': '60s',
            'period': '3600s',
            'offset': '90s',
            'execution_buffer': '0s'}}],
         'default': [{'table_name': 'GROCERYINVOICE',
           'feature_job_setting': {'blind_spot': '0s',
            'period': '3600s',
            'offset': '90s',
            'execution_buffer': '0s'}}]}

        Check table cleaning operation of this feature first:

        >>> feature = catalog.get_feature("InvoiceAmountAvg_60days")
        >>> feature.info()["table_cleaning_operation"]
        {'this': [], 'default': []}

        Create a new version of a feature with different table cleaning operations:

        >>> new_feature = feature.create_new_version(
        ...     table_cleaning_operations=[
        ...         fb.TableCleaningOperation(
        ...             table_name="GROCERYINVOICE",
        ...             column_cleaning_operations=[
        ...                 fb.ColumnCleaningOperation(
        ...                     column_name="Amount",
        ...                     cleaning_operations=[fb.MissingValueImputation(imputed_value=0.0)],
        ...                 )
        ...             ],
        ...         )
        ...     ]
        ... )
        >>> new_feature.info()["table_cleaning_operation"]
        {'this': [{'table_name': 'GROCERYINVOICE',
           'column_cleaning_operations': [{'column_name': 'Amount',
             'cleaning_operations': [{'type': 'missing', 'imputed_value': 0.0}]}]}],
         'default': []}

        Check the tables used by this feature first:

        >>> feature = catalog.get_feature("InvoiceAmountAvg_60days")
        >>> feature.info()["tables"]
        [{'name': 'GROCERYINVOICE', 'status': 'PUBLIC_DRAFT', 'catalog_name': 'grocery'}]

        Create a new version of a feature with irrelevant table cleaning operations (for example, the specified
        table name or column name is not used by the feature):

        >>> feature.create_new_version(
        ...     table_cleaning_operations=[
        ...         fb.TableCleaningOperation(
        ...             table_name="GROCERYPRODUCT",
        ...             column_cleaning_operations=[
        ...                 fb.ColumnCleaningOperation(
        ...                     column_name="GroceryProductGuid",
        ...                     cleaning_operations=[fb.MissingValueImputation(imputed_value=0)],
        ...                 )
        ...             ],
        ...         )
        ...     ]
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
                "table_feature_job_settings": (
                    [
                        table_feature_job_setting.model_dump()
                        for table_feature_job_setting in table_feature_job_settings
                    ]
                    if table_feature_job_settings
                    else None
                ),
                "table_cleaning_operations": (
                    [clean_ops.model_dump() for clean_ops in table_cleaning_operations]
                    if table_cleaning_operations
                    else None
                ),
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
        readiness: Union[FeatureReadiness, str],
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
        readiness: Union[FeatureReadiness, str]
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
        readiness_value = FeatureReadiness(readiness).value
        self.update(
            update_payload={"readiness": readiness_value, "ignore_guardrails": ignore_guardrails},
            allow_update_local=False,
        )

    @typechecked
    def update_default_version_mode(
        self, default_version_mode: Union[DefaultVersionMode, str]
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
        default_version_mode: Union[DefaultVersionMode, str]
            Feature default version mode.

        Examples
        --------

        >>> feature = catalog.get_feature("InvoiceCount_60days")
        >>> feature.update_default_version_mode("MANUAL")
        """
        mode_value = DefaultVersionMode(default_version_mode).value
        self.feature_namespace.update(
            update_payload={"default_version_mode": mode_value},
            allow_update_local=False,
        )

    @typechecked
    def update_feature_type(self, feature_type: Union[FeatureType, str]) -> None:
        """
        Update feature type of feature.

        Parameters
        ----------
        feature_type: Union[FeatureType, str]
            Feature type to be updated.

        Examples
        --------

        >>> feature = catalog.get_feature("InvoiceCount_60days")
        >>> feature.update_feature_type("numeric")
        """
        self.feature_namespace.update_feature_type(feature_type=feature_type)

    def as_default_version(self) -> None:
        """
        When a feature has its default version mode set to manual, this method designates the Feature object as the
        default version for that specific feature. Note that the specified feature must be a version with the highest
        level of readiness. This method is used when there are other version that share the same level of readiness
        as the default version and the user wants to manually set the default version.

        Each feature is recognized by its name and can possess numerous versions, though only a single default
        version is allowed.

        The default version streamlines feature reuse by supplying the most suitable version when none is explicitly
        indicated. By default, the feature's default version mode is automatic, selecting the version with the highest
        readiness level as the default. If several versions share the same readiness level, the most recent one
        becomes the default.

        Examples
        --------

        >>> feature = catalog.get_feature("InvoiceCount_60days")
        >>> feature.update_default_version_mode(fb.DefaultVersionMode.MANUAL)
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
        pruned_graph, mapped_node = self.extract_pruned_graph_and_node()
        payload = FeatureSQL(
            graph=QueryGraph(**pruned_graph.model_dump(by_alias=True)),
            node_name=mapped_node.name,
        )

        client = Configurations().get_client()
        response = client.post("/feature/sql", json=payload.json_dict())
        if response.status_code != HTTPStatus.OK:
            raise RecordRetrievalException(response)

        return cast(
            str,
            response.json(),
        )

    @substitute_docstring(
        doc_template=ISNULL_DOC,
        format_kwargs=DOCSTRING_FORMAT_PARAMS,
        examples=(
            """
            >>> feature = catalog.get_feature("InvoiceCount_60days")
            >>> new_feature = feature.isnull()
            """
        ),
    )
    def isnull(self) -> Feature:
        return super().isnull()

    @substitute_docstring(
        doc_template=NOTNULL_DOC,
        format_kwargs=DOCSTRING_FORMAT_PARAMS,
        examples=(
            """
            >>> feature = catalog.get_feature("InvoiceCount_60days")
            >>> new_feature = feature.notnull()
            """
        ),
    )
    def notnull(self) -> Feature:
        return super().notnull()

    # descriptors
    list_versions: ClassVar[ClassInstanceMethodDescriptor] = ClassInstanceMethodDescriptor(
        class_method=_list_versions,
        instance_method=_list_versions_with_same_name,
    )

    @typechecked
    def update_description(self, description: Optional[str]) -> None:
        """
        Update feature description

        Parameters
        ----------
        description: Optional[str]
            Description of feature
        """
        self.feature_namespace.update_description(description=description)

    @typechecked
    def update_version_description(self, description: Optional[str]) -> None:
        """
        Update feature version description

        Parameters
        ----------
        description: Optional[str]
            Description of feature version
        """
        super().update_description(description=description)
