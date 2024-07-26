"""
Catalog - get_by_*_id mixin.
"""

from __future__ import annotations

from typing import Any

from bson import ObjectId

from featurebyte.api.batch_feature_table import BatchFeatureTable
from featurebyte.api.batch_request_table import BatchRequestTable
from featurebyte.api.catalog_decorator import update_and_reset_catalog
from featurebyte.api.context import Context
from featurebyte.api.data_source import DataSource
from featurebyte.api.deployment import Deployment
from featurebyte.api.entity import Entity
from featurebyte.api.feature import Feature
from featurebyte.api.feature_job_setting_analysis import FeatureJobSettingAnalysis
from featurebyte.api.feature_list import FeatureList
from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.historical_feature_table import HistoricalFeatureTable
from featurebyte.api.observation_table import ObservationTable
from featurebyte.api.periodic_task import PeriodicTask
from featurebyte.api.relationship import Relationship
from featurebyte.api.static_source_table import StaticSourceTable
from featurebyte.api.table import Table
from featurebyte.api.target import Target
from featurebyte.api.use_case import UseCase
from featurebyte.api.user_defined_function import UserDefinedFunction
from featurebyte.api.view import View


class CatalogGetByIdMixin:
    """
    Mixin to add get_by_id functionality into the catalog.
    """

    @update_and_reset_catalog
    def get_data_source_by_feature_store_id(
        self,
        id: ObjectId,
    ) -> DataSource:
        """
        Get data source by a given feature store id.

        Parameters
        ----------
        id: ObjectId
            Feature store id.

        Returns
        -------
        DataSource
            Data source object

        Examples
        --------
        Get data source.

        >>> data_source = catalog.get_data_source_by_feature_store_id(ObjectId())  # doctest: +SKIP
        """
        feature_store = FeatureStore.get_by_id(id=id)
        return feature_store.get_data_source()

    @update_and_reset_catalog
    def get_view_by_table_id(
        self,
        id: ObjectId,
    ) -> View:
        """
        Get view for a given table id.

        Parameters
        ----------
        id: ObjectId
            Table id.

        Returns
        -------
        View
            View object.

        Examples
        --------
        Get an event view from an event table.

        >>> event_view = catalog.get_view_by_table_id(ObjectId())  # doctest: +SKIP
        """
        table = Table.get_by_id(id=id)
        return table.get_view()  # type: ignore[no-any-return]

    @update_and_reset_catalog
    def get_feature_by_id(
        self,
        id: ObjectId,
    ) -> Feature:
        """
        Returns a Feature object by its unique identifier (ID).

        Parameters
        ----------
        id: ObjectId
            Feature id.

        Returns
        -------
        Feature
            Feature object.

        Examples
        --------
        Get a saved feature.

        >>> feature = catalog.get_feature_by_id(<Feature_Object_Id>)  # doctest: +SKIP
        """
        return Feature.get_by_id(id=id)

    @update_and_reset_catalog
    def get_feature_list_by_id(
        self,
        id: ObjectId,
    ) -> FeatureList:
        """
        Gets a FeatureList object by its unique identifier (ID).

        Parameters
        ----------
        id: ObjectId
            FeatureList unique identifier (ID) to retrieve.

        Returns
        -------
        FeatureList
            Feature list object.

        Examples
        --------
        Get a saved feature list.

        >>> feature_list = catalog.get_feature_list_by_id(<Feature_Object_ID>)  # doctest: +SKIP
        """
        return FeatureList.get_by_id(id=id)

    @update_and_reset_catalog
    def get_table_by_id(
        self,
        id: ObjectId,
    ) -> Any:
        """
        Returns a Table object by its unique identifier (ID).

        Parameters
        ----------
        id: ObjectId
            Table unique identifier (ID)  to retrieve.

        Returns
        -------
        Any
            Retrieved table.

        Examples
        --------
        Get a saved table.

        >>> item_table = catalog.get_table_by_id(<table_Object_ID>)  # doctest: +SKIP
        """
        return Table.get_by_id(id=id)

    @update_and_reset_catalog
    def get_relationship_by_id(
        self,
        id: ObjectId,
    ) -> Relationship:
        """
        Returns a Relationship object by its unique identifier (ID).

        Parameters
        ----------
        id: ObjectId
            Relationship object unique identifier ID.

        Returns
        -------
        Relationship
            Relationship object.

        Examples
        --------
        Get a saved relationship.

        >>> relationship = catalog.get_relationship_by_id(<Relationship_Object_ID>)  # doctest: +SKIP
        """
        return Relationship.get_by_id(id=id)

    @update_and_reset_catalog
    def get_feature_job_setting_analysis_by_id(
        self,
        id: ObjectId,
    ) -> FeatureJobSettingAnalysis:
        """
        Get feature job setting analysis by id.

        Parameters
        ----------
        id: ObjectId
            Feature job setting analysis id.

        Returns
        -------
        FeatureJobSettingAnalysis
            Feature job setting analysis object.

        Examples
        --------
        Get a saved feature job setting analysis.

        >>> feature_job_setting_analysis = catalog.get_feature_job_setting_analysis_by_id(
        ...     ObjectId()
        ... )  # doctest: +SKIP
        """
        return FeatureJobSettingAnalysis.get_by_id(id=id)

    @update_and_reset_catalog
    def get_feature_store_by_id(
        self,
        id: ObjectId,
    ) -> FeatureStore:
        """
        Get feature store by id.

        Parameters
        ----------
        id: ObjectId
            FeatureStore unique identifier (ID) to retrieve.

        Returns
        -------
        FeatureStore
            Feature store object.

        Examples
        --------
        Get a saved feature store.

        >>> feature_store = catalog.get_feature_store_by_id(ObjectId())  # doctest: +SKIP
        """
        return FeatureStore.get_by_id(id=id)

    @update_and_reset_catalog
    def get_entity_by_id(
        self,
        id: ObjectId,
    ) -> Entity:
        """
        Returns an Entity object by its unique identifier (ID).

        Parameters
        ----------
        id: ObjectId
            Entity id.

        Returns
        -------
        Entity
            Entity object.

        Examples
        --------
        Get a saved entity.

        >>> entity = catalog.get_entity_by_id(<Entity_Object_ID>)  # doctest: +SKIP
        """
        return Entity.get_by_id(id=id)

    @update_and_reset_catalog
    def get_periodic_task_by_id(
        self,
        id: ObjectId,
    ) -> PeriodicTask:
        """
        Get periodic task by id.

        Parameters
        ----------
        id: ObjectId
            Periodic task id.

        Returns
        -------
        PeriodicTask
            Periodic task object.

        Examples
        --------
        Get a saved periodic task.

        >>> periodic_task = catalog.get_periodic_task_by_id(ObjectId())  # doctest: +SKIP
        """
        return PeriodicTask.get_by_id(id=id)

    @update_and_reset_catalog
    def get_observation_table_by_id(
        self,
        id: ObjectId,
    ) -> ObservationTable:
        """
        Get observation table by id.

        Parameters
        ----------
        id: ObjectId
            Observation table id.

        Returns
        -------
        ObservationTable
            Observation table object.

        Examples
        --------
        Get a saved observation table.

        >>> observation_table = catalog.get_observation_table_by_id(ObjectId())  # doctest: +SKIP
        """
        return ObservationTable.get_by_id(id=id)

    @update_and_reset_catalog
    def get_historical_feature_table_by_id(
        self,
        id: ObjectId,
    ) -> HistoricalFeatureTable:
        """
        Get historical feature table by id.

        Parameters
        ----------
        id: ObjectId
            Historical feature table id.

        Returns
        -------
        HistoricalFeatureTable
            Historical feature table object.

        Examples
        --------
        Get a saved historical feature table.

        >>> historiacl_feature_table = catalog.get_historical_feature_table_by_id(
        ...     ObjectId()
        ... )  # doctest: +SKIP
        """
        return HistoricalFeatureTable.get_by_id(id=id)

    @update_and_reset_catalog
    def get_batch_request_table_by_id(
        self,
        id: ObjectId,
    ) -> BatchRequestTable:
        """
        Get batch request table by id.

        Parameters
        ----------
        id: ObjectId
            Batch request table id.

        Returns
        -------
        BatchRequestTable
            Batch request table object.

        Examples
        --------
        Get a saved batch request table.

        >>> batch_request_table = catalog.get_batch_request_table_by_id(
        ...     ObjectId()
        ... )  # doctest: +SKIP
        """
        return BatchRequestTable.get_by_id(id=id)

    @update_and_reset_catalog
    def get_batch_feature_table_by_id(
        self,
        id: ObjectId,
    ) -> BatchFeatureTable:
        """
        Get batch feature table task by id.

        Parameters
        ----------
        id: ObjectId
            Batch feature table id.

        Returns
        -------
        BatchFeatureTable
            Batch feature table object.

        Examples
        --------
        Get a saved batch feature table.

        >>> batch_feature_table = catalog.get_batch_feature_table_by_id(
        ...     ObjectId()
        ... )  # doctest: +SKIP
        """
        return BatchFeatureTable.get_by_id(id=id)

    @update_and_reset_catalog
    def get_static_source_table_by_id(
        self,
        id: ObjectId,
    ) -> StaticSourceTable:
        """
        Get static source table by id.

        Parameters
        ----------
        id: ObjectId
            Static source table id.

        Returns
        -------
        StaticSourceTable
            Static source table object.

        Examples
        --------
        Get a saved static source table.

        >>> static_source_table = catalog.get_static_source_table_by_id(
        ...     ObjectId()
        ... )  # doctest: +SKIP
        """
        return StaticSourceTable.get_by_id(id=id)

    @update_and_reset_catalog
    def get_deployment_by_id(
        self,
        id: ObjectId,
    ) -> Deployment:
        """
        Get deployment by id.

        Parameters
        ----------
        id: ObjectId
            Deployment id.

        Returns
        -------
        Deployment
            Deployment object.

        Examples
        --------
        Get a saved deployment.

        >>> deployment = catalog.get_deployment_by_id(ObjectId())  # doctest: +SKIP
        """
        return Deployment.get_by_id(id=id)

    @update_and_reset_catalog
    def get_user_defined_function_by_id(
        self,
        id: ObjectId,
    ) -> UserDefinedFunction:
        """
        Get user defined function by id.

        Parameters
        ----------
        id: ObjectId
            User defined function id.

        Returns
        -------
        UserDefinedFunction
            User defined function object.

        Examples
        --------
        Get a saved user defined function.

        >>> user_defined_function = catalog.get_user_defined_function_by_id(
        ...     ObjectId()
        ... )  # doctest: +SKIP
        """
        return UserDefinedFunction.get_by_id(id=id)

    @update_and_reset_catalog
    def get_target_by_id(
        self,
        id: ObjectId,
    ) -> Target:
        """
        Get target by id.

        Parameters
        ----------
        id: ObjectId
            Target id.

        Returns
        -------
        Target
            Target object.

        Examples
        --------
        Get a saved target .

        >>> target = catalog.get_target_by_id(ObjectId())  # doctest: +SKIP
        """
        return Target.get_by_id(id=id)

    @update_and_reset_catalog
    def get_use_case_by_id(
        self,
        id: ObjectId,
    ) -> UseCase:
        """
        Get use case by id.

        Parameters
        ----------
        id: ObjectId
            UseCase id.

        Returns
        -------
        UseCase
            UseCase object.

        Examples
        --------
        Get a saved UseCase .

        >>> target = catalog.get_use_case_by_id(ObjectId())  # doctest: +SKIP
        """
        return UseCase.get_by_id(id=id)

    @update_and_reset_catalog
    def get_context_by_id(
        self,
        id: ObjectId,
    ) -> Context:
        """
        Get context by id.

        Parameters
        ----------
        id: ObjectId
            Context id.

        Returns
        -------
        Context
            Context object.

        Examples
        --------
        Get a saved context.

        >>> context = catalog.get_context_by_id(ObjectId())  # doctest: +SKIP
        """
        return Context.get_by_id(id=id)
