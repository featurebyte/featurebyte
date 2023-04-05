"""
Catalog - get_by_*_id mixin.
"""
from typing import Any

from bson import ObjectId

from featurebyte.api.catalog_decorator import update_and_reset_catalog
from featurebyte.api.data_source import DataSource
from featurebyte.api.entity import Entity
from featurebyte.api.feature import Feature
from featurebyte.api.feature_job_setting_analysis import FeatureJobSettingAnalysis
from featurebyte.api.feature_list import FeatureList
from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.periodic_task import PeriodicTask
from featurebyte.api.relationship import Relationship
from featurebyte.api.table import Table
from featurebyte.api.view import View


class CatalogGetByIdMixin:
    """
    Mixin to add get_by_id functionality into the catalog.
    """

    @update_and_reset_catalog
    def get_data_source_by_feature_store_id(
        self, id: ObjectId  # pylint: disable=redefined-builtin,invalid-name
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
        self, id: ObjectId  # pylint: disable=redefined-builtin,invalid-name
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
        self, id: ObjectId  # pylint: disable=redefined-builtin,invalid-name
    ) -> Feature:
        """
        Get a feature by id.

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

        >>> feature = catalog.get_feature_by_id(ObjectId())  # doctest: +SKIP
        """
        return Feature.get_by_id(id=id)

    @update_and_reset_catalog
    def get_feature_list_by_id(
        self, id: ObjectId  # pylint: disable=redefined-builtin,invalid-name
    ) -> FeatureList:
        """
        Get feature list by id.

        Parameters
        ----------
        id: ObjectId
            Feature list id.

        Returns
        -------
        FeatureList
            Feature list object.

        Examples
        --------
        Get a saved feature list.

        >>> feature_list = catalog.get_feature_list_by_id(ObjectId())  # doctest: +SKIP
        """
        return FeatureList.get_by_id(id=id)

    @update_and_reset_catalog
    def get_table_by_id(
        self, id: ObjectId  # pylint: disable=redefined-builtin,invalid-name
    ) -> Any:
        """
        Get table by id.

        Parameters
        ----------
        id: ObjectId
            Table id.

        Returns
        -------
        Any
            Retrieved table.

        Examples
        --------
        Get a saved table.

        >>> item_table = catalog.get_table_by_id(ObjectId())  # doctest: +SKIP
        """
        return Table.get_by_id(id=id)

    @update_and_reset_catalog
    def get_relationship_by_id(
        self, id: ObjectId  # pylint: disable=redefined-builtin,invalid-name
    ) -> Relationship:
        """
        Get relationship by id.

        Parameters
        ----------
        id: ObjectId
            Relationship id.

        Returns
        -------
        Relationship
            Relationship object.

        Examples
        --------
        Get a saved relationship.

        >>> relationship = catalog.get_relationship_by_id(ObjectId())  # doctest: +SKIP
        """
        return Relationship.get_by_id(id=id)

    @update_and_reset_catalog
    def get_feature_job_setting_analysis_by_id(
        self, id: ObjectId  # pylint: disable=redefined-builtin,invalid-name
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

        >>> feature_job_setting_analysis = catalog.get_feature_job_setting_analysis_by_id(ObjectId())  # doctest: +SKIP
        """
        return FeatureJobSettingAnalysis.get_by_id(id=id)

    @update_and_reset_catalog
    def get_feature_store_by_id(
        self, id: ObjectId  # pylint: disable=redefined-builtin,invalid-name
    ) -> FeatureStore:
        """
        Get feature store by id.

        Parameters
        ----------
        id: ObjectId
            Feature store id.

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
        self, id: ObjectId  # pylint: disable=redefined-builtin,invalid-name
    ) -> Entity:
        """
        Get entity by id.

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

        >>> entity = catalog.get_entity_by_id(ObjectId())  # doctest: +SKIP
        """
        return Entity.get_by_id(id=id)

    @update_and_reset_catalog
    def get_periodic_task_by_id(
        self, id: ObjectId  # pylint: disable=redefined-builtin,invalid-name
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
