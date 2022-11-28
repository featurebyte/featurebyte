"""
BaseService class
"""
from __future__ import annotations

from typing import Any

from featurebyte.enum import StrEnum
from featurebyte.persistent.base import Persistent
from featurebyte.service.dimension_data import DimensionDataService
from featurebyte.service.entity import EntityService
from featurebyte.service.event_data import EventDataService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_job_setting_analysis import FeatureJobSettingAnalysisService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.item_data import ItemDataService
from featurebyte.service.mixin import OpsServiceMixin
from featurebyte.service.scd_data import SCDDataService
from featurebyte.service.semantic import SemanticService
from featurebyte.service.tabular_data import DataService


class BaseService(OpsServiceMixin):
    """
    BaseService class has access to all document services as property.
    """

    def __init__(self, user: Any, persistent: Persistent):
        self.user = user
        self.persistent = persistent

    @property
    def data_service(self) -> DataService:
        """
        DataService object

        Returns
        -------
        DataService
        """
        return DataService(user=self.user, persistent=self.persistent)

    @property
    def feature_store_service(self) -> FeatureStoreService:
        """
        FeatureStoreService object

        Returns
        -------
        FeatureStoreService
        """
        return FeatureStoreService(user=self.user, persistent=self.persistent)

    @property
    def entity_service(self) -> EntityService:
        """
        EntityService object

        Returns
        -------
        EntityService
        """
        return EntityService(user=self.user, persistent=self.persistent)

    @property
    def semantic_service(self) -> SemanticService:
        """
        SemanticService object

        Returns
        -------
        SemanticService
        """
        return SemanticService(user=self.user, persistent=self.persistent)

    @property
    def dimension_data_service(self) -> DimensionDataService:
        """
        DimensionDataService object

        Returns
        -------
        DimensionDataService
        """
        return DimensionDataService(user=self.user, persistent=self.persistent)

    @property
    def scd_data_service(self) -> SCDDataService:
        """
        SCDDataService object

        Returns
        -------
        SCDDataService
        """
        return SCDDataService(user=self.user, persistent=self.persistent)

    @property
    def event_data_service(self) -> EventDataService:
        """
        EventDataService object

        Returns
        -------
        EventDataService
        """
        return EventDataService(user=self.user, persistent=self.persistent)

    @property
    def item_data_service(self) -> ItemDataService:
        """
        ItemDataService object

        Returns
        -------
        ItemDataService
        """
        return ItemDataService(user=self.user, persistent=self.persistent)

    @property
    def feature_service(self) -> FeatureService:
        """
        FeatureService object

        Returns
        -------
        FeatureService
        """
        return FeatureService(user=self.user, persistent=self.persistent)

    @property
    def feature_namespace_service(self) -> FeatureNamespaceService:
        """
        FeatureNamespaceService object

        Returns
        -------
        FeatureNamespaceService
        """
        return FeatureNamespaceService(user=self.user, persistent=self.persistent)

    @property
    def feature_list_service(self) -> FeatureListService:
        """
        FeatureListService object

        Returns
        -------
        FeatureListService
        """
        return FeatureListService(user=self.user, persistent=self.persistent)

    @property
    def feature_list_namespace_service(self) -> FeatureListNamespaceService:
        """
        FeatureListNamespaceService object

        Returns
        -------
        FeatureListNamespaceService
        """
        return FeatureListNamespaceService(user=self.user, persistent=self.persistent)

    @property
    def feature_job_setting_analysis_service(self) -> FeatureJobSettingAnalysisService:
        """
        FeatureJobSettingAnalysisService object

        Returns
        -------
        FeatureJobSettingAnalysisService
        """
        return FeatureJobSettingAnalysisService(user=self.user, persistent=self.persistent)
