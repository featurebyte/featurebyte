"""
BaseService class
"""
from __future__ import annotations

from typing import Any

from featurebyte.persistent.base import Persistent
from featurebyte.service.entity import EntityService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.mixin import OpsServiceMixin


class BaseService(OpsServiceMixin):
    """
    BaseService class has access to all document services as property.
    """

    def __init__(self, user: Any, persistent: Persistent):
        self.user = user
        self.persistent = persistent

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
