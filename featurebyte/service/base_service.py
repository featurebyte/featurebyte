"""
BaseService class
"""
from __future__ import annotations

from typing import Any

from featurebyte.persistent.base import Persistent
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService
from featurebyte.service.mixin import OpsServiceMixin


class BaseService(OpsServiceMixin):
    """
    BaseService class has access to all document services as property.
    """

    def __init__(self, user: Any, persistent: Persistent):
        self.user = user
        self.persistent = persistent

    @property
    def feature_list_namespace_service(self) -> FeatureListNamespaceService:
        """
        FeatureListNamespaceService object

        Returns
        -------
        FeatureListNamespaceService
        """
        return FeatureListNamespaceService(user=self.user, persistent=self.persistent)
