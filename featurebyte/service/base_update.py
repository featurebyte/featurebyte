"""
BaseUpdateService class
"""
from __future__ import annotations

from typing import Any, Optional

from bson.objectid import ObjectId

from featurebyte.models.feature import FeatureModel, FeatureNamespaceModel
from featurebyte.models.feature_list import FeatureListModel, FeatureListNamespaceModel
from featurebyte.persistent.base import Persistent
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.mixin import OpsServiceMixin


class BaseUpdateService(OpsServiceMixin):
    """
    BaseUpdateService
    """

    def __init__(self, user: Any, persistent: Persistent):
        self.user = user
        self.persistent = persistent

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

    async def get_feature_document(
        self, document_id: ObjectId, document: Optional[FeatureModel]
    ) -> FeatureModel:
        """
        Retrieve feature document given feature ID if the document is empty

        Parameters
        ----------
        document_id: ObjectId
            Feature ID
        document: Optional[FeatureModel]
            Feature object

        Returns
        -------
        FeatureModel
        """
        if document is None:
            document = await self.feature_service.get_document(document_id=document_id)
        return document

    async def get_feature_namespace_document(
        self, document_id: ObjectId, document: Optional[FeatureNamespaceModel]
    ) -> FeatureNamespaceModel:
        """
        Retrieve feature namespace document given feature namespace ID if the document is empty

        Parameters
        ----------
        document_id: ObjectId
            FeatureNamespace ID
        document: Optional[FeatureNamespaceModel]
            FeatureNamespace object

        Returns
        -------
        FeatureNamespaceModel
        """
        if document is None:
            document = await self.feature_namespace_service.get_document(document_id=document_id)
        return document

    async def get_feature_list_document(
        self, document_id: ObjectId, document: Optional[FeatureListModel]
    ) -> FeatureListModel:
        """
        Retrieve feature list document given feature list ID if the document is empty

        Parameters
        ----------
        document_id: ObjectId
            FeatureList ID
        document: Optional[FeatureListModel]
            FeatureList object

        Returns
        -------
        FeatureListModel
        """
        if document is None:
            document = await self.feature_list_service.get_document(document_id=document_id)
        return document

    async def get_feature_list_namespace_document(
        self, document_id: ObjectId, document: Optional[FeatureListNamespaceModel]
    ) -> FeatureListNamespaceModel:
        """
        Retrieve feature list namespace document given feature namespace list ID if the document is empty

        Parameters
        ----------
        document_id: ObjectId
            FeatureListNamespace ID
        document: Optional[FeatureListNamespaceModel]
            FeatureListNamespace object

        Returns
        -------
        FeatureListNamespaceModel
        """
        if document is None:
            document = await self.feature_list_namespace_service.get_document(
                document_id=document_id
            )
        return document
