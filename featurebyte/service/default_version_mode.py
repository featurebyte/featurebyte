"""
DefaultVersionModeService class
"""
from __future__ import annotations

from typing import Optional

from bson.objectid import ObjectId

from featurebyte.models.feature import DefaultVersionMode, FeatureNamespaceModel
from featurebyte.models.feature_list import FeatureListNamespaceModel
from featurebyte.schema.feature_list_namespace import FeatureListNamespaceServiceUpdate
from featurebyte.schema.feature_namespace import FeatureNamespaceServiceUpdate
from featurebyte.service.base_update import BaseUpdateService
from featurebyte.service.feature_readiness import FeatureReadinessService


class DefaultVersionModeService(BaseUpdateService):
    """
    DefaultVersionModeService class is responsible for handling feature & feature list version mode.
    When there is a change in default version mode, this class will orchestrate feature readiness update
    through FeatureReadinessService.
    """

    @property
    def feature_readiness_service(self) -> FeatureReadinessService:
        """
        FeatureReadinessService object

        Returns
        -------
        FeatureReadinessService
        """
        return FeatureReadinessService(user=self.user, persistent=self.persistent)

    async def update_feature_default_version_mode(
        self,
        feature_namespace_id: ObjectId,
        default_version_mode: DefaultVersionMode,
        return_document: bool,
    ) -> Optional[FeatureNamespaceModel]:
        """
        Update feature namespace default version mode

        Parameters
        ----------
        feature_namespace_id: ObjectId
            Target FeatureNamespace ID
        default_version_mode: DefaultVersionMode
            Target default version mode
        return_document: bool
            Whether to return updated document

        Returns
        -------
        Optional[FeatureNamespaceModel]
        """
        namespace = await self.feature_namespace_service.get_document(
            document_id=feature_namespace_id
        )
        if namespace.default_version_mode != default_version_mode:
            await self.feature_namespace_service.update_document(
                document_id=feature_namespace_id,
                data=FeatureNamespaceServiceUpdate(default_version_mode=default_version_mode),
                document=namespace,
                return_document=False,
            )
            feature_namespace = await self.feature_readiness_service.update_feature_namespace(
                feature_namespace_id=feature_namespace_id,
                feature=None,
                return_document=return_document,
            )
            return feature_namespace
        if return_document:
            return namespace
        return None

    async def update_feature_list_default_version_mode(
        self,
        feature_list_namespace_id: ObjectId,
        default_version_mode: DefaultVersionMode,
        return_document: bool,
    ) -> Optional[FeatureListNamespaceModel]:
        """
        Update feature list namespace default version mode

        Parameters
        ----------
        feature_list_namespace_id: ObjectId
            Target FeatureListNamespace ID
        default_version_mode: DefaultVersionMode
            Target default version mode
        return_document: bool
            Whether to return updated document

        Returns
        -------
        Optional[FeatureListNamespaceModel]
        """
        namespace = await self.feature_list_namespace_service.get_document(
            document_id=feature_list_namespace_id
        )
        if namespace.default_version_mode != default_version_mode:
            await self.feature_list_namespace_service.update_document(
                document_id=feature_list_namespace_id,
                data=FeatureListNamespaceServiceUpdate(default_version_mode=default_version_mode),
                document=namespace,
                return_document=False,
            )
            feature_list_namespace = (
                await self.feature_readiness_service.update_feature_list_namespace(
                    feature_list_namespace_id=feature_list_namespace_id,
                    feature_list=None,
                    return_document=return_document,
                )
            )
            return feature_list_namespace
        if return_document:
            return namespace
        return None
