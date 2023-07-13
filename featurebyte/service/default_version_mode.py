"""
DefaultVersionModeService class
"""
from __future__ import annotations

from typing import Optional

from bson.objectid import ObjectId

from featurebyte.models.feature_list import FeatureListNamespaceModel
from featurebyte.models.feature_namespace import DefaultVersionMode, FeatureNamespaceModel
from featurebyte.schema.feature_list_namespace import FeatureListNamespaceServiceUpdate
from featurebyte.schema.feature_namespace import FeatureNamespaceServiceUpdate
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.feature_readiness import FeatureReadinessService
from featurebyte.service.mixin import OpsServiceMixin


class DefaultVersionModeService(OpsServiceMixin):
    """
    DefaultVersionModeService class is responsible for handling feature & feature list version mode.
    When there is a change in default version mode, this class will orchestrate feature readiness update
    through FeatureReadinessService.
    """

    def __init__(
        self,
        feature_namespace_service: FeatureNamespaceService,
        feature_readiness_service: FeatureReadinessService,
        feature_list_namespace_service: FeatureListNamespaceService,
    ):
        self.feature_namespace_service = feature_namespace_service
        self.feature_readiness_service = feature_readiness_service
        self.feature_list_namespace_service = feature_list_namespace_service

    async def update_feature_namespace(
        self,
        feature_namespace_id: ObjectId,
        default_version_mode: DefaultVersionMode,
        return_document: bool = True,
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
        document = await self.feature_namespace_service.get_document(
            document_id=feature_namespace_id
        )
        if document.default_version_mode != default_version_mode:
            await self.feature_namespace_service.update_document(
                document_id=feature_namespace_id,
                data=FeatureNamespaceServiceUpdate(default_version_mode=default_version_mode),
                document=document,
                return_document=False,
            )
            feature_namespace = await self.feature_readiness_service.update_feature_namespace(
                feature_namespace_id=feature_namespace_id,
                return_document=return_document,
            )
            return feature_namespace
        return self.conditional_return(document=document, condition=return_document)

    async def update_feature_list_namespace(
        self,
        feature_list_namespace_id: ObjectId,
        default_version_mode: DefaultVersionMode,
        return_document: bool = True,
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
        document = await self.feature_list_namespace_service.get_document(
            document_id=feature_list_namespace_id
        )
        if document.default_version_mode != default_version_mode:
            await self.feature_list_namespace_service.update_document(
                document_id=feature_list_namespace_id,
                data=FeatureListNamespaceServiceUpdate(default_version_mode=default_version_mode),
                document=document,
                return_document=False,
            )
            feature_list_namespace = (
                await self.feature_readiness_service.update_feature_list_namespace(
                    feature_list_namespace_id=feature_list_namespace_id,
                    return_document=return_document,
                )
            )
            return feature_list_namespace
        return self.conditional_return(document=document, condition=return_document)
