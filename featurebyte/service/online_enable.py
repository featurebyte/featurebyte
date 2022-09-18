"""
OnlineEnableService class
"""
from __future__ import annotations

from typing import Optional

from bson.objectid import ObjectId

from featurebyte.exception import DocumentUpdateError
from featurebyte.models.feature import FeatureModel, FeatureNamespaceModel, FeatureReadiness
from featurebyte.models.feature_list import FeatureListModel
from featurebyte.schema.feature import FeatureServiceUpdate
from featurebyte.schema.feature_list import FeatureListServiceUpdate
from featurebyte.schema.feature_namespace import FeatureNamespaceServiceUpdate
from featurebyte.service.base_update import BaseUpdateService


class OnlineEnableService(BaseUpdateService):
    """
    OnlineEnableService class is responsible for maintaining the feature & feature list structure
    of feature online enablement.
    """

    @classmethod
    def _extract_online_enabled_feature_ids(
        cls, feature: FeatureModel, document: FeatureListModel | FeatureNamespaceModel
    ) -> list[ObjectId]:
        if feature.online_enabled:
            return cls.include_object_id(document.online_enabled_feature_ids, feature.id)
        return cls.exclude_object_id(document.online_enabled_feature_ids, feature.id)

    async def update_feature_list(
        self,
        feature_list_id: ObjectId,
        feature: FeatureModel,
        document: Optional[FeatureListModel] = None,
        return_document: bool = True,
    ) -> Optional[FeatureListModel]:
        """
        Update online_enabled_feature_ids in feature list

        Parameters
        ----------
        feature_list_id: ObjectId
            Target feature list ID
        feature: FeatureModel
            Updated Feature object
        document: Optional[FeatureListModel]
            Document to be updated (when provided, this method won't query persistent for retrieval)
        return_document: bool
            Whether to return updated document

        Returns
        -------
        Optional[FeatureListModel]
        """
        if document is None:
            document = await self.feature_list_service.get_document(document_id=feature_list_id)

        return await self.feature_list_service.update_document(
            document_id=feature_list_id,
            data=FeatureListServiceUpdate(
                online_enabled_feature_ids=self._extract_online_enabled_feature_ids(
                    feature=feature, document=document
                ),
            ),
            document=document,
            return_document=return_document,
        )

    async def update_feature_namespace(
        self,
        feature_namespace_id: ObjectId,
        feature: FeatureModel,
        document: Optional[FeatureNamespaceModel] = None,
        return_document: bool = True,
    ) -> Optional[FeatureNamespaceModel]:
        """
        Update online_enabled_feature_ids in feature namespace

        Parameters
        ----------
        feature_namespace_id: ObjectId
            FeatureNamespace ID
        feature: FeatureModel
            Updated Feature object
        document: Optional[FeatureNamespaceModel]
            Document to be updated (when provided, this method won't query persistent for retrieval)
        return_document: bool
            Whether to return updated document

        Returns
        -------
        Optional[FeatureNamespaceModel]
        """
        if document is None:
            document = await self.feature_namespace_service.get_document(
                document_id=feature_namespace_id
            )

        return await self.feature_namespace_service.update_document(
            document_id=feature_namespace_id,
            data=FeatureNamespaceServiceUpdate(
                online_enabled_feature_ids=self._extract_online_enabled_feature_ids(
                    feature=feature, document=document
                ),
            ),
            document=document,
            return_document=return_document,
        )

    @staticmethod
    def _validate_online_enabled_operation(feature: FeatureModel, online_enabled: bool) -> None:
        if online_enabled and feature.readiness != FeatureReadiness.PRODUCTION_READY:
            raise DocumentUpdateError(
                f"Only Feature object with {FeatureReadiness.PRODUCTION_READY} readiness can be online enabled "
                f"(readiness: {feature.readiness})."
            )
        if not online_enabled and feature.deployed_feature_list_ids:
            raise DocumentUpdateError(
                "Failed to online disable the feature as it has been used by at least one deployed "
                "FeatureList object."
            )

    async def update_feature(
        self,
        feature_id: ObjectId,
        online_enabled: bool,
        document: Optional[FeatureModel] = None,
        return_document: bool = True,
    ) -> Optional[FeatureModel]:
        """
        Update feature online enabled & trigger list of cascading updates

        Parameters
        ----------
        feature_id: ObjectId
            Target feature ID
        online_enabled: bool
            Value to update the feature online_enabled status
        document: Optional[FeatureModel]
            Document to be updated (when provided, this method won't query persistent for retrieval)
        return_document: bool
            Whether to return updated document

        Returns
        -------
        Optional[FeatureModel]
        """
        if document is None:
            document = await self.feature_service.get_document(document_id=feature_id)

        if document.online_enabled != online_enabled:
            self._validate_online_enabled_operation(document, online_enabled)
            async with self.persistent.start_transaction():
                feature = await self.feature_service.update_document(
                    document_id=feature_id,
                    data=FeatureServiceUpdate(online_enabled=online_enabled),
                    document=document,
                    return_document=True,
                )
                assert isinstance(feature, FeatureModel)
                await self.update_feature_namespace(
                    feature_namespace_id=feature.feature_namespace_id,
                    feature=feature,
                    return_document=False,
                )
                for feature_list_id in feature.feature_list_ids:
                    await self.update_feature_list(
                        feature_list_id=feature_list_id,
                        feature=feature,
                        return_document=False,
                    )
                if return_document:
                    return feature
                return None

        if return_document:
            return document
        return None
