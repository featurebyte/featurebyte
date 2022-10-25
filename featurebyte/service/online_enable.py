"""
OnlineEnableService class
"""
from __future__ import annotations

from typing import Optional

from bson.objectid import ObjectId

from featurebyte.models.feature import FeatureModel, FeatureNamespaceModel
from featurebyte.models.feature_list import FeatureListModel
from featurebyte.schema.feature import FeatureServiceUpdate
from featurebyte.schema.feature_list import FeatureListServiceUpdate
from featurebyte.schema.feature_namespace import FeatureNamespaceServiceUpdate
from featurebyte.service.base_service import BaseService, DocServiceName


class OnlineEnableService(BaseService):
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

    async def _update_feature_list(
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
        document = await self.get_document(
            DocServiceName.FEATURE_LIST, feature_list_id, document=document
        )
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

    async def _update_feature_namespace(
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
        document = await self.get_document(
            DocServiceName.FEATURE_NAMESPACE, feature_namespace_id, document=document
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
        document = await self.get_document(DocServiceName.FEATURE, feature_id, document=document)
        if document.online_enabled != online_enabled:
            async with self.persistent.start_transaction():
                feature = await self.feature_service.update_document(
                    document_id=feature_id,
                    data=FeatureServiceUpdate(online_enabled=online_enabled),
                    document=document,
                    return_document=True,
                )
                assert isinstance(feature, FeatureModel)
                await self._update_feature_namespace(
                    feature_namespace_id=feature.feature_namespace_id,
                    feature=feature,
                    return_document=False,
                )
                for feature_list_id in feature.feature_list_ids:
                    await self._update_feature_list(
                        feature_list_id=feature_list_id,
                        feature=feature,
                        return_document=False,
                    )
                if return_document:
                    return await self.get_document(DocServiceName.FEATURE, feature_id)
        return self.conditional_return(document=document, condition=return_document)
