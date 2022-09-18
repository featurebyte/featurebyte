"""
DeployService class
"""
from __future__ import annotations

from typing import Optional

from bson.objectid import ObjectId

from featurebyte.exception import DocumentUpdateError
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_list import FeatureListModel, FeatureListNamespaceModel
from featurebyte.schema.feature import FeatureServiceUpdate
from featurebyte.schema.feature_list import FeatureListServiceUpdate
from featurebyte.schema.feature_list_namespace import FeatureListNamespaceServiceUpdate
from featurebyte.service.base_update import BaseUpdateService


class DeployService(BaseUpdateService):
    """
    DeployService class is responsible for maintaining the feature & feature list structure
    of feature list deployment.
    """

    @classmethod
    def _extract_deployed_feature_list_ids(
        cls, feature_list: FeatureListModel, document: FeatureListNamespaceModel | FeatureModel
    ) -> list[ObjectId]:
        if feature_list.deployed:
            return cls.include_object_id(document.deployed_feature_list_ids, feature_list.id)
        return cls.exclude_object_id(document.deployed_feature_list_ids, feature_list.id)

    async def update_feature(
        self,
        feature_id: ObjectId,
        feature_list: FeatureListModel,
        document: Optional[FeatureModel] = None,
        return_document: bool = True,
    ) -> Optional[FeatureModel]:
        """
        Update deployed_feature_list_ids in feature

        Parameters
        ----------
        feature_id: ObjectId
            Target Feature ID
        feature_list: FeatureListModel
            Updated FeatureList object (deployed status)
        document: Optional[FeatureListNamespaceModel]
            Document to be updated (when provided, this method won't query persistent for retrieval)
        return_document: bool
            Whether to return updated document

        Returns
        -------
        Optional[FeatureModel]:
        """
        if document is None:
            document = await self.feature_service.get_document(document_id=feature_id)

        return await self.feature_service.update_document(
            document_id=feature_id,
            data=FeatureServiceUpdate(
                deployed_feature_list_ids=self._extract_deployed_feature_list_ids(
                    feature_list=feature_list,
                    document=document,
                ),
            ),
            document=document,
            return_document=return_document,
        )

    async def update_feature_list_namespace(
        self,
        feature_list_namespace_id: ObjectId,
        feature_list: FeatureListModel,
        document: Optional[FeatureListNamespaceModel] = None,
        return_document: bool = True,
    ) -> Optional[FeatureListNamespaceModel]:
        """
        Update deployed_feature_list_ids in feature list namespace

        Parameters
        ----------
        feature_list_namespace_id: ObjectId
            Target FeatureListNamespace ID
        feature_list: FeatureListModel
            Updated FeatureList object (deployed status)
        document: Optional[FeatureListNamespaceModel]
            Document to be updated (when provided, this method won't query persistent for retrieval)
        return_document: bool
            Whether to return updated document

        Returns
        -------
        Optional[FeatureListNamespaceModel]
        """
        if document is None:
            document = await self.feature_list_namespace_service.get_document(
                document_id=feature_list_namespace_id
            )

        return await self.feature_list_namespace_service.update_document(
            document_id=feature_list_namespace_id,
            data=FeatureListNamespaceServiceUpdate(
                deployed_feature_list_ids=self._extract_deployed_feature_list_ids(
                    feature_list=feature_list,
                    document=document,
                ),
            ),
            document=document,
            return_document=return_document,
        )

    @staticmethod
    def _validate_deployed_operation(feature_list: FeatureListModel) -> None:
        if feature_list.online_enabled_feature_ids != feature_list.feature_ids:
            raise DocumentUpdateError(
                "Only FeatureList object will all features online enabled can be deployed."
            )

    async def update_feature_list(
        self,
        feature_list_id: ObjectId,
        deployed: bool,
        document: Optional[FeatureListModel] = None,
        return_document: bool = True,
    ) -> Optional[FeatureListModel]:
        """
        Update deployed status in feature list

        Parameters
        ----------
        feature_list_id: ObjectId
            Target feature list ID
        deployed: bool
            Target deployed status
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

        if document.deployed != deployed:
            self._validate_deployed_operation(document)
            async with self.persistent.start_transaction():
                feature_list = await self.feature_list_service.update_document(
                    document_id=feature_list_id,
                    data=FeatureListServiceUpdate(deployed=deployed),
                    document=document,
                    return_document=True,
                )
                assert isinstance(feature_list, FeatureListModel)
                await self.update_feature_list_namespace(
                    feature_list_namespace_id=feature_list.feature_list_namespace_id,
                    feature_list=feature_list,
                    return_document=False,
                )
                for feature_id in feature_list.feature_ids:
                    await self.update_feature(
                        feature_id=feature_id,
                        feature_list=feature_list,
                        return_document=False,
                    )
                if return_document:
                    return feature_list
                return None

        if return_document:
            return document
        return None
