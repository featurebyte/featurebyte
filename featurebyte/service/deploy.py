"""
DeployService class
"""
from __future__ import annotations

from typing import Any, Optional

from bson.objectid import ObjectId

from featurebyte.exception import DocumentUpdateError
from featurebyte.models.feature import FeatureModel, FeatureReadiness
from featurebyte.models.feature_list import FeatureListModel, FeatureListNamespaceModel
from featurebyte.persistent import Persistent
from featurebyte.schema.feature import FeatureServiceUpdate
from featurebyte.schema.feature_list import FeatureListServiceUpdate
from featurebyte.schema.feature_list_namespace import FeatureListNamespaceServiceUpdate
from featurebyte.service.base_service import BaseService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService
from featurebyte.service.online_enable import OnlineEnableService


class DeployService(BaseService):
    """
    DeployService class is responsible for maintaining the feature & feature list structure
    of feature list deployment.
    """

    def __init__(self, user: Any, persistent: Persistent):
        super().__init__(user, persistent)
        self.feature_service = FeatureService(user=user, persistent=persistent)
        self.online_enable_service = OnlineEnableService(user=user, persistent=persistent)
        self.feature_list_service = FeatureListService(user=user, persistent=persistent)
        self.feature_list_namespace_service = FeatureListNamespaceService(
            user=user, persistent=persistent
        )

    @classmethod
    def _extract_deployed_feature_list_ids(
        cls, feature_list: FeatureListModel, document: FeatureListNamespaceModel | FeatureModel
    ) -> list[ObjectId]:
        if feature_list.deployed:
            return cls.include_object_id(document.deployed_feature_list_ids, feature_list.id)
        return cls.exclude_object_id(document.deployed_feature_list_ids, feature_list.id)

    async def _update_feature(
        self,
        feature_id: ObjectId,
        feature_list: FeatureListModel,
        get_credential: Any,
        return_document: bool = True,
    ) -> Optional[FeatureModel]:
        """
        Update deployed_feature_list_ids in feature. For each update, trigger online service to update
        online enabled status at feature level.

        Parameters
        ----------
        feature_id: ObjectId
            Target Feature ID
        feature_list: FeatureListModel
            Updated FeatureList object (deployed status)
        get_credential: Any
            Get credential handler function
        return_document: bool
            Whether to return updated document

        Returns
        -------
        Optional[FeatureModel]:
        """
        document = await self.feature_service.get_document(document_id=feature_id)
        deployed_feature_list_ids = self._extract_deployed_feature_list_ids(
            feature_list=feature_list, document=document
        )
        online_enabled = len(deployed_feature_list_ids) > 0
        if document.online_enabled != online_enabled:
            document = await self.online_enable_service.update_feature(
                feature_id=feature_id,
                online_enabled=online_enabled,
                get_credential=get_credential,
            )
        return await self.feature_service.update_document(
            document_id=feature_id,
            data=FeatureServiceUpdate(
                deployed_feature_list_ids=deployed_feature_list_ids,
            ),
            document=document,
            return_document=return_document,
        )

    async def _update_feature_list_namespace(
        self,
        feature_list_namespace_id: ObjectId,
        feature_list: FeatureListModel,
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
        return_document: bool
            Whether to return updated document

        Returns
        -------
        Optional[FeatureListNamespaceModel]
        """
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

    async def _validate_deployed_operation(
        self, feature_list: FeatureListModel, deployed: bool
    ) -> None:
        # if enabling deployment, check is there any feature with readiness not equal to production ready
        if deployed:
            query_filter = {"_id": {"$in": feature_list.feature_ids}}
            async for feature in self.feature_service.list_documents_iterator(
                query_filter=query_filter
            ):
                if FeatureReadiness(feature["readiness"]) != FeatureReadiness.PRODUCTION_READY:
                    raise DocumentUpdateError(
                        "Only FeatureList object of all production ready features can be deployed."
                    )

    async def update_feature_list(
        self,
        feature_list_id: ObjectId,
        deployed: bool,
        get_credential: Any,
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
        get_credential: Any
            Get credential handler function
        return_document: bool
            Whether to return updated document

        Returns
        -------
        Optional[FeatureListModel]
        """
        document = await self.feature_list_service.get_document(document_id=feature_list_id)
        if document.deployed != deployed:
            await self._validate_deployed_operation(document, deployed)
            async with self.persistent.start_transaction():
                feature_list = await self.feature_list_service.update_document(
                    document_id=feature_list_id,
                    data=FeatureListServiceUpdate(deployed=deployed),
                    document=document,
                    return_document=True,
                )
                assert isinstance(feature_list, FeatureListModel)
                await self._update_feature_list_namespace(
                    feature_list_namespace_id=feature_list.feature_list_namespace_id,
                    feature_list=feature_list,
                    return_document=False,
                )
                for feature_id in feature_list.feature_ids:
                    await self._update_feature(
                        feature_id=feature_id,
                        feature_list=feature_list,
                        get_credential=get_credential,
                        return_document=False,
                    )
                if return_document:
                    return await self.feature_list_service.get_document(document_id=feature_list_id)
        return self.conditional_return(document=document, condition=return_document)
