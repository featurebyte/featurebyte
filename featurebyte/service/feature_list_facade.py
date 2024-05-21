"""
Feature List Facade Service which is responsible for handling high level feature list operations
"""

from typing import Any, Callable, Coroutine, Optional

from bson import ObjectId

from featurebyte.exception import DocumentDeletionError, DocumentNotFoundError
from featurebyte.models.feature_list import FeatureListModel
from featurebyte.models.feature_list_namespace import FeatureListNamespaceModel, FeatureListStatus
from featurebyte.models.feature_namespace import FeatureReadiness
from featurebyte.schema.feature_list import FeatureListNewVersionCreate, FeatureListServiceCreate
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService
from featurebyte.service.feature_list_status import FeatureListStatusService
from featurebyte.service.feature_readiness import FeatureReadinessService
from featurebyte.service.version import VersionService


class FeatureListFacadeService:
    """
    Feature List Facade Service which is responsible for handling high level feature list operations,
    and delegating the actual operations to the corresponding services.
    """

    def __init__(
        self,
        feature_list_service: FeatureListService,
        feature_list_namespace_service: FeatureListNamespaceService,
        feature_list_status_service: FeatureListStatusService,
        feature_readiness_service: FeatureReadinessService,
        version_service: VersionService,
    ):
        self.feature_list_service = feature_list_service
        self.feature_list_namespace_service = feature_list_namespace_service
        self.feature_list_status_service = feature_list_status_service
        self.feature_readiness_service = feature_readiness_service
        self.version_service = version_service

    async def create_feature_list(
        self,
        data: FeatureListServiceCreate,
        progress_callback: Optional[Callable[..., Coroutine[Any, Any, None]]] = None,
    ) -> FeatureListModel:
        """
        Create a new feature list

        Parameters
        ----------
        data: FeatureListServiceCreate
            Feature list service create payload
        progress_callback: Optional[Callable[..., Coroutine[Any, Any, None]]]
            Progress callback

        Returns
        -------
        FeatureListModel
        """
        document = await self.feature_list_service.create_document(
            data=data, progress_callback=progress_callback
        )
        await self.feature_readiness_service.update_feature_list_namespace(
            feature_list_namespace_id=document.feature_list_namespace_id,
        )
        output = await self.feature_list_service.get_document(document_id=document.id)
        return output

    async def create_new_version(self, data: FeatureListNewVersionCreate) -> FeatureListModel:
        """
        Create a new version of a feature list

        Parameters
        ----------
        data: FeatureListNewVersionCreate
            Feature list new version create payload

        Returns
        -------
        FeatureListModel
        """
        document = await self.version_service.create_new_feature_list_version(data=data)
        await self.feature_readiness_service.update_feature_list_namespace(
            feature_list_namespace_id=document.feature_list_namespace_id,
        )
        output = await self.feature_list_service.get_document(document_id=document.id)
        return output

    async def make_features_production_ready(
        self,
        feature_list_id: ObjectId,
        ignore_guardrails: bool = False,
        progress_callback: Optional[Callable[..., Coroutine[Any, Any, None]]] = None,
    ) -> None:
        """
        Make feature(s) of the given feature list production ready

        Parameters
        ----------
        feature_list_id: ObjectId
            Feature list id
        ignore_guardrails: bool
            Ignore guardrails of feature readiness update
        progress_callback: Optional[Callable[..., Coroutine[Any, Any, None]]]
            Progress callback to update progress
        """
        feature_list = await self.feature_list_service.get_document(
            document_id=feature_list_id, populate_remote_attributes=False
        )
        total_features = len(feature_list.feature_ids)
        for i, feature_id in enumerate(feature_list.feature_ids):
            await self.feature_readiness_service.update_feature(
                feature_id=feature_id,
                readiness=FeatureReadiness.PRODUCTION_READY,
                ignore_guardrails=ignore_guardrails,
            )
            if progress_callback:
                percent = int((i + 1) / total_features * 100)
                await progress_callback(
                    percent=percent,
                    message=f"Updated feature readiness for feature {i + 1} / {total_features}",
                )

        if progress_callback:
            await progress_callback(
                percent=100, message="Completed making all features production ready"
            )

    async def update_status(
        self, feature_list_namespace_id: ObjectId, status: FeatureListStatus
    ) -> FeatureListNamespaceModel:
        """
        Update status of a feature list namespace

        Parameters
        ----------
        feature_list_namespace_id: ObjectId
            Feature list namespace id to update
        status: FeatureListStatus
            Feature list namespace status

        Returns
        -------
        FeatureListNamespaceModel
        """
        await self.feature_list_status_service.update_feature_list_namespace_status(
            feature_list_namespace_id=feature_list_namespace_id,
            target_feature_list_status=status,
        )
        return await self.feature_list_namespace_service.get_document(
            document_id=feature_list_namespace_id
        )

    async def delete_feature_list(self, feature_list_id: ObjectId) -> None:
        """
        Delete a feature list

        Parameters
        ----------
        feature_list_id: ObjectId
            Feature list id to delete

        Raises
        ------
        DocumentDeletionError
            If feature list is not in DRAFT status
        """
        feature_list_doc = await self.feature_list_service.get_document_as_dict(
            document_id=feature_list_id
        )
        try:
            feature_list_namespace_id = feature_list_doc["feature_list_namespace_id"]
            feature_list_namespace = await self.feature_list_namespace_service.get_document(
                document_id=feature_list_namespace_id
            )
            if feature_list_namespace.status != FeatureListStatus.DRAFT:
                raise DocumentDeletionError("Only feature list with DRAFT status can be deleted.")

            await self.feature_readiness_service.update_feature_list_namespace(
                feature_list_namespace_id=feature_list_namespace_id,
                deleted_feature_list_ids=[feature_list_id],
            )
        except DocumentNotFoundError:
            # if feature list namespace is deleted, do nothing
            pass

        await self.feature_list_service.delete_document(document_id=feature_list_id)
