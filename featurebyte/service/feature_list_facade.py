"""
Feature List Facade Service which is responsible for handling high level feature list operations
"""
from bson import ObjectId

from featurebyte.exception import DocumentDeletionError, DocumentNotFoundError, DocumentUpdateError
from featurebyte.models.feature_list import (
    FeatureListModel,
    FeatureListNamespaceModel,
    FeatureListStatus,
)
from featurebyte.models.feature_namespace import DefaultVersionMode, FeatureReadiness
from featurebyte.schema.feature_list import FeatureListNewVersionCreate, FeatureListServiceCreate
from featurebyte.schema.feature_list_namespace import FeatureListNamespaceServiceUpdate
from featurebyte.service.default_version_mode import DefaultVersionModeService
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
        default_version_mode_service: DefaultVersionModeService,
    ):
        self.feature_list_service = feature_list_service
        self.feature_list_namespace_service = feature_list_namespace_service
        self.feature_list_status_service = feature_list_status_service
        self.feature_readiness_service = feature_readiness_service
        self.version_service = version_service
        self.default_version_mode_service = default_version_mode_service

    async def create_feature_list(self, data: FeatureListServiceCreate) -> FeatureListModel:
        """
        Create a new feature list

        Parameters
        ----------
        data: FeatureListServiceCreate
            Feature list service create payload

        Returns
        -------
        FeatureListModel
        """
        document = await self.feature_list_service.create_document(data=data)
        await self.feature_readiness_service.update_feature_list_namespace(
            feature_list_namespace_id=document.feature_list_namespace_id,
            return_document=False,
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
            return_document=False,
        )
        output = await self.feature_list_service.get_document(document_id=document.id)
        return output

    async def make_feature_production_ready(
        self, feature_list_id: ObjectId, ignore_guardrails: bool = False
    ) -> None:
        """
        Make feature(s) of the given feature list production ready

        Parameters
        ----------
        feature_list_id: ObjectId
            Feature list id
        ignore_guardrails: bool
            Ignore guardrails of feature readiness update
        """
        feature_list = await self.feature_list_service.get_document(document_id=feature_list_id)
        for feature_id in feature_list.feature_ids:
            await self.feature_readiness_service.update_feature(
                feature_id=feature_id,
                readiness=FeatureReadiness.PRODUCTION_READY,
                ignore_guardrails=ignore_guardrails,
                return_document=False,
            )
        await self.feature_list_service.get_document(document_id=feature_list_id)

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

    async def update_default_version_mode(
        self, feature_list_namespace_id: ObjectId, default_version_mode: DefaultVersionMode
    ) -> FeatureListNamespaceModel:
        """
        Update default version mode of a feature list namespace

        Parameters
        ----------
        feature_list_namespace_id: ObjectId
            Feature list namespace id to update
        default_version_mode: DefaultVersionMode
            Default version mode

        Returns
        -------
        FeatureListNamespaceModel
        """
        await self.default_version_mode_service.update_feature_list_namespace(
            feature_list_namespace_id=feature_list_namespace_id,
            default_version_mode=default_version_mode,
            return_document=False,
        )
        return await self.feature_list_namespace_service.get_document(
            document_id=feature_list_namespace_id
        )

    async def update_default_feature_list(
        self, feature_list_id: ObjectId
    ) -> FeatureListNamespaceModel:
        """
        Update default feature list of a feature list namespace

        Parameters
        ----------
        feature_list_id: ObjectId
            Feature list id to update

        Returns
        -------
        FeatureListNamespaceModel

        Raises
        ------
        DocumentUpdateError
            If the default feature cannot be updated
        """
        new_default_fl = await self.feature_list_service.get_document(document_id=feature_list_id)
        namespace_id = new_default_fl.feature_list_namespace_id
        feature_list_namespace = await self.feature_list_namespace_service.get_document(
            document_id=namespace_id
        )
        if feature_list_namespace.default_version_mode != DefaultVersionMode.MANUAL:
            raise DocumentUpdateError(
                "Cannot set default feature list ID when default version mode is not MANUAL."
            )

        # update feature list namespace default feature list ID and update feature readiness
        await self.feature_list_namespace_service.update_document(
            document_id=namespace_id,
            data=FeatureListNamespaceServiceUpdate(default_feature_list_id=feature_list_id),
        )
        await self.feature_readiness_service.update_feature_list_namespace(
            feature_list_namespace_id=namespace_id
        )
        return await self.feature_list_namespace_service.get_document(document_id=namespace_id)

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
            If feature list is not in DRAFT status or is the default feature list of the feature list namespace
        """
        feature_list = await self.feature_list_service.get_document(document_id=feature_list_id)
        feature_list_namespace = await self.feature_list_namespace_service.get_document(
            document_id=feature_list.feature_list_namespace_id
        )
        if feature_list_namespace.status != FeatureListStatus.DRAFT:
            raise DocumentDeletionError("Only feature list with DRAFT status can be deleted.")

        if (
            feature_list_namespace.default_feature_list_id == feature_list_id
            and feature_list_namespace.default_version_mode == DefaultVersionMode.MANUAL
        ):
            raise DocumentDeletionError(
                "Feature list is the default feature list of the feature list namespace and the "
                "default version mode is manual. Please set another feature list as the default feature list "
                "or change the default version mode to auto."
            )

        await self.feature_list_service.delete_document(document_id=feature_list_id)
        try:
            await self.feature_readiness_service.update_feature_list_namespace(
                feature_list_namespace_id=feature_list.feature_list_namespace_id,
                deleted_feature_list_ids=[feature_list_id],
                return_document=False,
            )
        except DocumentNotFoundError:
            # if feature list namespace is deleted, do nothing
            pass
