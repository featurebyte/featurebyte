"""
FeatureListStatusService class
"""

import json

from bson import ObjectId

from featurebyte.exception import DocumentUpdateError
from featurebyte.models.feature_list_namespace import FeatureListStatus
from featurebyte.schema.feature_list_namespace import FeatureListNamespaceServiceUpdate
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService


class FeatureListStatusService:
    """
    FeatureListStatusService class is responsible for handling feature list status update and
    orchestrate feature list status update through FeatureListService and FeatureListNamespaceService.
    """

    def __init__(
        self,
        feature_list_namespace_service: FeatureListNamespaceService,
        feature_list_service: FeatureListService,
    ):
        self.feature_list_namespace_service = feature_list_namespace_service
        self.feature_list_service = feature_list_service

    async def update_feature_list_namespace_status(
        self, feature_list_namespace_id: ObjectId, target_feature_list_status: FeatureListStatus
    ) -> None:
        """
        Update feature list namespace

        Parameters
        ----------
        feature_list_namespace_id: ObjectId
            Target FeatureListNamespace ID
        target_feature_list_status: FeatureListStatus
            Target feature list status

        Raises
        ------
        DocumentUpdateError
            If feature list namespace status is not allowed to be updated to target status
        """
        feature_list_namespace = await self.feature_list_namespace_service.get_document(
            document_id=feature_list_namespace_id
        )
        deployed_feature_list_ids = [
            str(feature_list_id)
            for feature_list_id in feature_list_namespace.deployed_feature_list_ids
        ]
        if feature_list_namespace.status == target_feature_list_status:
            # if no change in status, do nothing
            return

        if (
            feature_list_namespace.status == FeatureListStatus.DRAFT
            and target_feature_list_status == FeatureListStatus.DEPRECATED
        ):
            raise DocumentUpdateError(
                "Not allowed to update status of feature list from DRAFT to DEPRECATED. "
                "Valid status transition: DRAFT -> PUBLIC_DRAFT or DRAFT -> TEMPLATE. "
                "Please delete feature list instead if it is no longer needed."
            )

        if target_feature_list_status == FeatureListStatus.DRAFT:
            # feature list is not allowed to be updated to draft status
            raise DocumentUpdateError(
                f'Not allowed to update status of FeatureList (name: "{feature_list_namespace.name}") '
                "to draft status."
            )

        if target_feature_list_status == FeatureListStatus.DEPLOYED:
            if not deployed_feature_list_ids:
                raise DocumentUpdateError(
                    f'Not allowed to update status of FeatureList (name: "{feature_list_namespace.name}") '
                    "to deployed status without deployed feature list."
                )

        if feature_list_namespace.status == FeatureListStatus.DEPLOYED:
            # if feature list is deployed, it can only be updated to public draft status if there is no
            # deployed feature list
            if deployed_feature_list_ids:
                raise DocumentUpdateError(
                    f'Not allowed to update status of FeatureList (name: "{feature_list_namespace.name}") '
                    f"with deployed feature list ids: {json.dumps(deployed_feature_list_ids)}."
                )

            if target_feature_list_status != FeatureListStatus.PUBLIC_DRAFT:
                raise DocumentUpdateError(
                    f'Deployed FeatureList (name: "{feature_list_namespace.name}") can only be updated to '
                    f"public draft status."
                )

        await self.feature_list_namespace_service.update_document(
            document_id=feature_list_namespace_id,
            data=FeatureListNamespaceServiceUpdate(status=target_feature_list_status),
            return_document=False,
        )
