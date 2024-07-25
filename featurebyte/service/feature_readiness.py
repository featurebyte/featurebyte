"""
FeatureReadinessService
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Sequence

from bson import ObjectId
from pymongo.errors import OperationFailure
from tenacity import retry, retry_if_exception_type, wait_chain, wait_random

from featurebyte.exception import DocumentUpdateError
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_list import FeatureReadinessDistribution, FeatureReadinessTransition
from featurebyte.models.feature_list_namespace import FeatureListNamespaceModel
from featurebyte.models.feature_namespace import (
    DefaultVersionMode,
    FeatureNamespaceModel,
    FeatureReadiness,
)
from featurebyte.persistent import Persistent
from featurebyte.schema.feature_list_namespace import FeatureListNamespaceServiceUpdate
from featurebyte.schema.feature_namespace import FeatureNamespaceServiceUpdate
from featurebyte.service.base_document import RETRY_MAX_ATTEMPT_NUM, RETRY_MAX_WAIT_IN_SEC
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.validator.production_ready_validator import ProductionReadyValidator


class FeatureReadinessService:
    """
    FeatureReadinessService class is responsible for maintaining the feature readiness structure
    consistencies between feature & feature list (version & namespace).
    """

    def __init__(
        self,
        persistent: Persistent,
        feature_service: FeatureService,
        feature_namespace_service: FeatureNamespaceService,
        feature_list_service: FeatureListService,
        feature_list_namespace_service: FeatureListNamespaceService,
        production_ready_validator: ProductionReadyValidator,
    ):
        self.persistent = persistent
        self.feature_service = feature_service
        self.feature_namespace_service = feature_namespace_service
        self.feature_list_service = feature_list_service
        self.feature_list_namespace_service = feature_list_namespace_service
        self.production_ready_validator = production_ready_validator

    async def _get_default_feature_list_doc(
        self, feature_list_ids: Sequence[ObjectId]
    ) -> Dict[str, Any]:
        """
        Get default feature from list of feature IDs

        Parameters
        ----------
        feature_list_ids: Sequence[ObjectId]
            Feature list IDs

        Returns
        -------
        Dict[str, Any]
        """
        assert len(feature_list_ids) > 0, "feature_list_ids should not be empty"
        default_feature_list: Optional[Dict[str, Any]] = None
        async for feature_list in self.feature_list_service.list_documents_as_dict_iterator(
            query_filter={"_id": {"$in": feature_list_ids}}
        ):
            readiness_dist = FeatureReadinessDistribution(feature_list["readiness_distribution"])
            if default_feature_list is None:
                default_feature_list = feature_list
            else:
                default_readiness_dist = FeatureReadinessDistribution(
                    default_feature_list["readiness_distribution"]
                )
                if readiness_dist > default_readiness_dist:
                    default_feature_list = feature_list
                elif (
                    readiness_dist == default_readiness_dist
                    and feature_list["created_at"] > default_feature_list["created_at"]
                ):
                    default_feature_list = feature_list
        assert default_feature_list is not None, "default_feature_list should not be None"
        return default_feature_list

    async def update_feature_list_namespace(
        self,
        feature_list_namespace_id: ObjectId,
        deleted_feature_list_ids: Optional[list[ObjectId]] = None,
    ) -> FeatureListNamespaceModel:
        """
        Update default feature list and feature list readiness distribution in feature list namespace

        Parameters
        ----------
        feature_list_namespace_id: ObjectId
            FeatureListNamespace ID
        deleted_feature_list_ids: Optional[list[ObjectId]]
            Deleted feature list IDs

        Returns
        -------
        FeatureListNamespaceModel
        """
        document = await self.feature_list_namespace_service.get_document(
            document_id=feature_list_namespace_id
        )
        excluded_feature_list_ids = set(deleted_feature_list_ids or [])
        update_dict: dict[str, Any] = {}
        feature_list_ids = [
            feature_list_id
            for feature_list_id in document.feature_list_ids
            if feature_list_id not in excluded_feature_list_ids
        ]
        if feature_list_ids != document.feature_list_ids:
            update_dict["feature_list_ids"] = feature_list_ids

        if feature_list_ids:
            default_feature_list = await self._get_default_feature_list_doc(feature_list_ids)
            update_dict["default_feature_list_id"] = default_feature_list["_id"]

        if update_dict:
            await self.feature_list_namespace_service.update_document(
                document_id=feature_list_namespace_id,
                data=FeatureListNamespaceServiceUpdate(**update_dict),
                return_document=False,
            )
            return await self.feature_list_namespace_service.get_document(
                document_id=feature_list_namespace_id
            )
        return document

    async def update_feature_list(
        self,
        feature_list_id: ObjectId,
        from_readiness: FeatureReadiness,
        to_readiness: FeatureReadiness,
    ) -> Dict[str, Any]:
        """
        Update FeatureReadiness distribution in feature list

        Parameters
        ----------
        feature_list_id: ObjectId
            FeatureList ID
        from_readiness: FeatureReadiness
            From feature readiness
        to_readiness: FeatureReadiness
            To feature readiness

        Returns
        -------
        Dict[str, Any]
        """
        document = await self.feature_list_service.get_document_as_dict(document_id=feature_list_id)
        if from_readiness != to_readiness:
            doc_readiness_dist = FeatureReadinessDistribution(document["readiness_distribution"])
            readiness_dist = doc_readiness_dist.update_readiness(
                transition=FeatureReadinessTransition(
                    from_readiness=from_readiness, to_readiness=to_readiness
                ),
            )
            await self.feature_list_service.update_readiness_distribution(
                document_id=feature_list_id,
                readiness_distribution=readiness_dist,
            )
            return await self.feature_list_service.get_document_as_dict(document_id=feature_list_id)
        return document

    async def _get_default_feature(self, feature_ids: Sequence[ObjectId]) -> Dict[str, Any]:
        """
        Get default feature from list of feature IDs

        Parameters
        ----------
        feature_ids: Sequence[ObjectId]
            Feature IDs

        Returns
        -------
        Dict[str, Any]
        """
        assert len(feature_ids) > 0, "feature_ids should not be empty"
        default_feature: Optional[Dict[str, Any]] = None
        async for feature in self.feature_service.list_documents_as_dict_iterator(
            query_filter={"_id": {"$in": feature_ids}}
        ):
            if default_feature is None:
                default_feature = feature
            elif FeatureReadiness(feature["readiness"]) > FeatureReadiness(
                default_feature["readiness"]
            ):
                # when doing non-equality comparison, must cast it explicitly to FeatureReadiness
                # otherwise, it will become normal string comparison
                default_feature = feature
            elif (
                feature["readiness"] == default_feature["readiness"]
                and feature["created_at"] > default_feature["created_at"]
            ):
                default_feature = feature
        assert default_feature is not None, "default_feature should not be None"
        return default_feature

    async def update_feature_namespace(
        self,
        feature_namespace_id: ObjectId,
        deleted_feature_ids: Optional[list[ObjectId]] = None,
    ) -> FeatureNamespaceModel:
        """
        Update default feature and feature readiness in feature namespace

        Parameters
        ----------
        feature_namespace_id: ObjectId
            FeatureNamespace ID
        deleted_feature_ids: Optional[list[ObjectId]]
            Deleted feature IDs

        Returns
        -------
        Optional[FeatureNamespaceModel]
        """
        document = await self.feature_namespace_service.get_document(
            document_id=feature_namespace_id
        )
        excluded_feature_ids = set(deleted_feature_ids or [])
        update_dict: dict[str, Any] = {}
        feature_ids = [
            feature_id
            for feature_id in document.feature_ids
            if feature_id not in excluded_feature_ids
        ]
        if feature_ids != document.feature_ids:
            update_dict["feature_ids"] = feature_ids

        if document.default_version_mode == DefaultVersionMode.AUTO:
            # when default version mode is AUTO & (feature is not specified or already in current namespace)
            if feature_ids:
                default_feature = await self._get_default_feature(feature_ids=feature_ids)
                update_dict["default_feature_id"] = default_feature["_id"]
                update_dict["readiness"] = default_feature["readiness"]
        else:
            assert (
                document.default_feature_id not in excluded_feature_ids
            ), "default feature should not be deleted"
            # use projection to reduce the amount of data transfer &
            # default feature is used within this function only
            default_feature = await self.feature_service.get_document_as_dict(
                document_id=document.default_feature_id,
                projection={"readiness": 1},
            )
            default_feature_readiness = FeatureReadiness(default_feature["readiness"])
            if default_feature_readiness != document.readiness:
                # when feature readiness get updated and feature namespace in manual default mode
                update_dict["readiness"] = default_feature_readiness

        if update_dict:
            await self.feature_namespace_service.update_document(
                document_id=feature_namespace_id,
                data=FeatureNamespaceServiceUpdate(**update_dict),
                document=document,
                return_document=False,
            )
            return await self.feature_namespace_service.get_document(
                document_id=feature_namespace_id
            )

        return document

    async def _validate_readiness_transition(
        self, document: FeatureModel, target_readiness: FeatureReadiness, ignore_guardrails: bool
    ) -> None:
        # validate the readiness transition is valid or not
        if target_readiness == FeatureReadiness.PRODUCTION_READY:
            assert document.name is not None
            await self.production_ready_validator.validate(
                promoted_feature=document,
                ignore_guardrails=ignore_guardrails,
            )

        if (
            document.readiness != FeatureReadiness.DRAFT
            and target_readiness == FeatureReadiness.DRAFT
        ):
            raise DocumentUpdateError("Cannot update feature readiness to DRAFT.")

        if (
            document.readiness == FeatureReadiness.DRAFT
            and target_readiness == FeatureReadiness.DEPRECATED
        ):
            raise DocumentUpdateError(
                "Not allowed to update feature readiness from DRAFT to DEPRECATED. "
                "Valid transitions are DRAFT -> PUBLIC_DRAFT or DRAFT -> PRODUCTION_READY. "
                "Please delete the feature instead if it is no longer needed."
            )

    @retry(
        retry=retry_if_exception_type(OperationFailure),
        wait=wait_chain(*[
            wait_random(max=RETRY_MAX_WAIT_IN_SEC) for _ in range(RETRY_MAX_ATTEMPT_NUM)
        ]),
    )
    async def update_feature(
        self,
        feature_id: ObjectId,
        readiness: FeatureReadiness,
        ignore_guardrails: bool = False,
    ) -> FeatureModel:
        """
        Update feature readiness & trigger list of cascading updates

        Parameters
        ----------
        feature_id: ObjectId
            Target feature ID
        readiness: FeatureReadiness
            Target feature readiness status
        ignore_guardrails: bool
            Allow a user to specify if they want to ignore any guardrails when updating this feature. This should
            currently only apply of the FeatureReadiness value is being updated to PRODUCTION_READY. This should
            be a no-op for all other scenarios.

        Returns
        -------
        FeatureModel
        """
        document = await self.feature_service.get_document(document_id=feature_id)
        await self._validate_readiness_transition(
            document=document, target_readiness=readiness, ignore_guardrails=ignore_guardrails
        )
        if document.readiness != readiness:
            async with self.persistent.start_transaction():
                await self.feature_service.update_readiness(
                    document_id=feature_id, readiness=readiness
                )
                # use projection to reduce the amount of data transfer &
                # feature is used within this function only
                feature = await self.feature_service.get_document_as_dict(
                    document_id=feature_id,
                    projection={"feature_namespace_id": 1, "feature_list_ids": 1, "readiness": 1},
                )
                await self.update_feature_namespace(
                    feature_namespace_id=feature["feature_namespace_id"],
                )
                for feature_list_id in feature["feature_list_ids"]:
                    feature_list = await self.update_feature_list(
                        feature_list_id=feature_list_id,
                        from_readiness=document.readiness,
                        to_readiness=FeatureReadiness(feature["readiness"]),
                    )
                    await self.update_feature_list_namespace(
                        feature_list_namespace_id=feature_list["feature_list_namespace_id"],
                    )

            return await self.feature_service.get_document(document_id=feature_id)
        return document
