"""
FeatureReadinessService
"""
from __future__ import annotations

from typing import Any, Optional

from bson.objectid import ObjectId

from featurebyte.models.feature import (
    DefaultVersionMode,
    FeatureModel,
    FeatureNamespaceModel,
    FeatureReadiness,
)
from featurebyte.models.feature_list import (
    FeatureListModel,
    FeatureListNamespaceModel,
    FeatureReadinessTransition,
)
from featurebyte.persistent import Persistent
from featurebyte.schema.feature import FeatureServiceUpdate
from featurebyte.schema.feature_list import FeatureListServiceUpdate
from featurebyte.schema.feature_list_namespace import FeatureListNamespaceServiceUpdate
from featurebyte.schema.feature_namespace import FeatureNamespaceServiceUpdate
from featurebyte.service.base_service import BaseService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.validator.production_ready_validator import ProductionReadyValidator
from featurebyte.service.version import VersionService


class FeatureReadinessService(BaseService):
    """
    FeatureReadinessService class is responsible for maintaining the feature readiness structure
    consistencies between feature & feature list (version & namespace).
    """

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        workspace_id: ObjectId,
        feature_service: FeatureService,
        feature_namespace_service: FeatureNamespaceService,
        feature_list_service: FeatureListService,
        feature_list_namespace_service: FeatureListNamespaceService,
        version_service: VersionService,
    ):
        super().__init__(user, persistent, workspace_id)
        self.feature_service = feature_service
        self.feature_namespace_service = feature_namespace_service
        self.feature_list_service = feature_list_service
        self.feature_list_namespace_service = feature_list_namespace_service
        self.production_ready_validator = ProductionReadyValidator(
            self.feature_namespace_service, version_service, feature_service
        )

    async def update_feature_list_namespace(
        self,
        feature_list_namespace_id: ObjectId,
        return_document: bool = True,
    ) -> Optional[FeatureListNamespaceModel]:
        """
        Update default feature list and feature list readiness distribution in feature list namespace

        Parameters
        ----------
        feature_list_namespace_id: ObjectId
            FeatureListNamespace ID
        return_document: bool
            Whether to return updated document

        Returns
        -------
        Optional[FeatureListNamespaceModel]
        """
        document = await self.feature_list_namespace_service.get_document(
            document_id=feature_list_namespace_id
        )
        default_feature_list = await self.feature_list_service.get_document(
            document_id=document.default_feature_list_id
        )
        update_dict: dict[str, Any] = {}
        if document.default_version_mode == DefaultVersionMode.AUTO:
            # when default version mode is AUTO & (feature is not specified or already in current namespace)
            readiness_distribution = document.readiness_distribution.worst_case()
            for feature_list_id in document.feature_list_ids:
                version = await self.feature_list_service.get_document(document_id=feature_list_id)
                if version.readiness_distribution > readiness_distribution:
                    readiness_distribution = version.readiness_distribution
                    default_feature_list = version
                elif (
                    version.readiness_distribution == readiness_distribution
                    and version.created_at > default_feature_list.created_at  # type: ignore[operator]
                ):
                    default_feature_list = version
            update_dict["readiness_distribution"] = default_feature_list.readiness_distribution
            update_dict["default_feature_list_id"] = default_feature_list.id

        if (
            document.default_version_mode == DefaultVersionMode.MANUAL
            and default_feature_list.readiness_distribution != document.readiness_distribution
        ):
            # when feature readiness get updated and feature list namespace in manual default mode
            update_dict["readiness_distribution"] = default_feature_list.readiness_distribution

        updated_document: Optional[FeatureListNamespaceModel] = document
        if update_dict:
            updated_document = await self.feature_list_namespace_service.update_document(
                document_id=feature_list_namespace_id,
                data=FeatureListNamespaceServiceUpdate(**update_dict),
                return_document=return_document,
            )

        return self.conditional_return(document=updated_document, condition=return_document)

    async def update_feature_list(
        self,
        feature_list_id: ObjectId,
        from_readiness: FeatureReadiness,
        to_readiness: FeatureReadiness,
        return_document: bool = True,
    ) -> Optional[FeatureListModel]:
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
        return_document: bool
            Whether to return updated document

        Returns
        -------
        Optional[FeatureListModel]
        """
        document = await self.feature_list_service.get_document(document_id=feature_list_id)
        if from_readiness != to_readiness:
            readiness_dist = document.readiness_distribution.update_readiness(
                transition=FeatureReadinessTransition(
                    from_readiness=from_readiness, to_readiness=to_readiness
                ),
            )
            return await self.feature_list_service.update_document(
                document_id=feature_list_id,
                data=FeatureListServiceUpdate(readiness_distribution=readiness_dist),
                document=document,
                return_document=return_document,
            )
        return self.conditional_return(document=document, condition=return_document)

    async def update_feature_namespace(
        self,
        feature_namespace_id: ObjectId,
        return_document: bool = True,
    ) -> Optional[FeatureNamespaceModel]:
        """
        Update default feature and feature readiness in feature namespace

        Parameters
        ----------
        feature_namespace_id: ObjectId
            FeatureNamespace ID
        return_document: bool
            Whether to return updated document

        Returns
        -------
        Optional[FeatureNamespaceModel]
        """
        document = await self.feature_namespace_service.get_document(
            document_id=feature_namespace_id
        )
        default_feature = await self.feature_service.get_document(
            document_id=document.default_feature_id
        )
        update_dict: dict[str, Any] = {}
        if document.default_version_mode == DefaultVersionMode.AUTO:
            # when default version mode is AUTO & (feature is not specified or already in current namespace)
            readiness = min(FeatureReadiness)
            for feature_id in document.feature_ids:
                version = await self.feature_service.get_document(document_id=feature_id)
                if version.readiness > readiness:
                    readiness = FeatureReadiness(version.readiness)
                    default_feature = version
                elif (
                    version.readiness == readiness
                    and version.created_at > default_feature.created_at  # type: ignore[operator]
                ):
                    default_feature = version
            update_dict["readiness"] = default_feature.readiness
            update_dict["default_feature_id"] = default_feature.id

        if (
            document.default_version_mode == DefaultVersionMode.MANUAL
            and default_feature.readiness != document.readiness
        ):
            # when feature readiness get updated and feature namespace in manual default mode
            update_dict["readiness"] = default_feature.readiness

        updated_document: Optional[FeatureNamespaceModel] = document
        if update_dict:
            updated_document = await self.feature_namespace_service.update_document(
                document_id=feature_namespace_id,
                data=FeatureNamespaceServiceUpdate(**update_dict),
                document=document,
                return_document=return_document,
            )
        return self.conditional_return(document=updated_document, condition=return_document)

    async def update_feature(
        self,
        feature_id: ObjectId,
        readiness: FeatureReadiness,
        ignore_guardrails: bool = False,
        return_document: bool = True,
    ) -> Optional[FeatureModel]:
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
        return_document: bool
            Whether to return updated document

        Returns
        -------
        Optional[FeatureModel]

        Raises
        ------
        ValueError
            raised if the feature has no name
        """
        document = await self.feature_service.get_document(document_id=feature_id)

        # If we are updating the feature readiness status to PRODUCTION_READY, perform some additional validation.
        if readiness == FeatureReadiness.PRODUCTION_READY:
            if document.name is None:
                raise ValueError("feature document has no name")
            await self.production_ready_validator.validate(
                document.name, document.graph, ignore_guardrails
            )

        if document.readiness != readiness:
            async with self.persistent.start_transaction():
                feature = await self.feature_service.update_document(
                    document_id=feature_id,
                    data=FeatureServiceUpdate(readiness=readiness),
                    document=document,
                    return_document=True,
                )
                assert isinstance(feature, FeatureModel)
                await self.update_feature_namespace(
                    feature_namespace_id=feature.feature_namespace_id,
                    return_document=False,
                )
                for feature_list_id in feature.feature_list_ids:
                    feature_list = await self.update_feature_list(
                        feature_list_id=feature_list_id,
                        from_readiness=document.readiness,
                        to_readiness=feature.readiness,
                        return_document=True,
                    )
                    assert isinstance(feature_list, FeatureListModel)
                    await self.update_feature_list_namespace(
                        feature_list_namespace_id=feature_list.feature_list_namespace_id,
                        return_document=False,
                    )
                return self.conditional_return(document=feature, condition=return_document)
        return self.conditional_return(document=document, condition=return_document)
