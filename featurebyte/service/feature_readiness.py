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
from featurebyte.schema.feature import FeatureServiceUpdate
from featurebyte.schema.feature_list import FeatureListServiceUpdate
from featurebyte.schema.feature_list_namespace import FeatureListNamespaceServiceUpdate
from featurebyte.schema.feature_namespace import FeatureNamespaceServiceUpdate
from featurebyte.service.base_update import BaseUpdateService
from featurebyte.service.feature import validate_feature_version_and_namespace_consistency
from featurebyte.service.feature_list import validate_feature_list_version_and_namespace_consistency


class FeatureReadinessService(BaseUpdateService):
    """
    FeatureReadinessService class is responsible for maintaining the feature readiness structure
    consistencies between feature & feature list (version & namespace).
    """

    async def update_feature_list_namespace(
        self,
        feature_list_namespace_id: ObjectId,
        feature_list: Optional[FeatureListModel],
        document: Optional[FeatureListNamespaceModel] = None,
        return_document: bool = True,
    ) -> Optional[FeatureListNamespaceModel]:
        """
        Update default feature list and feature list readiness distribution in feature list namespace

        Parameters
        ----------
        feature_list_namespace_id: ObjectId
            FeatureListNamespace ID
        feature_list: Optional[FeatureListModel]
            FeatureList
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

        default_feature_list = await self.feature_list_service.get_document(
            document_id=document.default_feature_list_id
        )
        to_find_default_feature_list = document.default_version_mode == DefaultVersionMode.AUTO
        found_default_feature_list = False
        update_dict: dict[str, Any] = {}
        if feature_list:
            await validate_feature_list_version_and_namespace_consistency(
                feature_list=feature_list,
                feature_list_namespace=document,
                feature_service=self.feature_service,
            )
            if feature_list.id not in document.feature_list_ids:
                # when a new feature list version is added to the namespace
                update_dict["feature_list_ids"] = self.include_object_id(
                    document.feature_list_ids, feature_list.id
                )
                if to_find_default_feature_list:
                    # compare with the default readiness stored at the namespace is sufficient to find the default
                    assert feature_list.created_at is not None
                    assert default_feature_list.created_at is not None
                    if (
                        feature_list.readiness_distribution >= document.readiness_distribution  # type: ignore
                        and feature_list.created_at > default_feature_list.created_at
                    ):
                        update_dict["readiness_distribution"] = feature_list.readiness_distribution
                        update_dict["default_feature_list_id"] = feature_list.id
                    found_default_feature_list = True

        if to_find_default_feature_list and not found_default_feature_list:
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

        return await self.feature_list_namespace_service.update_document(
            document_id=feature_list_namespace_id,
            data=FeatureListNamespaceServiceUpdate(**update_dict),
            return_document=return_document,
        )

    async def update_feature_list(
        self,
        feature_list_id: ObjectId,
        from_readiness: FeatureReadiness,
        to_readiness: FeatureReadiness,
        document: Optional[FeatureListModel] = None,
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

        if return_document:
            return document
        return None

    async def update_feature_namespace(
        self,
        feature_namespace_id: ObjectId,
        feature: Optional[FeatureModel],
        document: Optional[FeatureNamespaceModel] = None,
        return_document: bool = True,
    ) -> Optional[FeatureNamespaceModel]:
        """
        Update default feature and feature readiness in feature namespace

        Parameters
        ----------
        feature_namespace_id: ObjectId
            FeatureNamespace ID
        feature: Optional[FeatureModel]
            Feature
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

        default_feature = await self.feature_service.get_document(
            document_id=document.default_feature_id
        )
        to_find_default_feature = document.default_version_mode == DefaultVersionMode.AUTO
        found_default_feature = False
        update_dict: dict[str, Any] = {}
        if feature:
            await validate_feature_version_and_namespace_consistency(
                feature=feature, feature_namespace=document
            )
            if feature.id not in document.feature_ids:
                # when a new feature version is added to the namespace
                update_dict["feature_ids"] = self.include_object_id(
                    document.feature_ids, feature.id
                )
                if to_find_default_feature:
                    # compare with the default readiness stored at the namespace is sufficient to find the default
                    if (
                        FeatureReadiness(feature.readiness) >= document.readiness
                        and feature.created_at > default_feature.created_at  # type: ignore[operator]
                    ):
                        update_dict["readiness"] = feature.readiness
                        update_dict["default_feature_id"] = feature.id
                    found_default_feature = True

        if to_find_default_feature and not found_default_feature:
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

        return await self.feature_namespace_service.update_document(
            document_id=feature_namespace_id,
            data=FeatureNamespaceServiceUpdate(**update_dict),
            document=document,
            return_document=return_document,
        )

    async def update_feature(
        self,
        feature_id: ObjectId,
        readiness: FeatureReadiness,
        document: Optional[FeatureModel] = None,
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
                    feature=feature,
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
                        feature_list=feature_list,
                        return_document=False,
                    )
                if return_document:
                    return feature

        if return_document:
            return document
        return None
