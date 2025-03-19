"""
Feature Facade Service which is responsible for handling high level feature operations
"""

from pprint import pformat
from typing import Optional

from bson import ObjectId

from featurebyte.exception import DocumentDeletionError, DocumentUpdateError
from featurebyte.models.base import VersionIdentifier
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_namespace import (
    DefaultVersionMode,
    FeatureNamespaceModel,
    FeatureReadiness,
)
from featurebyte.schema.feature import FeatureNewVersionCreate, FeatureServiceCreate
from featurebyte.schema.feature_namespace import FeatureNamespaceServiceUpdate
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.feature_readiness import FeatureReadinessService
from featurebyte.service.feature_type import FeatureTypeService
from featurebyte.service.version import VersionService


class FeatureFacadeService:
    """
    Feature Facade Service class is responsible for providing simple APIs for feature operations,
    and delegating the work to the appropriate services.
    """

    def __init__(
        self,
        feature_service: FeatureService,
        feature_namespace_service: FeatureNamespaceService,
        feature_readiness_service: FeatureReadinessService,
        feature_type_service: FeatureTypeService,
        version_service: VersionService,
        feature_list_service: FeatureListService,
    ):
        self.feature_service = feature_service
        self.feature_namespace_service = feature_namespace_service
        self.feature_readiness_service = feature_readiness_service
        self.feature_type_service = feature_type_service
        self.version_service = version_service
        self.feature_list_service = feature_list_service

    async def create_feature(self, data: FeatureServiceCreate) -> FeatureModel:
        """
        Create a feature given feature service create payload

        Parameters
        ----------
        data: FeatureServiceCreate
            Feature service create payload

        Returns
        -------
        FeatureModel
        """
        document = await self.feature_service.create_document(data=data)
        await self.feature_readiness_service.update_feature_namespace(
            feature_namespace_id=document.feature_namespace_id,
        )
        feature_namespace = await self.feature_namespace_service.get_document(
            document_id=document.feature_namespace_id
        )
        if feature_namespace.feature_type is None:
            feature_type = await self.feature_type_service.detect_feature_type(feature=document)
            await self.feature_namespace_service.update_document(
                document_id=document.feature_namespace_id,
                data=FeatureNamespaceServiceUpdate(feature_type=feature_type),
            )

        output = await self.feature_service.get_document(document_id=document.id)
        return output

    async def create_new_version(
        self, data: FeatureNewVersionCreate, to_save: bool = True
    ) -> FeatureModel:
        """
        Create a new version of a feature

        Parameters
        ----------
        data: FeatureNewVersionCreate
            Feature new version create payload
        to_save: bool
            Whether to save the new version

        Returns
        -------
        FeatureModel
        """
        document = await self.version_service.create_new_feature_version(data=data, to_save=to_save)
        if to_save:
            await self.feature_readiness_service.update_feature_namespace(
                feature_namespace_id=document.feature_namespace_id,
            )
            output = await self.feature_service.get_document(document_id=document.id)
            return output
        return document

    async def update_readiness(
        self, feature_id: ObjectId, readiness: FeatureReadiness, ignore_guardrails: bool = False
    ) -> FeatureModel:
        """
        Update the readiness of a feature

        Parameters
        ----------
        feature_id: ObjectId
            Feature id to update
        readiness: FeatureReadiness
            Feature readiness
        ignore_guardrails: bool
            Ignore guardrails

        Returns
        -------
        FeatureModel
        """
        await self.feature_readiness_service.update_feature(
            feature_id=feature_id,
            readiness=FeatureReadiness(readiness),
            ignore_guardrails=ignore_guardrails,
        )
        output = await self.feature_service.get_document(document_id=feature_id)
        return output

    async def update_default_version_mode(
        self, feature_namespace_id: ObjectId, default_version_mode: DefaultVersionMode
    ) -> FeatureNamespaceModel:
        """
        Update the default version mode of a feature namespace

        Parameters
        ----------
        feature_namespace_id: ObjectId
            Feature namespace id to update
        default_version_mode: DefaultVersionMode
            Default version mode

        Returns
        -------
        FeatureNamespaceModel
        """
        document = await self.feature_namespace_service.get_document(
            document_id=feature_namespace_id
        )
        if document.default_version_mode != default_version_mode:
            await self.feature_namespace_service.update_document(
                document_id=feature_namespace_id,
                data=FeatureNamespaceServiceUpdate(default_version_mode=default_version_mode),
                document=document,
                return_document=False,
            )
            await self.feature_readiness_service.update_feature_namespace(
                feature_namespace_id=feature_namespace_id,
            )
            feature_namespace = await self.feature_namespace_service.get_document(
                document_id=feature_namespace_id
            )
            return feature_namespace
        return document

    async def update_default_feature(self, feature_id: ObjectId) -> FeatureNamespaceModel:
        """
        Update the default feature of a feature namespace

        Parameters
        ----------
        feature_id: ObjectId
            Feature id to update

        Returns
        -------
        FeatureNamespaceModel

        Raises
        ------
        DocumentUpdateError
            If the default feature cannot be updated
        """
        new_default_feature = await self.feature_service.get_document(document_id=feature_id)
        namespace_id = new_default_feature.feature_namespace_id
        feature_namespace = await self.feature_namespace_service.get_document(
            document_id=namespace_id
        )
        if feature_namespace.default_version_mode != DefaultVersionMode.MANUAL:
            raise DocumentUpdateError(
                "Cannot set default feature ID when default version mode is not MANUAL."
            )

        # make sure the new default is the highest readiness level among all versions
        max_readiness = FeatureReadiness(new_default_feature.readiness)
        version: Optional[str] = None
        async for feature_dict in self.feature_service.list_documents_as_dict_iterator(
            query_filter={"_id": {"$in": feature_namespace.feature_ids}},
            projection={"_id": 1, "readiness": 1, "version": 1},
        ):
            max_readiness = max(max_readiness, FeatureReadiness(feature_dict["readiness"]))
            if feature_dict["readiness"] == max_readiness:
                version = VersionIdentifier(**feature_dict["version"]).to_str()

        if new_default_feature.readiness != max_readiness:
            raise DocumentUpdateError(
                f"Cannot set default feature ID to {new_default_feature.id} "
                f"because its readiness level ({new_default_feature.readiness}) "
                f"is lower than the readiness level of version {version} ({max_readiness.value})."
            )

        # update feature namespace default feature ID and update feature readiness
        await self.feature_namespace_service.update_document(
            document_id=namespace_id,
            data=FeatureNamespaceServiceUpdate(default_feature_id=feature_id),
        )
        await self.feature_readiness_service.update_feature_namespace(
            feature_namespace_id=namespace_id
        )
        return await self.feature_namespace_service.get_document(document_id=namespace_id)

    async def delete_feature(self, feature_id: ObjectId) -> None:
        """
        Delete a feature

        Parameters
        ----------
        feature_id: ObjectId
            Feature id to delete

        Raises
        ------
        DocumentDeletionError
            * If the feature is not in draft readiness
            * If the feature is the default feature and the default version mode is manual
            * If the feature is in any saved feature list
        """
        feature = await self.feature_service.get_document(document_id=feature_id)
        feature_namespace = await self.feature_namespace_service.get_document(
            document_id=feature.feature_namespace_id
        )

        if feature.readiness != FeatureReadiness.DRAFT:
            raise DocumentDeletionError("Only feature with draft readiness can be deleted.")

        if (
            feature_namespace.default_feature_id == feature_id
            and feature_namespace.default_version_mode == DefaultVersionMode.MANUAL
        ):
            raise DocumentDeletionError(
                "Feature is the default feature of the feature namespace and the default version mode is manual. "
                "Please set another feature as the default feature or change the default version mode to auto."
            )

        if feature.feature_list_ids:
            feature_list_info = []
            async for feature_list in self.feature_list_service.list_documents_as_dict_iterator(
                query_filter={"_id": {"$in": feature.feature_list_ids}},
                projection={"_id": 1, "name": 1, "version": 1},
            ):
                feature_list_info.append({
                    "id": str(feature_list["_id"]),
                    "name": feature_list["name"],
                    "version": VersionIdentifier(**feature_list["version"]).to_str(),
                })

            raise DocumentDeletionError(
                f"Feature is still in use by feature list(s). Please remove the following feature list(s) first:\n"
                f"{pformat(feature_list_info)}"
            )

        # use transaction to ensure atomicity
        async with self.feature_service.persistent.start_transaction():
            # delete feature from the persistent
            await self.feature_service.delete_document(document_id=feature_id)
            await self.feature_readiness_service.update_feature_namespace(
                feature_namespace_id=feature.feature_namespace_id,
                deleted_feature_ids=[feature_id],
            )
            feature_namespace = await self.feature_namespace_service.get_document(
                document_id=feature.feature_namespace_id
            )
            if not feature_namespace.feature_ids:
                # delete feature namespace if it has no more feature
                await self.feature_namespace_service.delete_document(
                    document_id=feature.feature_namespace_id
                )
