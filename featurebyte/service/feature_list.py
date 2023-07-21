"""
FeatureListService class
"""
from __future__ import annotations

from typing import Any, AsyncIterator, Dict, List, Optional, Sequence

from bson.objectid import ObjectId

from featurebyte.common.model_util import get_version
from featurebyte.exception import DocumentError, DocumentInconsistencyError, DocumentNotFoundError
from featurebyte.models.base import VersionIdentifier
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_list import (
    EntityRelationshipInfo,
    FeatureListModel,
    FeatureListNamespaceModel,
)
from featurebyte.models.feature_namespace import DefaultVersionMode
from featurebyte.persistent import Persistent
from featurebyte.schema.feature_list import FeatureListServiceCreate, FeatureListServiceUpdate
from featurebyte.schema.feature_list_namespace import FeatureListNamespaceServiceUpdate
from featurebyte.schema.info import (
    DefaultFeatureFractionComparison,
    FeatureListBriefInfoList,
    FeatureListInfo,
    FeatureListNamespaceInfo,
)
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.entity import EntityService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService
from featurebyte.service.relationship_info import RelationshipInfoService


async def validate_feature_list_version_and_namespace_consistency(
    feature_list: FeatureListModel,
    feature_list_namespace: FeatureListNamespaceModel,
    feature_service: FeatureService,
) -> None:
    """
    Validate whether the feature list & feature list namespace are consistent

    Parameters
    ----------
    feature_list: FeatureListModel
        Feature list object
    feature_list_namespace: FeatureListNamespaceModel
        Feature list namespace object
    feature_service: FeatureService
        Feature Service object

    Raises
    ------
    DocumentInconsistencyError
        If the inconsistency between version & namespace found
    """
    if feature_list.name != feature_list_namespace.name:
        raise DocumentInconsistencyError(
            f'FeatureList (name: "{feature_list.name}") object(s) within the same namespace '
            f'must have the same "name" value (namespace: "{feature_list_namespace.name}", '
            f'feature_list: "{feature_list.name}").'
        )

    feature_namespace_ids = []
    for feature_id in feature_list.feature_ids:
        feature = await feature_service.get_document(document_id=feature_id)
        feature_namespace_ids.append(feature.feature_namespace_id)

    if sorted(feature_namespace_ids) != sorted(feature_list_namespace.feature_namespace_ids):
        raise DocumentInconsistencyError(
            f'FeatureList (name: "{feature_list.name}") object(s) within the same namespace '
            f"must share the same feature name(s)."
        )


def compute_default_feature_fraction(
    feature_list: FeatureListModel,
    default_feature_list: FeatureListModel,
    feature_list_namespace_info: FeatureListNamespaceInfo,
) -> DefaultFeatureFractionComparison:
    """
    Helper method to compute default feature fractions.

    Parameters
    ----------
    feature_list: FeatureListModel
        Feature list object
    default_feature_list: FeatureListModel
        Default feature list object
    feature_list_namespace_info: FeatureListNamespaceInfo
        Feature list namespace info object

    Returns
    -------
    DefaultFeatureFractionComparison
    """
    default_feature_ids = set(feature_list_namespace_info.default_feature_ids)
    this_count, default_count = 0, 0
    for feat_id in feature_list.feature_ids:
        if feat_id in default_feature_ids:
            this_count += 1
    for feat_id in default_feature_list.feature_ids:
        if feat_id in default_feature_ids:
            default_count += 1
    return DefaultFeatureFractionComparison(
        this=this_count / len(feature_list.feature_ids),
        default=default_count / len(default_feature_list.feature_ids),
    )


class FeatureListService(
    BaseDocumentService[FeatureListModel, FeatureListServiceCreate, FeatureListServiceUpdate]
):
    """
    FeatureListService class
    """

    document_class = FeatureListModel

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: Optional[ObjectId],
        entity_service: EntityService,
        relationship_info_service: RelationshipInfoService,
        feature_service: FeatureService,
        feature_list_namespace_service: FeatureListNamespaceService,
    ):
        super().__init__(user=user, persistent=persistent, catalog_id=catalog_id)
        self.entity_service = entity_service
        self.relationship_info_service = relationship_info_service
        self.feature_service = feature_service
        self.feature_list_namespace_service = feature_list_namespace_service

    async def _feature_iterator(
        self, feature_ids: Sequence[ObjectId]
    ) -> AsyncIterator[FeatureModel]:
        for feature_id in feature_ids:
            feature = await self.feature_service.get_document(document_id=feature_id)
            yield feature

    async def _extract_feature_data(self, document: FeatureListModel) -> Dict[str, Any]:
        feature_store_id: Optional[ObjectId] = None
        feature_namespace_ids = set()
        features = []
        async for feature in self._feature_iterator(feature_ids=document.feature_ids):
            # retrieve feature from the persistent
            features.append(feature)

            # validate the feature list
            if feature_store_id and (feature_store_id != feature.tabular_source.feature_store_id):
                raise DocumentInconsistencyError(
                    "All the Feature objects within the same FeatureList object must be from the same "
                    "feature store."
                )

            # check whether there are duplicated feature names in a feature list
            if feature.feature_namespace_id in feature_namespace_ids:
                raise DocumentError(
                    "Two Feature objects must not share the same name in a FeatureList object."
                )

            # update feature_namespace_ids
            feature_namespace_ids.add(feature.feature_namespace_id)

            # store previous feature store id
            feature_store_id = feature.tabular_source.feature_store_id

        derived_output = {
            "feature_store_id": feature_store_id,
            "features": features,
        }
        return derived_output

    async def _extract_relationships_info(
        self, features: List[FeatureModel]
    ) -> List[EntityRelationshipInfo]:
        entity_ids = set()
        for feature in features:
            entity_ids.update(feature.entity_ids)

        ancestor_entity_ids = set(entity_ids)
        async for entity in self.entity_service.list_documents_iterator(
            query_filter={"_id": {"$in": list(entity_ids)}}
        ):
            ancestor_entity_ids.update(entity.ancestor_ids)

        descendant_entity_ids = set(entity_ids)
        async for entity in self.entity_service.list_documents_iterator(
            query_filter={"ancestor_ids": {"$in": list(entity_ids)}}
        ):
            descendant_entity_ids.add(entity.id)

        relationships_info = [
            EntityRelationshipInfo(**relationship_info)
            async for relationship_info in self.relationship_info_service.list_documents_as_dict_iterator(
                query_filter={
                    "$or": [
                        {"entity_id": {"$in": list(descendant_entity_ids)}},
                        {"related_entity_id": {"$in": list(ancestor_entity_ids)}},
                    ]
                }
            )
        ]
        return relationships_info

    async def _update_features(
        self,
        feature_ids: Sequence[ObjectId],
        inserted_feature_list_id: Optional[ObjectId] = None,
        deleted_feature_list_id: Optional[ObjectId] = None,
    ) -> None:
        if inserted_feature_list_id:
            await self.feature_service.update_documents(
                query_filter={"_id": {"$in": feature_ids}},
                update={"$addToSet": {"feature_list_ids": inserted_feature_list_id}},
            )

        if deleted_feature_list_id:
            await self.feature_service.update_documents(
                query_filter={"_id": {"$in": feature_ids}},
                update={"$pull": {"feature_list_ids": deleted_feature_list_id}},
            )

    async def _get_feature_list_version(self, name: str) -> VersionIdentifier:
        version_name = get_version()
        query_result = await self.list_documents_as_dict(
            query_filter={"name": name, "version.name": version_name}
        )
        count = query_result["total"]
        return VersionIdentifier(name=version_name, suffix=count or None)

    async def create_document(self, data: FeatureListServiceCreate) -> FeatureListModel:
        # sort feature_ids before saving to persistent storage to ease feature_ids comparison in uniqueness check
        document = FeatureListModel(
            **{
                **data.dict(by_alias=True),
                "version": await self._get_feature_list_version(data.name),
                "user_id": self.user.id,
                "catalog_id": self.catalog_id,
            }
        )
        # check any conflict with existing documents
        await self._check_document_unique_constraints(document=document)

        # check whether the feature(s) in the feature list saved to persistent or not
        feature_data = await self._extract_feature_data(document)
        relationships_info = await self._extract_relationships_info(feature_data["features"])

        # update document with derived output
        document = FeatureListModel(
            **{
                **document.dict(by_alias=True),
                "features": feature_data["features"],
                "relationships_info": relationships_info,
            }
        )

        async with self.persistent.start_transaction() as session:
            insert_id = await session.insert_one(
                collection_name=self.collection_name,
                document=document.dict(by_alias=True),
                user_id=self.user.id,
            )
            assert insert_id == document.id

            try:
                feature_list_namespace = await self.feature_list_namespace_service.get_document(
                    document_id=document.feature_list_namespace_id,
                )
                await validate_feature_list_version_and_namespace_consistency(
                    feature_list=document,
                    feature_list_namespace=feature_list_namespace,
                    feature_service=self.feature_service,
                )
                feature_list_namespace = await self.feature_list_namespace_service.update_document(
                    document_id=document.feature_list_namespace_id,
                    data=FeatureListNamespaceServiceUpdate(
                        feature_list_ids=self.include_object_id(
                            feature_list_namespace.feature_list_ids, document.id
                        ),
                    ),
                    return_document=True,
                )  # type: ignore[assignment]
                assert feature_list_namespace is not None

            except DocumentNotFoundError:
                await self.feature_list_namespace_service.create_document(
                    data=FeatureListNamespaceModel(
                        _id=document.feature_list_namespace_id or ObjectId(),
                        name=document.name,
                        feature_list_ids=[insert_id],
                        readiness_distribution=document.readiness_distribution,
                        default_feature_list_id=insert_id,
                        default_version_mode=DefaultVersionMode.AUTO,
                        features=feature_data["features"],
                    )
                )

            # update feature's feature_list_ids attribute
            await self._update_features(document.feature_ids, inserted_feature_list_id=insert_id)
        return await self.get_document(document_id=insert_id)

    async def delete_document(
        self,
        document_id: ObjectId,
        exception_detail: Optional[str] = None,
        use_raw_query_filter: bool = False,
        **kwargs: Any,
    ) -> int:
        feature_list = await self.get_document(document_id=document_id)
        async with self.persistent.start_transaction():
            deleted_count = await super().delete_document(document_id=document_id)
            feature_list_namespace = await self.feature_list_namespace_service.get_document(
                document_id=feature_list.feature_list_namespace_id
            )
            feature_list_namespace = await self.feature_list_namespace_service.update_document(
                document_id=feature_list.feature_list_namespace_id,
                data=FeatureListNamespaceServiceUpdate(
                    feature_list_ids=self.exclude_object_id(
                        feature_list_namespace.feature_list_ids, document_id
                    ),
                ),
                return_document=True,
            )  # type: ignore[assignment]

            # update feature's feature_list_ids attribute
            await self._update_features(
                feature_ids=feature_list.feature_ids, deleted_feature_list_id=document_id
            )

            if not feature_list_namespace.feature_list_ids:
                # delete feature list namespace if it has no more feature list
                await self.feature_list_namespace_service.delete_document(
                    document_id=feature_list.feature_list_namespace_id
                )

        return deleted_count

    async def get_feature_list_info(self, document_id: ObjectId, verbose: bool) -> FeatureListInfo:
        """
        Get feature list info

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Verbose or not

        Returns
        -------
        FeatureListInfo
        """
        feature_list = await self.get_document(document_id=document_id)
        namespace_info = await self.feature_list_namespace_service.get_feature_list_namespace_info(
            document_id=feature_list.feature_list_namespace_id,
            verbose=verbose,
        )
        default_feature_list = await self.get_document(
            document_id=namespace_info.default_feature_list_id
        )
        versions_info = None
        if verbose:
            namespace = await self.feature_list_namespace_service.get_document(
                document_id=feature_list.feature_list_namespace_id
            )
            versions_info = FeatureListBriefInfoList.from_paginated_data(
                await self.list_documents_as_dict(
                    page=1,
                    page_size=0,
                    query_filter={"_id": {"$in": namespace.feature_list_ids}},
                )
            )

        namespace_info_dict = namespace_info.dict()
        # use feature list description instead of namespace description
        namespace_description = namespace_info_dict.pop("description", None)
        return FeatureListInfo(
            **namespace_info_dict,
            version={
                "this": feature_list.version.to_str() if feature_list.version else None,
                "default": default_feature_list.version.to_str()
                if default_feature_list.version
                else None,
            },
            production_ready_fraction={
                "this": feature_list.readiness_distribution.derive_production_ready_fraction(),
                "default": default_feature_list.readiness_distribution.derive_production_ready_fraction(),
            },
            default_feature_fraction=compute_default_feature_fraction(
                feature_list, default_feature_list, namespace_info
            ),
            versions_info=versions_info,
            deployed=feature_list.deployed,
            namespace_description=namespace_description,
            description=feature_list.description,
        )


class AllFeatureListService(
    BaseDocumentService[FeatureListModel, FeatureListServiceCreate, FeatureListServiceUpdate]
):
    """
    AllFeatureListService class
    """

    document_class = FeatureListModel

    @property
    def is_catalog_specific(self) -> bool:
        return False
