"""
FeatureListService class
"""
from __future__ import annotations

from typing import Any, AsyncIterator, Dict, List, Optional, Sequence, cast

from bson.objectid import ObjectId

from featurebyte.common.model_util import get_version
from featurebyte.exception import DocumentError, DocumentInconsistencyError, DocumentNotFoundError
from featurebyte.models.base import VersionIdentifier
from featurebyte.models.entity import EntityModel
from featurebyte.models.feature import DefaultVersionMode, FeatureModel
from featurebyte.models.feature_list import (
    EntityRelationshipInfo,
    FeatureListModel,
    FeatureListNamespaceModel,
)
from featurebyte.persistent import Persistent
from featurebyte.schema.feature import FeatureServiceUpdate
from featurebyte.schema.feature_list import FeatureListServiceCreate, FeatureListServiceUpdate
from featurebyte.schema.feature_list_namespace import FeatureListNamespaceServiceUpdate
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


class FeatureListService(
    BaseDocumentService[FeatureListModel, FeatureListServiceCreate, FeatureListServiceUpdate]
):
    """
    FeatureListService class
    """

    document_class = FeatureListModel

    def __init__(self, user: Any, persistent: Persistent, catalog_id: ObjectId):
        super().__init__(user=user, persistent=persistent, catalog_id=catalog_id)
        self.entity_service = EntityService(
            user=self.user, persistent=self.persistent, catalog_id=self.catalog_id
        )
        self.relationship_info_service = RelationshipInfoService(
            user=self.user, persistent=self.persistent, catalog_id=self.catalog_id
        )
        self.feature_service = FeatureService(
            user=self.user, persistent=self.persistent, catalog_id=self.catalog_id
        )
        self.feature_list_namespace_service = FeatureListNamespaceService(
            user=self.user, persistent=self.persistent, catalog_id=self.catalog_id
        )

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
        async for entity_doc in self.entity_service.list_documents_iterator(
            query_filter={"_id": {"$in": list(entity_ids)}}
        ):
            entity = EntityModel(**entity_doc)
            ancestor_entity_ids.update(entity.ancestor_ids)

        descendant_entity_ids = set(entity_ids)
        async for entity_doc in self.entity_service.list_documents_iterator(
            query_filter={"ancestor_ids": {"$in": list(entity_ids)}}
        ):
            entity = EntityModel(**entity_doc)
            descendant_entity_ids.add(entity.id)

        relationships_info = [
            EntityRelationshipInfo(**relationship_info)
            async for relationship_info in self.relationship_info_service.list_documents_iterator(
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
        features: list[FeatureModel],
        inserted_feature_list_id: Optional[ObjectId] = None,
        deleted_feature_list_id: Optional[ObjectId] = None,
    ) -> None:
        for feature in features:
            feature_list_ids = cast(List[ObjectId], feature.feature_list_ids)
            if inserted_feature_list_id:
                feature_list_ids = self.include_object_id(
                    feature_list_ids, inserted_feature_list_id
                )
            if deleted_feature_list_id:
                feature_list_ids = self.exclude_object_id(feature_list_ids, deleted_feature_list_id)
            await self.feature_service.update_document(
                document_id=feature.id,
                data=FeatureServiceUpdate(feature_list_ids=feature_list_ids),
                document=feature,
                return_document=False,
            )

    async def _get_feature_list_version(self, name: str) -> VersionIdentifier:
        version_name = get_version()
        query_result = await self.list_documents(
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
            await self._update_features(
                feature_data["features"], inserted_feature_list_id=insert_id
            )
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
            features = [
                feature
                async for feature in self._feature_iterator(feature_ids=feature_list.feature_ids)
            ]
            await self._update_features(
                features=features,
                deleted_feature_list_id=document_id,
            )

            if not feature_list_namespace.feature_list_ids:
                # delete feature list namespace if it has no more feature list
                await self.feature_list_namespace_service.delete_document(
                    document_id=feature_list.feature_list_namespace_id
                )

        return deleted_count
