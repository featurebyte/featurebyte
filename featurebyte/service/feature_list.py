"""
FeatureListService class
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

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

    async def _extract_feature_data(self, document: FeatureListModel) -> Dict[str, Any]:
        feature_store_id: Optional[ObjectId] = None
        feature_namespace_ids = set()
        features = []
        feature_service = FeatureService(
            user=self.user, persistent=self.persistent, catalog_id=self.catalog_id
        )
        for feature_id in document.feature_ids:
            # retrieve feature from the persistent
            feature = await feature_service.get_document(document_id=feature_id)
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
        relationship_info_service = RelationshipInfoService(
            user=self.user, persistent=self.persistent, catalog_id=self.catalog_id
        )
        entity_service = EntityService(
            user=self.user, persistent=self.persistent, catalog_id=self.catalog_id
        )
        entity_ids = set()
        for feature in features:
            entity_ids.update(feature.entity_ids)

        ancestor_entity_ids = set(entity_ids)
        async for entity_doc in entity_service.list_documents_iterator(
            query_filter={"_id": {"$in": list(entity_ids)}}
        ):
            entity = EntityModel(**entity_doc)
            ancestor_entity_ids.update(entity.ancestor_ids)

        descendant_entity_ids = set(entity_ids)
        async for entity_doc in entity_service.list_documents_iterator(
            query_filter={"ancestor_ids": {"$in": list(entity_ids)}}
        ):
            entity = EntityModel(**entity_doc)
            descendant_entity_ids.add(entity.id)

        relationships_info = [
            EntityRelationshipInfo(**relationship_info)
            async for relationship_info in relationship_info_service.list_documents_iterator(
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
        self, features: list[FeatureModel], feature_list_id: ObjectId
    ) -> None:
        feature_service = FeatureService(
            user=self.user, persistent=self.persistent, catalog_id=self.catalog_id
        )
        for feature in features:
            await feature_service.update_document(
                document_id=feature.id,
                data=FeatureServiceUpdate(
                    feature_list_ids=self.include_object_id(
                        feature.feature_list_ids, feature_list_id
                    ),
                ),
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
                **data.json_dict(),
                "feature_ids": sorted(data.feature_ids),
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

            feature_list_namespace_service = FeatureListNamespaceService(
                user=self.user,
                persistent=self.persistent,
                catalog_id=self.catalog_id,
            )
            try:
                feature_list_namespace = await feature_list_namespace_service.get_document(
                    document_id=document.feature_list_namespace_id,
                )
                await validate_feature_list_version_and_namespace_consistency(
                    feature_list=document,
                    feature_list_namespace=feature_list_namespace,
                    feature_service=FeatureService(
                        user=self.user, persistent=self.persistent, catalog_id=self.catalog_id
                    ),
                )
                feature_list_namespace = await feature_list_namespace_service.update_document(
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
                await feature_list_namespace_service.create_document(
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
            await self._update_features(feature_data["features"], insert_id)
        return await self.get_document(document_id=insert_id)
