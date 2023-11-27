"""
FeatureListService class
"""
from __future__ import annotations

from typing import Any, AsyncIterator, Dict, List, Optional, Sequence

from dataclasses import dataclass

from bson.objectid import ObjectId

from featurebyte.common.model_util import get_version
from featurebyte.exception import DocumentError, DocumentInconsistencyError, DocumentNotFoundError
from featurebyte.models.base import VersionIdentifier
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_list import (
    FeatureCluster,
    FeatureListModel,
    FeatureListNamespaceModel,
    FeatureReadinessDistribution,
)
from featurebyte.models.persistent import QueryFilter
from featurebyte.persistent import Persistent
from featurebyte.query_graph.model.entity_relationship_info import EntityRelationshipInfo
from featurebyte.routes.block_modification_handler import BlockModificationHandler
from featurebyte.schema.feature_list import FeatureListServiceCreate, FeatureListServiceUpdate
from featurebyte.schema.feature_list_namespace import FeatureListNamespaceServiceUpdate
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.entity import EntityService
from featurebyte.service.entity_relationship_extractor import (
    EntityRelationshipExtractorService,
    ServingEntityEnumeration,
)
from featurebyte.service.entity_serving_names import EntityServingNamesService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService
from featurebyte.service.mixin import DEFAULT_PAGE_SIZE
from featurebyte.service.relationship_info import RelationshipInfoService
from featurebyte.service.validator.entity_relationship_validator import (
    FeatureListEntityRelationshipValidator,
)


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


@dataclass
class FeatureListEntityRelationshipData:
    """
    FeatureListEntityRelationshipData class
    """

    primary_entity_ids: List[ObjectId]
    relationships_info: List[EntityRelationshipInfo]
    supported_serving_entity_ids: List[List[ObjectId]]


class FeatureListService(
    BaseDocumentService[FeatureListModel, FeatureListServiceCreate, FeatureListServiceUpdate]
):
    """
    FeatureListService class
    """

    document_class = FeatureListModel

    def __init__(  # pylint: disable=too-many-arguments
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: Optional[ObjectId],
        entity_service: EntityService,
        relationship_info_service: RelationshipInfoService,
        feature_service: FeatureService,
        feature_list_namespace_service: FeatureListNamespaceService,
        block_modification_handler: BlockModificationHandler,
        entity_serving_names_service: EntityServingNamesService,
        entity_relationship_extractor_service: EntityRelationshipExtractorService,
        feature_list_entity_relationship_validator: FeatureListEntityRelationshipValidator,
    ):
        super().__init__(
            user=user,
            persistent=persistent,
            catalog_id=catalog_id,
            block_modification_handler=block_modification_handler,
        )
        self.entity_service = entity_service
        self.relationship_info_service = relationship_info_service
        self.feature_service = feature_service
        self.feature_list_namespace_service = feature_list_namespace_service
        self.entity_serving_names_service = entity_serving_names_service
        self.entity_relationship_extractor_service = entity_relationship_extractor_service
        self.feature_list_entity_relationship_validator = feature_list_entity_relationship_validator

    async def _feature_iterator(
        self, feature_ids: Sequence[ObjectId]
    ) -> AsyncIterator[FeatureModel]:
        # use this iterator to check whether the feature(s) in the feature list saved to persistent or not
        # if not, raise DocumentNotFoundError
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

        # validate entity relationships
        await self.feature_list_entity_relationship_validator.validate(features=features)

        derived_output = {
            "feature_store_id": feature_store_id,
            "features": features,
        }
        return derived_output

    async def extract_entity_relationship_data(
        self, feature_primary_entity_ids: List[List[ObjectId]]
    ) -> FeatureListEntityRelationshipData:
        """
        Extract entity relationship data from feature primary entity ids

        Parameters
        ----------
        feature_primary_entity_ids: List[List[ObjectId]]
            List of feature primary entity ids

        Returns
        -------
        FeatureListEntityRelationshipData
        """
        combined_primary_entity_ids = set().union(*feature_primary_entity_ids)
        primary_entity_ids = list(combined_primary_entity_ids)
        extractor = self.entity_relationship_extractor_service
        relationships_info = await extractor.extract_primary_entity_descendant_relationship(
            primary_entity_ids=primary_entity_ids
        )
        serving_entity_enumeration = ServingEntityEnumeration.create(
            relationships_info=relationships_info
        )
        fl_primary_entity_ids = serving_entity_enumeration.reduce_entity_ids(
            entity_ids=primary_entity_ids
        )
        supported_serving_entity_ids = serving_entity_enumeration.generate(
            entity_ids=fl_primary_entity_ids
        )
        return FeatureListEntityRelationshipData(
            primary_entity_ids=fl_primary_entity_ids,
            relationships_info=relationships_info,
            supported_serving_entity_ids=supported_serving_entity_ids,
        )

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
        entity_relationship_data = await self.extract_entity_relationship_data(
            feature_primary_entity_ids=[
                feature.primary_entity_ids for feature in feature_data["features"]
            ]
        )

        # update document with derived output
        document = FeatureListModel(
            **{
                **document.dict(by_alias=True),
                "features": feature_data["features"],
                "primary_entity_ids": entity_relationship_data.primary_entity_ids,
                "relationships_info": entity_relationship_data.relationships_info,
                "supported_serving_entity_ids": entity_relationship_data.supported_serving_entity_ids,
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
                        default_feature_list_id=insert_id,
                        features=feature_data["features"],
                    )
                )

            # update feature's feature_list_ids attribute
            await self._update_features(document.feature_ids, inserted_feature_list_id=insert_id)
        return await self.get_document(document_id=insert_id)

    async def list_documents_iterator(  # type: ignore[override]
        self,
        query_filter: QueryFilter,
        page_size: int = DEFAULT_PAGE_SIZE,
        use_raw_query_filter: bool = False,
    ) -> AsyncIterator[FeatureListModel]:
        raise RuntimeError(
            "Do not use this method as it takes long time to deserialize the data, "
            "use list_documents_as_dict_iterator instead"
        )

    async def update_readiness_distribution(
        self,
        document_id: ObjectId,
        readiness_distribution: FeatureReadinessDistribution,
    ) -> None:
        """
        Update readiness distribution

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        readiness_distribution: FeatureReadinessDistribution
            Feature readiness distribution
        """
        document = await self.get_document_as_dict(
            document_id=document_id,
            projection={"block_modification_by": 1},
        )
        self._check_document_modifiable(document=document)

        await self.persistent.update_one(
            collection_name=self.collection_name,
            query_filter=self._construct_get_query_filter(document_id=document_id),
            update={"$set": {"readiness_distribution": readiness_distribution.dict()["__root__"]}},
            user_id=self.user.id,
            disable_audit=self.should_disable_audit,
        )

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

    async def get_feature_clusters(self, feature_list_id: ObjectId) -> List[FeatureCluster]:
        """
        Get list of FeatureCluster from feature_list_id

        Parameters
        ----------
        feature_list_id: ObjectId
            input feature_list_id

        Returns
        -------
        List[FeatureCluster]
        """
        feature_list = await self.get_document(document_id=feature_list_id)

        features = []
        async for feature in self.feature_service.list_documents_iterator(
            query_filter={"_id": {"$in": feature_list.feature_ids}}
        ):
            features.append(feature)

        return FeatureListModel.derive_feature_clusters(features)

    async def get_sample_entity_serving_names(  # pylint: disable=too-many-locals
        self, feature_list_id: ObjectId, count: int
    ) -> List[Dict[str, str]]:
        """
        Get sample entity serving names for a feature list

        Parameters
        ----------
        feature_list_id: ObjectId
            FeatureList Id
        count: int
            Number of sample entity serving names to return

        Returns
        -------
        List[Dict[str, str]]
        """
        feature_list = await self.get_document(feature_list_id)

        # get entities and tables used for the feature list
        return await self.entity_serving_names_service.get_sample_entity_serving_names(
            entity_ids=feature_list.entity_ids,
            table_ids=feature_list.table_ids,
            count=count,
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
