"""
FeatureListService class
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncIterator, Callable, Coroutine, Dict, List, Optional, Sequence, cast

from bson import ObjectId, json_util
from redis import Redis
from redis.lock import Lock

from featurebyte.common.model_util import get_version
from featurebyte.common.progress import get_ranged_progress_callback
from featurebyte.common.utils import timer
from featurebyte.exception import DocumentError, DocumentInconsistencyError, DocumentNotFoundError
from featurebyte.logging import get_logger
from featurebyte.models.base import VersionIdentifier
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_list import (
    FeatureCluster,
    FeatureListModel,
    FeatureMetadata,
    FeatureReadinessDistribution,
)
from featurebyte.models.feature_list_namespace import FeatureListNamespaceModel
from featurebyte.models.persistent import QueryFilter
from featurebyte.persistent import Persistent
from featurebyte.query_graph.model.entity_lookup_plan import EntityLookupPlanner
from featurebyte.query_graph.model.entity_relationship_info import (
    EntityRelationshipInfo,
    FeatureEntityLookupInfo,
)
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
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.feature_offline_store_info import OfflineStoreInfoInitializationService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.relationship_info import RelationshipInfoService
from featurebyte.service.validator.entity_relationship_validator import (
    FeatureListEntityRelationshipValidator,
)
from featurebyte.storage import Storage

FEATURE_LIST_CREATION_REDIS_LOCK_TIMEOUT = 60

logger = get_logger(__name__)


def validate_feature_list_version_and_namespace_consistency(
    feature_list: FeatureListModel,
    feature_list_namespace: FeatureListNamespaceModel,
    feature_namespace_ids: Sequence[ObjectId],
) -> None:
    """
    Validate whether the feature list & feature list namespace are consistent

    Parameters
    ----------
    feature_list: FeatureListModel
        Feature list object
    feature_list_namespace: FeatureListNamespaceModel
        Feature list namespace object
    feature_namespace_ids: Sequence[ObjectId]
        List of feature namespace ids of the feature list

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
    features_entity_lookup_info: List[FeatureEntityLookupInfo]


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
        feature_store_service: FeatureStoreService,
        entity_service: EntityService,
        relationship_info_service: RelationshipInfoService,
        feature_service: FeatureService,
        feature_namespace_service: FeatureNamespaceService,
        feature_list_namespace_service: FeatureListNamespaceService,
        block_modification_handler: BlockModificationHandler,
        entity_serving_names_service: EntityServingNamesService,
        entity_relationship_extractor_service: EntityRelationshipExtractorService,
        feature_list_entity_relationship_validator: FeatureListEntityRelationshipValidator,
        offline_store_info_initialization_service: OfflineStoreInfoInitializationService,
        storage: Storage,
        redis: Redis[Any],
    ):
        super().__init__(
            user=user,
            persistent=persistent,
            catalog_id=catalog_id,
            block_modification_handler=block_modification_handler,
            storage=storage,
            redis=redis,
        )
        self.feature_store_service = feature_store_service
        self.entity_service = entity_service
        self.relationship_info_service = relationship_info_service
        self.feature_service = feature_service
        self.feature_namespace_service = feature_namespace_service
        self.feature_list_namespace_service = feature_list_namespace_service
        self.entity_serving_names_service = entity_serving_names_service
        self.entity_relationship_extractor_service = entity_relationship_extractor_service
        self.feature_list_entity_relationship_validator = feature_list_entity_relationship_validator
        self.offline_store_info_initialization_service = offline_store_info_initialization_service

    def get_feature_list_creation_lock(self, timeout: int) -> Lock:
        """
        Get feature list creation lock

        Parameters
        ----------
        timeout: int
            Maximum life for the lock in seconds

        Returns
        -------
        Lock
        """
        return self.redis.lock(f"feature_list_creation:{self.catalog_id}", timeout=timeout)

    async def _populate_remote_attributes(self, document: FeatureListModel) -> FeatureListModel:
        if document.feature_clusters_path:
            feature_clusters = await self.storage.get_text(
                Path(document.feature_clusters_path),
                cache_key=document.feature_clusters_path,
            )
            document.internal_feature_clusters = json_util.loads(feature_clusters)
        return document

    async def _move_feature_cluster_to_storage(
        self, document: FeatureListModel
    ) -> FeatureListModel:
        feature_cluster_path = self.get_full_remote_file_path(
            f"feature_list/{document.id}/feature_clusters.json"
        )
        feature_clusters = []
        assert document.internal_feature_clusters is not None
        for cluster in document.internal_feature_clusters:
            if isinstance(cluster, FeatureCluster):
                feature_clusters.append(cluster.model_dump(by_alias=True))
            else:
                feature_clusters.append(dict(cluster))
        await self.storage.put_text(json_util.dumps(feature_clusters), feature_cluster_path)
        document.feature_clusters_path = str(feature_cluster_path)
        document.internal_feature_clusters = None
        return document

    async def extract_features_metadata(
        self,
        feature_ids: List[ObjectId],
        feature_id_to_namespace_id: Optional[Dict[ObjectId, ObjectId]] = None,
    ) -> List[FeatureMetadata]:
        """
        Extract features metadata given the feature ids

        Parameters
        ----------
        feature_ids: List[ObjectId]
            List of feature ids
        feature_id_to_namespace_id: Optional[Dict[ObjectId, ObjectId]]
            Mapping of feature id to feature namespace id

        Returns
        -------
        List[FeatureMetadata]
        """
        if feature_id_to_namespace_id is None:
            feature_id_to_namespace_id = {
                feature.id: feature.feature_namespace_id
                async for feature in self.feature_service.list_documents_iterator(
                    query_filter={"_id": {"$in": feature_ids}}
                )
            }

        # extract features metadata
        namespace_to_feature_type = {}
        async for feature_namespace in self.feature_namespace_service.list_documents_iterator(
            query_filter={"_id": {"$in": list(feature_id_to_namespace_id.values())}}
        ):
            namespace_to_feature_type[feature_namespace.id] = feature_namespace.feature_type

        return [
            FeatureMetadata(
                feature_id=feature_id,
                feature_type=namespace_to_feature_type[feature_id_to_namespace_id[feature_id]],
            )
            for feature_id in feature_ids
        ]

    async def _extract_feature_data(self, document: FeatureListModel) -> Dict[str, Any]:
        feature_store_id: Optional[ObjectId] = None
        feature_namespace_ids = set()
        features = []
        feature_id_to_feature = {
            feature.id: feature
            async for feature in self.feature_service.list_documents_iterator(
                query_filter={"_id": {"$in": document.feature_ids}},
            )
        }
        for feature_id in document.feature_ids:
            if feature_id not in feature_id_to_feature:
                # call get_document to raise DocumentNotFoundError
                await self.feature_service.get_document(document_id=feature_id)

            # retrieve feature from the persistent
            feature = feature_id_to_feature[feature_id]
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

        # construct features metadata
        features_metadata = await self.extract_features_metadata(
            feature_ids=document.feature_ids,
            feature_id_to_namespace_id={
                feature.id: feature.feature_namespace_id for feature in features
            },
        )

        derived_output = {
            "feature_store_id": feature_store_id,
            "features": features,
            "features_metadata": features_metadata,
        }
        return derived_output

    async def extract_entity_relationship_data(
        self,
        features: List[FeatureModel],
        progress_callback: Optional[Callable[..., Coroutine[Any, Any, None]]] = None,
    ) -> FeatureListEntityRelationshipData:
        """
        Extract entity relationship data from feature models

        Parameters
        ----------
        features: List[FeatureModel]
            List of feature models
        progress_callback: Optional[Callable[..., Coroutine[Any, Any, None]]]
            Progress callback

        Returns
        -------
        FeatureListEntityRelationshipData
        """
        feature_primary_entity_ids = [feature.primary_entity_ids for feature in features]
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
        features_entity_lookup_info = []
        entity_id_to_serving_name = (
            await self.entity_serving_names_service.get_entity_id_to_serving_name_for_offline_store(
                entity_ids=list(set().union(*[feature.entity_ids for feature in features]))
            )
        )
        store_info_service = self.offline_store_info_initialization_service
        for idx, feature in enumerate(features):
            feature_list_to_feature_primary_entity_join_steps = (
                EntityLookupPlanner.generate_lookup_steps(
                    available_entity_ids=fl_primary_entity_ids,
                    required_entity_ids=feature.primary_entity_ids,
                    relationships_info=relationships_info,
                )
            )
            feature_internal_entity_join_steps = (
                await store_info_service.get_entity_join_steps_for_feature_table(
                    feature=feature, entity_id_to_serving_name=entity_id_to_serving_name
                )
            )
            feature_entity_lookup_info = FeatureEntityLookupInfo(
                feature_id=feature.id,
                feature_list_to_feature_primary_entity_join_steps=feature_list_to_feature_primary_entity_join_steps,
                feature_internal_entity_join_steps=feature_internal_entity_join_steps,
            )
            if feature_entity_lookup_info.join_steps:
                features_entity_lookup_info.append(feature_entity_lookup_info)

            if progress_callback:
                await progress_callback(
                    (idx + 1) * 100 // len(features),
                    f"Extracting entity relationship data for feature {feature.name}",
                )

        return FeatureListEntityRelationshipData(
            primary_entity_ids=fl_primary_entity_ids,
            relationships_info=relationships_info,
            supported_serving_entity_ids=supported_serving_entity_ids,
            features_entity_lookup_info=features_entity_lookup_info,
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

    async def _clean_up_mongo_create_document(
        self,
        feature_list: FeatureListModel,
        feature_list_namespace: Optional[FeatureListNamespaceModel],
    ) -> None:
        await self.persistent.delete_one(
            collection_name=self.collection_name,
            query_filter={"_id": feature_list.id},
            user_id=self.user.id,
            disable_audit=self.should_disable_audit,
        )
        if feature_list_namespace:
            feature_list_ids = self.exclude_object_id(
                feature_list_namespace.feature_list_ids, feature_list.id
            )
            if feature_list_ids:
                await self.feature_list_namespace_service.update_document(
                    document_id=feature_list.feature_list_namespace_id,
                    data=FeatureListNamespaceServiceUpdate(feature_list_ids=feature_list_ids),
                    return_document=False,
                )
            else:
                await self.feature_list_namespace_service.delete_document(
                    document_id=feature_list.feature_list_namespace_id
                )

        await self._update_features(
            feature_list.feature_ids, deleted_feature_list_id=feature_list.id
        )

    async def _create_document(
        self, feature_list: FeatureListModel, features: List[FeatureModel]
    ) -> ObjectId:
        assert feature_list.feature_clusters is None
        feature_list_doc = feature_list.model_dump(by_alias=True)
        feature_namespace_ids = [feature.feature_namespace_id for feature in features]

        # check whether feature list namespace exists or not
        feature_list_namespace = None
        try:
            feature_list_namespace = await self.feature_list_namespace_service.get_document(
                document_id=feature_list.feature_list_namespace_id,
            )
            validate_feature_list_version_and_namespace_consistency(
                feature_list=feature_list,
                feature_list_namespace=feature_list_namespace,
                feature_namespace_ids=feature_namespace_ids,
            )
        except DocumentNotFoundError:
            pass

        # create feature list document
        with self.get_feature_list_creation_lock(FEATURE_LIST_CREATION_REDIS_LOCK_TIMEOUT):
            try:
                insert_id = await self.persistent.insert_one(
                    collection_name=self.collection_name,
                    document=feature_list_doc,
                    user_id=self.user.id,
                )
                assert insert_id == feature_list.id

                if feature_list_namespace:
                    await self.feature_list_namespace_service.update_document(
                        document_id=feature_list.feature_list_namespace_id,
                        data=FeatureListNamespaceServiceUpdate(
                            feature_list_ids=self.include_object_id(
                                feature_list_namespace.feature_list_ids, feature_list.id
                            ),
                        ),
                        return_document=True,
                    )
                else:
                    feature_list_namespace = (
                        await self.feature_list_namespace_service.create_document(
                            data=FeatureListNamespaceModel(
                                _id=feature_list.feature_list_namespace_id or ObjectId(),
                                name=feature_list.name,
                                feature_list_ids=[insert_id],
                                default_feature_list_id=insert_id,
                                features=features,
                            )
                        )
                    )

                # update feature's feature_list_ids attribute
                await self._update_features(
                    feature_list.feature_ids, inserted_feature_list_id=insert_id
                )
                return cast(ObjectId, insert_id)
            except Exception as exc:
                await self._clean_up_mongo_create_document(
                    feature_list=feature_list, feature_list_namespace=feature_list_namespace
                )
                raise exc

    async def create_document(
        self,
        data: FeatureListServiceCreate,
        progress_callback: Optional[Callable[..., Coroutine[Any, Any, None]]] = None,
    ) -> FeatureListModel:
        # sort feature_ids before saving to persistent storage to ease feature_ids comparison in uniqueness check
        document = FeatureListModel(**{
            **data.model_dump(by_alias=True),
            "version": await self._get_feature_list_version(data.name),
            "user_id": self.user.id,
            "catalog_id": self.catalog_id,
        })
        # check any conflict with existing documents
        await self._check_document_unique_constraints(document=document)

        # check whether the feature(s) in the feature list saved to persistent or not
        if progress_callback:
            await progress_callback(10, "Extracting feature data")
        feature_data = await self._extract_feature_data(document)

        if progress_callback:
            await progress_callback(30, "Extracting entity relationship data")
        entity_relationship_data = await self.extract_entity_relationship_data(
            features=feature_data["features"],
            progress_callback=(
                get_ranged_progress_callback(progress_callback, 30, 60)
                if progress_callback
                else None
            ),
        )

        # update document with derived output
        document = FeatureListModel(**{
            **document.model_dump(by_alias=True),
            "features": feature_data["features"],
            "primary_entity_ids": entity_relationship_data.primary_entity_ids,
            "relationships_info": entity_relationship_data.relationships_info,
            "supported_serving_entity_ids": entity_relationship_data.supported_serving_entity_ids,
            "features_entity_lookup_info": entity_relationship_data.features_entity_lookup_info,
            "features_metadata": feature_data["features_metadata"],
        })

        if progress_callback:
            await progress_callback(70, "Moving feature clusters to storage")
        await self._move_feature_cluster_to_storage(document)

        if progress_callback:
            await progress_callback(80, "Creating feature list document")
        try:
            with timer("Creating feature list document", logger):
                insert_id = await self._create_document(
                    feature_list=document, features=feature_data["features"]
                )

        except Exception as exc:
            # clean up the feature_clusters file if the document creation failed
            if document.feature_clusters_path:
                logger.info("Cleaning up feature clusters file after document creation failure")
                await self.storage.delete(Path(document.feature_clusters_path))
            raise exc

        if progress_callback:
            await progress_callback(100, "Feature list document created")
        return await self.get_document(document_id=insert_id)

    async def list_documents_iterator(  # type: ignore[override]
        self,
        query_filter: QueryFilter,
        use_raw_query_filter: bool = False,
        populate_remote_attributes: bool = True,
        **kwargs: Any,
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
            query_filter=await self.construct_get_query_filter(document_id=document_id),
            update={"$set": {"readiness_distribution": readiness_distribution.model_dump()}},
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
        feature_list = await self.get_document(
            document_id=document_id,
            populate_remote_attributes=False,
        )
        await self.feature_list_namespace_service.update_documents(
            query_filter={"_id": feature_list.feature_list_namespace_id},
            update={"$pull": {"feature_list_ids": feature_list.id}},
        )

        # update feature's feature_list_ids attribute
        await self._update_features(
            feature_ids=feature_list.feature_ids, deleted_feature_list_id=feature_list.id
        )

        try:
            feature_list_namespace_dict = (
                await self.feature_list_namespace_service.get_document_as_dict(
                    document_id=feature_list.feature_list_namespace_id
                )
            )
            if not feature_list_namespace_dict["feature_list_ids"]:
                # delete feature list namespace if it has no more feature list
                await self.feature_list_namespace_service.delete_document(
                    document_id=feature_list.feature_list_namespace_id
                )

        except DocumentNotFoundError:
            pass

        return await super().delete_document(document_id=feature_list.id)

    async def get_sample_entity_serving_names(
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
        feature_list = await self.get_document(feature_list_id, populate_remote_attributes=False)

        # get entities and tables used for the feature list
        return await self.entity_serving_names_service.get_sample_entity_serving_names(
            entity_ids=feature_list.entity_ids,
            table_ids=feature_list.table_ids,
            count=count,
        )

    async def iterate_online_enabled_feature_lists_as_dict(self) -> AsyncIterator[dict[str, Any]]:
        """
        Iterate over online enabled feature lists as dictionaries. Such feature lists consist of
        features that are all online enabled.

        Yields
        ------
        dict[str, Any]
            Feature list dict objects that are online enabled
        """
        async for feature_list_dict in self.list_documents_as_dict_iterator(
            query_filter={"online_enabled_feature_ids.0": {"$exists": True}}
        ):
            if sorted(feature_list_dict["feature_ids"]) == sorted(
                feature_list_dict["online_enabled_feature_ids"]
            ):
                yield feature_list_dict


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
