"""
FeatureService class
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from bson import ObjectId
from redis import Redis

from featurebyte.exception import DocumentCreationError, DocumentNotFoundError
from featurebyte.models.base import VersionIdentifier
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_namespace import DefaultVersionMode, FeatureReadiness
from featurebyte.persistent import Persistent
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.transform.offline_store_ingest import (
    OfflineStoreIngestQueryGraphTransformer,
)
from featurebyte.routes.block_modification_handler import BlockModificationHandler
from featurebyte.routes.common.derive_primary_entity_helper import DerivePrimaryEntityHelper
from featurebyte.schema.feature import FeatureServiceCreate
from featurebyte.schema.feature_namespace import (
    FeatureNamespaceCreate,
    FeatureNamespaceServiceUpdate,
)
from featurebyte.service.base_feature_service import BaseFeatureService
from featurebyte.service.entity_relationship_extractor import EntityRelationshipExtractorService
from featurebyte.service.entity_serving_names import EntityServingNamesService
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.feature_offline_store_info import OfflineStoreInfoInitializationService
from featurebyte.service.namespace_handler import (
    NamespaceHandler,
    validate_version_and_namespace_consistency,
)
from featurebyte.service.table import TableService
from featurebyte.storage import Storage


class FeatureService(BaseFeatureService[FeatureModel, FeatureServiceCreate]):
    """
    FeatureService class
    """

    document_class = FeatureModel

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: Optional[ObjectId],
        block_modification_handler: BlockModificationHandler,
        entity_relationship_extractor_service: EntityRelationshipExtractorService,
        derive_primary_entity_helper: DerivePrimaryEntityHelper,
        table_service: TableService,
        feature_namespace_service: FeatureNamespaceService,
        namespace_handler: NamespaceHandler,
        entity_serving_names_service: EntityServingNamesService,
        offline_store_info_initialization_service: OfflineStoreInfoInitializationService,
        storage: Storage,
        redis: Redis[Any],
    ):
        super().__init__(
            user=user,
            persistent=persistent,
            catalog_id=catalog_id,
            block_modification_handler=block_modification_handler,
            entity_relationship_extractor_service=entity_relationship_extractor_service,
            derive_primary_entity_helper=derive_primary_entity_helper,
            storage=storage,
            redis=redis,
        )
        self.table_service = table_service
        self.feature_namespace_service = feature_namespace_service
        self.namespace_handler = namespace_handler
        self.entity_serving_names_service = entity_serving_names_service
        self.offline_store_info_initialization_service = offline_store_info_initialization_service

    async def prepare_feature_model(
        self, data: FeatureServiceCreate, sanitize_for_definition: bool
    ) -> FeatureModel:
        """
        Prepare the feature model by pruning the query graph

        Parameters
        ----------
        data: FeatureServiceCreate
            Feature creation data
        sanitize_for_definition: bool
            Whether to sanitize the query graph for generating feature definition

        Returns
        -------
        FeatureModel
        """
        data_dict = data.model_dump(by_alias=True)
        graph = QueryGraph(**data_dict.pop("graph"))
        node = graph.get_node_by_name(data_dict.pop("node_name"))
        # Prepare the graph to store. This performs the required pruning steps to remove any
        # unnecessary operations or parameters from the graph.
        prepared_graph, prepared_node_name = await self.namespace_handler.prepare_graph_to_store(
            graph=graph,
            node=node,
            sanitize_for_definition=sanitize_for_definition,
        )

        # derived attributes
        derived_data = await self.extract_derived_data(
            graph=prepared_graph, node_name=prepared_node_name
        )

        feature_dict = {
            **{
                **data_dict,
                "graph": prepared_graph,
                "node_name": prepared_node_name,
                "readiness": FeatureReadiness.DRAFT,
                "primary_entity_ids": derived_data.primary_entity_ids,
                "relationships_info": derived_data.relationships_info,
                "version": await self.get_document_version(data.name),
                "user_id": self.user.id,
                "catalog_id": self.catalog_id,
                "agg_result_name_include_serving_names": True,
            }
        }
        if sanitize_for_definition:
            # since the feature model is created for definition, actual aggregation attributes are not
            # required. Add aggregation attributes to avoid triggering unnecessary aggregation attributes
            # derivation.
            feature_dict["aggregation_ids"] = ["dummy_aggregation_id"]
            feature_dict["aggregation_result_names"] = ["dummy_aggregation_result_name"]

        feature = FeatureModel(**feature_dict)
        if sanitize_for_definition:
            return feature

        # derive entity join steps
        store_info_service = self.offline_store_info_initialization_service
        entity_id_to_serving_name = {
            entity_id: entity.serving_names[0]
            for entity_id, entity in derived_data.entity_id_to_entity.items()
        }
        feature_dict[
            "entity_join_steps"
        ] = await store_info_service.get_entity_join_steps_for_feature_table(
            feature=feature, entity_id_to_serving_name=entity_id_to_serving_name
        )
        return FeatureModel(**feature_dict)

    @staticmethod
    def validate_feature(feature: FeatureModel) -> None:
        """
        Validate feature model before saving

        Parameters
        ----------
        feature: FeatureModel
            Feature model to validate

        Raises
        ------
        DocumentCreationError
            If the feature's feature job settings are not consistent
        """
        # validate feature job settings are consistent
        table_id_feature_job_settings = feature.extract_table_id_feature_job_settings()
        table_id_to_feature_job_setting = {}
        for table_id_feature_job_setting in table_id_feature_job_settings:
            table_id = table_id_feature_job_setting.table_id
            feature_job_setting = table_id_feature_job_setting.feature_job_setting
            if table_id not in table_id_to_feature_job_setting:
                table_id_to_feature_job_setting[table_id] = feature_job_setting
            else:
                if table_id_to_feature_job_setting[table_id] != feature_job_setting:
                    raise DocumentCreationError(
                        f"Feature job settings for table {table_id} are not consistent. "
                        f"Two different feature job settings are found: "
                        f"{table_id_to_feature_job_setting[table_id]} and {feature_job_setting}"
                    )

        # validate feature with UDFs
        if feature.used_user_defined_function:
            transformer = OfflineStoreIngestQueryGraphTransformer(graph=feature.graph)
            assert feature.name is not None
            result = transformer.transform(
                target_node=feature.node,
                relationships_info=feature.relationships_info or [],
                feature_name=feature.name,
                feature_version=feature.version.to_str(),
            )
            if result.is_decomposed:
                # if the graph is decomposed, it implies that on-demand-function is used when the
                # feature is online-enabled. Check whether the UDF is used in the on-demand function.
                decom_graph = result.graph
                decom_node = result.graph.get_node_by_name(result.node_name_map[feature.node.name])
                if decom_graph.has_node_type(
                    target_node=decom_node, node_type=NodeType.GENERIC_FUNCTION
                ):
                    raise DocumentCreationError(
                        "This feature requires a Python on-demand function during deployment. "
                        "We cannot proceed with creating the feature because the on-demand function involves a UDF, "
                        "and the Python version of the UDF is not supported at the moment."
                    )

    async def create_document(self, data: FeatureServiceCreate) -> FeatureModel:
        document = await self.prepare_feature_model(data=data, sanitize_for_definition=False)
        self.validate_feature(feature=document)

        async with self.persistent.start_transaction() as session:
            # check any conflict with existing documents
            await self._check_document_unique_constraints(document=document)

            # prepare feature definition
            definition = await self.namespace_handler.prepare_definition(document=document)

            # insert the document
            definition_hash_output = document.extract_definition_hash()
            insert_id = await session.insert_one(
                collection_name=self.collection_name,
                document={
                    **document.model_dump(by_alias=True),
                    "definition": definition,
                    "definition_hash": definition_hash_output.definition_hash,
                    "raw_graph": data.graph.model_dump(),
                },
                user_id=self.user.id,
            )
            assert insert_id == document.id

            try:
                feature_namespace = await self.feature_namespace_service.get_document(
                    document_id=document.feature_namespace_id,
                )
                await validate_version_and_namespace_consistency(
                    base_model=document,
                    base_namespace_model=feature_namespace,
                    attributes=["name", "dtype", "entity_ids", "table_ids"],
                )
                await self.feature_namespace_service.update_document(
                    document_id=document.feature_namespace_id,
                    data=FeatureNamespaceServiceUpdate(
                        feature_ids=self.include_object_id(
                            feature_namespace.feature_ids, document.id
                        )
                    ),
                    return_document=True,
                )
            except DocumentNotFoundError:
                await self.feature_namespace_service.create_document(
                    data=FeatureNamespaceCreate(
                        _id=document.feature_namespace_id,
                        name=document.name,
                        dtype=document.dtype,
                        feature_ids=[insert_id],
                        readiness=FeatureReadiness.DRAFT,
                        default_feature_id=insert_id,
                        default_version_mode=DefaultVersionMode.AUTO,
                        entity_ids=sorted(document.entity_ids),
                        table_ids=sorted(document.table_ids),
                    ),
                )
        return await self.get_document(document_id=insert_id)

    async def get_document_by_name_and_version(
        self, name: str, version: VersionIdentifier
    ) -> FeatureModel:
        """
        Retrieve feature given name & version

        Parameters
        ----------
        name: str
            Feature name
        version: VersionIdentifier
            Feature version

        Returns
        -------
        FeatureModel

        Raises
        ------
        DocumentNotFoundError
            If the specified feature name & version cannot be found
        """
        out_feat = None
        query_filter = {"name": name, "version": version.model_dump()}
        async for feat in self.list_documents_iterator(query_filter=query_filter):
            out_feat = feat

        if out_feat is None:
            exception_detail = (
                f'{self.class_name} (name: "{name}", version: "{version.to_str()}") not found. '
                f"Please save the {self.class_name} object first."
            )
            raise DocumentNotFoundError(exception_detail)
        return out_feat

    async def update_readiness(self, document_id: ObjectId, readiness: FeatureReadiness) -> None:
        """
        Update feature readiness

        Parameters
        ----------
        document_id: ObjectId
            Feature id
        readiness: FeatureReadiness
            Feature readiness
        """
        document = await self.get_document_as_dict(
            document_id=document_id,
            projection={"block_modification_by": 1},
        )
        self._check_document_modifiable(document=document)

        await self.persistent.update_one(
            collection_name=self.collection_name,
            query_filter=await self.construct_get_query_filter(document_id=document_id),
            update={"$set": {"readiness": str(readiness)}},
            user_id=self.user.id,
            disable_audit=self.should_disable_audit,
        )

    async def update_offline_store_info(
        self, document_id: ObjectId, store_info: Dict[str, Any]
    ) -> None:
        """
        Update offline store info for a feature

        Parameters
        ----------
        document_id: ObjectId
            Feature id
        store_info: Dict[str, Any]
            Offline store info
        """
        document = await self.get_document_as_dict(
            document_id=document_id,
            projection={"block_modification_by": 1},
        )
        self._check_document_modifiable(document=document)

        await self.persistent.update_one(
            collection_name=self.collection_name,
            query_filter=await self.construct_get_query_filter(document_id=document_id),
            update={"$set": {"offline_store_info": store_info}},
            user_id=self.user.id,
            disable_audit=self.should_disable_audit,
        )

    async def update_last_updated_by_scheduled_task_at(
        self, aggregation_ids: list[str], last_updated_by_scheduled_task_at: datetime
    ) -> None:
        """
        Update last updated date for features with the given aggregation ids

        Parameters
        ----------
        aggregation_ids: list[str]
            aggregation ids to filter features
        last_updated_by_scheduled_task_at: datetime
            last updated date
        """
        await self.update_documents(
            query_filter={"aggregation_ids": {"$in": aggregation_ids}},
            update={
                "$set": {"last_updated_by_scheduled_task_at": last_updated_by_scheduled_task_at}
            },
        )

    async def get_sample_entity_serving_names(
        self, feature_id: ObjectId, count: int
    ) -> List[Dict[str, str]]:
        """
        Get sample entity serving names for a feature

        Parameters
        ----------
        feature_id: ObjectId
            Feature Id
        count: int
            Number of sample entity serving names to return

        Returns
        -------
        List[Dict[str, str]]
        """
        feature = await self.get_document(feature_id)

        # get entities and tables used for the feature list
        return await self.entity_serving_names_service.get_sample_entity_serving_names(
            entity_ids=feature.entity_ids,
            table_ids=feature.table_ids,
            count=count,
        )

    async def get_online_enabled_feature_ids(self) -> List[ObjectId]:
        """
        Get the ids of the features that are online enabled

        Returns
        -------
        List[ObjectId]
        """
        enabled_feature_ids = []
        async for doc in self.list_documents_as_dict_iterator(
            query_filter={"online_enabled": True},
            projection={"_id": 1},
        ):
            enabled_feature_ids.append(doc["_id"])
        return enabled_feature_ids

    async def get_online_disabled_feature_ids(self) -> List[ObjectId]:
        """
        Get the ids of the features that are online disabled

        Returns
        -------
        List[ObjectId]
        """
        disabled_feature_ids = []
        async for doc in self.list_documents_as_dict_iterator(
            query_filter={"online_enabled": False},
            projection={"_id": 1},
        ):
            disabled_feature_ids.append(doc["_id"])
        return disabled_feature_ids
