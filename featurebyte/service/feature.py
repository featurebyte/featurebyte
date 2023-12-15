"""
FeatureService class
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from datetime import datetime

from bson import ObjectId

from featurebyte.exception import DocumentCreationError, DocumentNotFoundError
from featurebyte.models.base import VersionIdentifier
from featurebyte.models.deployment import FeastIntegrationSettings
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_namespace import DefaultVersionMode, FeatureReadiness
from featurebyte.persistent import Persistent
from featurebyte.query_graph.graph import QueryGraph
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
from featurebyte.service.namespace_handler import (
    NamespaceHandler,
    validate_version_and_namespace_consistency,
)
from featurebyte.service.table import TableService


class FeatureService(BaseFeatureService[FeatureModel, FeatureServiceCreate]):
    """
    FeatureService class
    """

    document_class = FeatureModel

    def __init__(  # pylint: disable=too-many-arguments
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
    ):
        super().__init__(
            user=user,
            persistent=persistent,
            catalog_id=catalog_id,
            block_modification_handler=block_modification_handler,
            entity_relationship_extractor_service=entity_relationship_extractor_service,
            derive_primary_entity_helper=derive_primary_entity_helper,
        )
        self.table_service = table_service
        self.feature_namespace_service = feature_namespace_service
        self.namespace_handler = namespace_handler
        self.entity_serving_names_service = entity_serving_names_service

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
        data_dict = data.dict(by_alias=True)
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

        return FeatureModel(
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
            }
        )

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
        # validate feature model
        table_id_feature_job_settings = feature.extract_table_id_feature_job_settings()
        table_id_to_feature_job_setting = {}
        for table_id_feature_job_setting in table_id_feature_job_settings:
            table_id = table_id_feature_job_setting.table_id
            feature_job_setting = table_id_feature_job_setting.feature_job_setting
            if table_id not in table_id_to_feature_job_setting:
                table_id_to_feature_job_setting[table_id] = feature_job_setting
            else:
                if (
                    table_id_to_feature_job_setting[table_id].to_seconds()
                    != feature_job_setting.to_seconds()
                ):
                    raise DocumentCreationError(
                        f"Feature job settings for table {table_id} are not consistent. "
                        f"Two different feature job settings are found: "
                        f"{table_id_to_feature_job_setting[table_id]} and {feature_job_setting}"
                    )

    async def create_document(self, data: FeatureServiceCreate) -> FeatureModel:
        document = await self.prepare_feature_model(data=data, sanitize_for_definition=False)
        self.validate_feature(feature=document)

        async with self.persistent.start_transaction() as session:
            # check any conflict with existing documents
            await self._check_document_unique_constraints(document=document)

            # prepare feature definition
            definition = await self.namespace_handler.prepare_definition(document=document)

            if FeastIntegrationSettings().FEATUREBYTE_FEAST_INTEGRATION_ENABLED:
                service = self.entity_serving_names_service
                entity_id_to_serving_name = (
                    await service.get_entity_id_to_serving_name_for_offline_store(
                        entity_ids=document.entity_ids
                    )
                )
                document.initialize_offline_store_info(
                    entity_id_to_serving_name=entity_id_to_serving_name
                )

            # insert the document
            definition_hash_output = document.extract_definition_hash()
            insert_id = await session.insert_one(
                collection_name=self.collection_name,
                document={
                    **document.dict(by_alias=True),
                    "definition": definition,
                    "definition_hash": definition_hash_output.definition_hash,
                    "raw_graph": data.graph.dict(),
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
        query_filter = {"name": name, "version": version.dict()}
        async for feat in self.list_documents_iterator(query_filter=query_filter, page_size=1):
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
            query_filter=self._construct_get_query_filter(document_id=document_id),
            update={"$set": {"readiness": str(readiness)}},
            user_id=self.user.id,
            disable_audit=self.should_disable_audit,
        )

    async def update_last_updated_by_scheduled_task_at(
        self, aggregation_id: str, last_updated_by_scheduled_task_at: datetime
    ) -> None:
        """
        Update last updated date for features with the given aggregation id

        Parameters
        ----------
        aggregation_id: str
            aggregation id
        last_updated_by_scheduled_task_at: datetime
            last updated date
        """
        await self.update_documents(
            query_filter={"aggregation_ids": aggregation_id},
            update={
                "$set": {"last_updated_by_scheduled_task_at": last_updated_by_scheduled_task_at}
            },
        )

    async def get_sample_entity_serving_names(  # pylint: disable=too-many-locals
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
