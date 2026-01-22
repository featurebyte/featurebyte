"""
DeploymentSqlGenerationService
"""

from typing import Optional

from bson import ObjectId
from sqlglot import expressions

from featurebyte.enum import SpecialColumnName
from featurebyte.exception import DeploymentSqlGenerationError
from featurebyte.models import EntityModel
from featurebyte.models.deployment_sql import DeploymentSqlModel, FeatureTableSql
from featurebyte.models.offline_store_feature_table import (
    OfflineStoreFeatureTableModel,
    get_combined_ingest_graph,
)
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.common import (
    CURRENT_TIMESTAMP_PLACEHOLDER,
    get_qualified_column_identifier,
    sql_to_string,
)
from featurebyte.query_graph.sql.deployment import get_deployment_feature_query_plan
from featurebyte.query_graph.sql.partition_filter_helper import get_partition_filters_from_graph
from featurebyte.service.catalog import CatalogService
from featurebyte.service.column_statistics import ColumnStatisticsService
from featurebyte.service.deployment import DeploymentService
from featurebyte.service.entity import EntityService
from featurebyte.service.entity_lookup_feature_table import EntityLookupFeatureTableService
from featurebyte.service.entity_validation import EntityValidationService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_offline_store_info import OfflineStoreInfoInitializationService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.offline_store_feature_table_construction import (
    OfflineStoreFeatureTableConstructionService,
)
from featurebyte.service.offline_store_feature_table_manager import OfflineIngestGraphContainer


class DeploymentSqlGenerationService:
    """
    Service for generating SQL for deployment operations.
    """

    def __init__(
        self,
        catalog_id: ObjectId,
        catalog_service: CatalogService,
        feature_store_service: FeatureStoreService,
        deployment_service: DeploymentService,
        feature_list_service: FeatureListService,
        feature_service: FeatureService,
        offline_store_info_initialization_service: OfflineStoreInfoInitializationService,
        offline_store_feature_table_construction_service: OfflineStoreFeatureTableConstructionService,
        entity_service: EntityService,
        entity_validation_service: EntityValidationService,
        column_statistics_service: ColumnStatisticsService,
        entity_lookup_feature_table_service: EntityLookupFeatureTableService,
    ):
        self.catalog_id = catalog_id
        self.catalog_service = catalog_service
        self.feature_store_service = feature_store_service
        self.deployment_service = deployment_service
        self.feature_list_service = feature_list_service
        self.feature_service = feature_service
        self.offline_store_info_initialization_service = offline_store_info_initialization_service
        self.offline_store_feature_table_construction_service = (
            offline_store_feature_table_construction_service
        )
        self.entity_service = entity_service
        self.entity_validation_service = entity_validation_service
        self.column_statistics_service = column_statistics_service
        self.entity_lookup_feature_table_service = entity_lookup_feature_table_service

    async def generate_deployment_sql(
        self,
        deployment_id: ObjectId,
        output_document_id: Optional[ObjectId] = None,
    ) -> DeploymentSqlModel:
        """
        Generate SQL code for the given deployment ID.

        Parameters
        ----------
        deployment_id: ObjectId
            The ID of the deployment for which to generate SQL.
        output_document_id: Optional[ObjectId]
            Optional ID of the output document to associate with the generated SQL.

        Returns
        -------
        DeploymentSqlModel
            The generated deployment SQL model.

        Raises
        ------
        DeploymentSqlGenerationError
            If deployment SQL generation fails.
        """
        if output_document_id is None:
            output_document_id = ObjectId()

        deployment = await self.deployment_service.get_document(deployment_id)
        feature_list = await self.feature_list_service.get_document(deployment.feature_list_id)
        feature_mapping = {}
        async for feature_model in self.feature_service.list_documents_iterator(
            query_filter={"_id": {"$in": feature_list.feature_ids}}
        ):
            feature_mapping[feature_model.id] = feature_model

        feature_infos = []
        for feature_model in feature_mapping.values():
            # dry_run should be False to set up the table names which is required for
            # OfflineIngestGraphContainer to work correctly.
            info = (
                await self.offline_store_info_initialization_service.initialize_offline_store_info(
                    feature_model,
                    table_name_prefix="temp",
                    dry_run=False,
                    include_feature_version_suffix=False,
                    deployment_sql_generation=True,
                )
            )
            if info.is_decomposed:
                raise DeploymentSqlGenerationError(
                    f"Deployment SQL generation is not supported for feature: {feature_model.name}"
                )
            feature_infos.append((feature_model, info))

        catalog_model = await self.catalog_service.get_document(self.catalog_id)
        feature_store_model = await self.feature_store_service.get_document(
            catalog_model.default_feature_store_ids[0]
        )
        graph_container = OfflineIngestGraphContainer.build_from_feature_infos(feature_infos)
        entity_mapping: dict[ObjectId, EntityModel] = {}
        feature_table_sqls = []

        point_in_time_placeholder = CURRENT_TIMESTAMP_PLACEHOLDER

        for table_name, ingest_graphs in graph_container.offline_store_table_name_to_graphs.items():
            features = graph_container.offline_store_table_name_to_features[table_name]
            ingest_graph = ingest_graphs[0]
            primary_entities = await self._get_entities(
                entity_mapping, ingest_graph.primary_entity_ids
            )

            # Information required for serving parent features
            lookup_entity_universe_expr = None
            lookup_info = None
            if deployment.serving_entity_ids is not None and sorted(
                ingest_graph.primary_entity_ids
            ) != sorted(deployment.serving_entity_ids):
                await self._retrieve_all_entities(entity_mapping)
                lookup_feature_table = await self.entity_lookup_feature_table_service.get_precomputed_lookup_feature_table(
                    primary_entity_ids=ingest_graph.primary_entity_ids,
                    feature_ids=[feature.id for feature in features],
                    feature_list=feature_list,
                    full_serving_entity_ids=deployment.serving_entity_ids,
                    feature_table_name="temp",
                    feature_table_has_ttl=False,
                    entity_id_to_serving_name={
                        entity.id: entity.serving_names[0] for entity in entity_mapping.values()
                    },
                    feature_store_model=feature_store_model,
                    feature_table_id=None,
                )
                if lookup_feature_table is not None:
                    lookup_entity_universe_expr = (
                        lookup_feature_table.entity_universe.get_entity_universe_expr(
                            current_feature_timestamp=point_in_time_placeholder,
                            last_materialized_timestamp=None,
                        )
                    )
                    lookup_info = lookup_feature_table.precomputed_lookup_feature_table_info

            ingest_graph_metadata = get_combined_ingest_graph(
                feature_infos=feature_infos,
                primary_entity_ids=ingest_graph.primary_entity_ids,
                has_ttl=ingest_graph.has_ttl,
                feature_job_setting=ingest_graph.feature_job_setting,
            )
            entity_universe = await self.offline_store_feature_table_construction_service.get_entity_universe_model(
                offline_ingest_graphs=ingest_graph_metadata.offline_ingest_graphs,
                source_info=feature_store_model.get_source_info(),
                feature_table_name=table_name,
            )
            request_column_names = [
                primary_entity.serving_names[0] for primary_entity in primary_entities
            ]
            graph = ingest_graph_metadata.feature_cluster.graph
            nodes = ingest_graph_metadata.feature_cluster.nodes
            offline_store_feature_table = OfflineStoreFeatureTableModel(
                name=table_name,
                feature_ids=[feature.id for feature in features],
                primary_entity_ids=ingest_graph.primary_entity_ids,
                serving_names=[entity.serving_names[0] for entity in primary_entities],
                feature_cluster=ingest_graph_metadata.feature_cluster,
                output_column_names=ingest_graph_metadata.output_column_names,
                output_dtypes=ingest_graph_metadata.output_dtypes,
                entity_universe=entity_universe.model_dump(by_alias=True),
                has_ttl=ingest_graph.has_ttl,
                feature_job_setting=ingest_graph.feature_job_setting,
                aggregation_ids=ingest_graph_metadata.aggregation_ids,
            )
            parent_serving_preparation = await self.entity_validation_service.validate_entities_or_prepare_for_parent_serving(
                graph_nodes=(graph, nodes),
                feature_list_model=None,
                offline_store_feature_table_model=offline_store_feature_table,
                request_column_names=set(request_column_names),
                feature_store=feature_store_model,
            )
            request_table_expr = entity_universe.get_entity_universe_expr(
                current_feature_timestamp=point_in_time_placeholder,
                last_materialized_timestamp=None,
            )
            partition_column_filters = get_partition_filters_from_graph(
                query_graph=graph,
                min_point_in_time=point_in_time_placeholder,
                max_point_in_time=point_in_time_placeholder,
                adapter=get_sql_adapter(feature_store_model.get_source_info()),
            )
            column_statistics_info = (
                await self.column_statistics_service.get_column_statistics_info()
            )
            feature_query_plan = get_deployment_feature_query_plan(
                graph=graph,
                nodes=nodes,
                source_info=feature_store_model.get_source_info(),
                point_in_time_placeholder=point_in_time_placeholder,
                request_table_columns=request_column_names,
                request_table_expr=request_table_expr,
                parent_serving_preparation=parent_serving_preparation,
                partition_column_filters=partition_column_filters,
                column_statistics_info=column_statistics_info,
            )
            feature_query_expr = feature_query_plan.get_standalone_expr()

            # Perform an additional join if the features need to be served using child entities
            if lookup_entity_universe_expr is not None:
                assert lookup_info is not None
                serving_name_exprs = {}
                join_conditions = []
                for source_entity_id in ingest_graph.primary_entity_ids:
                    source_serving_name = entity_mapping[source_entity_id].serving_names[0]
                    lookup_serving_name = lookup_info.get_lookup_feature_table_serving_name(
                        source_serving_name
                    )
                    if lookup_serving_name is None:
                        serving_name_exprs[source_serving_name] = get_qualified_column_identifier(
                            source_serving_name, "R"
                        )
                    else:
                        serving_name_exprs[lookup_serving_name] = get_qualified_column_identifier(
                            lookup_serving_name, "L"
                        )
                        join_conditions.append(
                            expressions.EQ(
                                this=get_qualified_column_identifier(source_serving_name, "L"),
                                expression=get_qualified_column_identifier(
                                    source_serving_name, "R"
                                ),
                            )
                        )

                assert join_conditions, (
                    "No join conditions found for parent serving. "
                    "All source entities have lookup_serving_name = None."
                )

                column_names = [ingest_graph.output_column_name for ingest_graph in ingest_graphs]
                feature_query_expr = (
                    expressions.select(
                        *(
                            list(serving_name_exprs.values())
                            + [
                                get_qualified_column_identifier(column_name, "R")
                                for column_name in column_names
                            ]
                        )
                    )
                    .from_(
                        lookup_entity_universe_expr.subquery(alias="L"),
                    )
                    .join(
                        feature_query_expr.subquery(alias="R"),
                        join_type="left",
                        on=expressions.and_(*join_conditions),
                    )
                )

            feature_query_expr = feature_query_expr.select(
                expressions.alias_(
                    point_in_time_placeholder,
                    alias=SpecialColumnName.POINT_IN_TIME,
                    quoted=True,
                )
            )
            feature_query_str = sql_to_string(
                feature_query_expr, source_type=feature_store_model.type
            )
            feature_table_sqls.append(
                FeatureTableSql(
                    feature_ids=[feature.id for feature in features],
                    feature_names=[feature.name for feature in features],
                    sql_code=feature_query_str,
                )
            )
        deployment_sql_model = DeploymentSqlModel(
            _id=output_document_id,
            deployment_id=deployment_id,
            feature_table_sqls=feature_table_sqls,
            catalog_id=catalog_model.id,
        )
        return deployment_sql_model

    async def _get_entities(
        self, entity_mapping: dict[ObjectId, EntityModel], entity_ids: list[ObjectId]
    ) -> list[EntityModel]:
        entities = []
        for entity_id in entity_ids:
            if entity_id not in entity_mapping:
                entity_model = await self.entity_service.get_document(entity_id)
                entity_mapping[entity_id] = entity_model
            entities.append(entity_mapping[entity_id])
        return entities

    async def _retrieve_all_entities(self, entity_mapping: dict[ObjectId, EntityModel]) -> None:
        async for entity_model in self.entity_service.list_documents_iterator(query_filter={}):
            entity_mapping[entity_model.id] = entity_model
