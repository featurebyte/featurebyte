"""
FeatureMaterializeService class
"""
from __future__ import annotations

from typing import List, Optional, Tuple

from dataclasses import dataclass, field

from bson import ObjectId
from sqlglot import expressions
from sqlglot.expressions import Select, select

from featurebyte.enum import DBVarType
from featurebyte.models.entity import EntityModel
from featurebyte.models.feature import FeatureModel
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.common import quoted_identifier, sql_to_string
from featurebyte.query_graph.sql.online_serving import (
    TemporaryBatchRequestTable,
    get_online_features,
)
from featurebyte.service.entity import EntityService
from featurebyte.service.feature import FeatureService
from featurebyte.service.online_store_compute_query_service import OnlineStoreComputeQueryService
from featurebyte.service.online_store_table_version import OnlineStoreTableVersionService
from featurebyte.session.base import BaseSession


@dataclass
class OfflineIngestGraphMetadata:
    """
    Information about offline ingest graph combined for all features
    """

    graph: QueryGraph
    node_names: List[str]
    output_column_names: List[str]
    output_dtypes: List[DBVarType]


@dataclass
class OnlineEnabledFeaturesMetadata:
    """
    Information about online enabled features having the same primary entity that should be
    published together
    """

    features: List[FeatureModel]
    online_store_table_names: List[str]
    primary_entities: List[EntityModel]
    ingest_graph_and_node_names: Tuple[QueryGraph, List[str]] = field(init=False)
    output_column_names: List[str] = field(init=False)
    output_dtypes: List[DBVarType] = field(init=False)

    def __post_init__(self) -> None:
        ingest_graph_metadata = self._get_combined_ingest_graph()
        self.ingest_graph_and_node_names = (
            ingest_graph_metadata.graph,
            ingest_graph_metadata.node_names,
        )
        self.output_column_names = ingest_graph_metadata.output_column_names
        self.output_dtypes = ingest_graph_metadata.output_dtypes

    @property
    def serving_names(self) -> List[str]:
        """
        Returns serving names for the primary entities

        Returns
        -------
        List[str]
        """
        return sorted([entity.serving_names[0] for entity in self.primary_entities])

    @property
    def primary_entity_ids(self) -> List[ObjectId]:
        """
        Returns primary entity ids

        Returns
        -------
        List[ObjectId]
        """
        return sorted([entity.id for entity in self.primary_entities])

    @property
    def entity_universe_expr(self) -> Optional[Select]:
        """
        Returns a SQL expression that selects the distinct entity serving names across all online
        store tables

        Returns
        -------
        Optional[Select]
        """
        if not self.online_store_table_names:
            return None

        # select distinct serving names across all online store tables
        distinct_serving_names_from_tables = [
            select(*[quoted_identifier(serving_name) for serving_name in self.serving_names])
            .distinct()
            .from_(table_name)
            for table_name in self.online_store_table_names
        ]

        union_expr = distinct_serving_names_from_tables[0]
        for expr in distinct_serving_names_from_tables[1:]:
            union_expr = expressions.Union(this=expr, distinct=True, expression=union_expr)  # type: ignore

        return union_expr

    def _get_combined_ingest_graph(self) -> OfflineIngestGraphMetadata:
        """
        Returns a combined ingest graph and related information for all features

        Returns
        -------
        OfflineIngestGraphMetadata
        """
        local_query_graph = QueryGraph()
        output_nodes = []
        output_column_names = []
        output_dtypes = []

        for feature in self.features:
            offline_ingest_graphs = feature.extract_offline_store_ingest_query_graphs()
            for offline_ingest_graph in offline_ingest_graphs:
                if offline_ingest_graph.primary_entity_ids != self.primary_entity_ids:
                    # Feature of a primary entity can be decomposed into ingest graphs with
                    # different primary entity. Skip if it is not the same as the primary_entity_ids
                    # of interest now.
                    continue
                graph, node = offline_ingest_graph.ingest_graph_and_node()
                local_query_graph, local_name_map = local_query_graph.load(graph)
                output_nodes.append(local_query_graph.get_node_by_name(local_name_map[node.name]))
                output_column_names.append(offline_ingest_graph.output_column_name)
                output_dtypes.append(offline_ingest_graph.output_dtype)

        return OfflineIngestGraphMetadata(
            graph=local_query_graph,
            node_names=[node.name for node in output_nodes],
            output_column_names=output_column_names,
            output_dtypes=output_dtypes,
        )


@dataclass
class MaterializedFeatures:
    """
    Information about materialised features ready to be published
    """

    materialized_table_name: str
    names: List[str]
    data_types: List[str]
    serving_names: List[str]


class FeatureMaterializeService:
    """
    FeatureMaterializeService is responsible for materialising a set of currently online enabled
    features so that they can be published to an external feature store. These features are
    materialised into a new table in the data warehouse.
    """

    def __init__(
        self,
        feature_service: FeatureService,
        online_store_compute_query_service: OnlineStoreComputeQueryService,
        entity_service: EntityService,
        online_store_table_version_service: OnlineStoreTableVersionService,
    ):
        self.feature_service = feature_service
        self.online_store_compute_query_service = online_store_compute_query_service
        self.entity_service = entity_service
        self.online_store_table_version_service = online_store_table_version_service

    async def materialize_features(
        self,
        session: BaseSession,
        primary_entity_ids: List[ObjectId],
        feature_job_setting: FeatureJobSetting,
    ) -> Optional[MaterializedFeatures]:
        """
        Materialise currently online enabled features matching the given primary entity and feature
        job settings into a new table in the data warehouse.

        Parameters
        ----------
        session: BaseSession
            Session object
        primary_entity_ids: List[ObjectId]
            List of primary entity ids
        feature_job_setting: FeatureJobSetting
            Feature job setting

        Returns
        -------
        Optional[MaterializedFeatures]
        """
        # Collect list of features to be published
        online_enabled_features_metadata = await self._get_online_enabled_features_metadata(
            primary_entity_ids, feature_job_setting
        )
        if online_enabled_features_metadata.entity_universe_expr is None:
            # TODO: handle feature types that are currently not being precomputed, e.g. lookup
            #  features, asat aggregates, etc.
            return None

        # Create temporary batch request table with the universe of entities
        unique_id = ObjectId()
        batch_request_table = TemporaryBatchRequestTable(
            table_details=TableDetails(
                database_name=session.database_name,
                schema_name=session.schema_name,
                table_name=f"TEMP_REQUEST_TABLE_{unique_id}".upper(),
            ),
            column_names=online_enabled_features_metadata.serving_names,
        )
        adapter = get_sql_adapter(session.source_type)
        create_batch_request_table_query = sql_to_string(
            adapter.create_table_as(
                table_details=batch_request_table.table_details,
                select_expr=online_enabled_features_metadata.entity_universe_expr,
            ),
            source_type=session.source_type,
        )
        await session.execute_query_long_running(create_batch_request_table_query)

        # Materialize features
        output_table_details = TableDetails(
            database_name=session.database_name,
            schema_name=session.schema_name,
            table_name=f"TEMP_FEATURE_TABLE_{unique_id}".upper(),
        )
        graph, node_names = online_enabled_features_metadata.ingest_graph_and_node_names
        try:
            await get_online_features(
                session=session,
                graph=graph,
                nodes=[graph.get_node_by_name(node_name) for node_name in node_names],
                request_data=batch_request_table,
                source_type=session.source_type,
                online_store_table_version_service=self.online_store_table_version_service,
                output_table_details=output_table_details,
            )
        finally:
            # Delete temporary batch request table
            await session.drop_table(
                table_name=batch_request_table.table_details.table_name,
                schema_name=batch_request_table.table_details.schema_name,  # type: ignore
                database_name=batch_request_table.table_details.database_name,  # type: ignore
                if_exists=True,
            )

        return MaterializedFeatures(
            materialized_table_name=output_table_details.table_name,
            names=online_enabled_features_metadata.output_column_names,
            data_types=[
                adapter.get_physical_type_from_dtype(dtype)
                for dtype in online_enabled_features_metadata.output_dtypes
            ],
            serving_names=online_enabled_features_metadata.serving_names,
        )

    async def _get_online_enabled_features_metadata(
        self,
        primary_entity_ids: List[ObjectId],
        feature_job_setting: FeatureJobSetting,
    ) -> OnlineEnabledFeaturesMetadata:
        """
        Get current online enabled features corresponding to the primary entity ids

        Parameters
        ----------
        primary_entity_ids: List[ObjectId]
            List of primary entity ids
        feature_job_setting: FeatureJobSetting
            Feature job setting

        Returns
        -------
        OnlineEnabledFeaturesMetadata
        """
        online_store_table_names = set()
        online_enabled_features = []

        # Note: Use of "$in" for primary_entity_ids field is intentional. For example, if a feature
        # has primary entity of CUSTOMER X PRODUCT. It could be a CUSTOMER feature combined with
        # PRODUCT feature, or a CUSTOMER X PRODUCT feature (via composite groupby keys). For the
        # former case, the feature will be decomposed into two parts - one for CUSTOMER and one for
        # PRODUCT. One of the parts need to be included in the CUSTOMER offline store table.
        async for feature_model in self.feature_service.list_documents_iterator(
            query_filter={
                "online_enabled": True,
                "primary_entity_ids": {"$in": primary_entity_ids},
            }
        ):
            # TODO: investigate why filtering directly by
            #  table_id_feature_job_settings.feature_job_setting doesn't work
            for table_id_feature_job_setting in feature_model.table_id_feature_job_settings:
                if table_id_feature_job_setting.feature_job_setting == feature_job_setting:
                    online_enabled_features.append(feature_model)
                    online_store_table_names.update(feature_model.online_store_table_names)
                    break

        online_store_table_names = sorted(online_store_table_names)  # type: ignore[assignment]

        # Get online tables
        online_store_table_name_to_serving_names = {}
        async for online_store_compute_query_model in self.online_store_compute_query_service.list_documents_iterator(
            query_filter={"table_name": {"$in": online_store_table_names}}
        ):
            online_store_table_name_to_serving_names[
                online_store_compute_query_model.table_name
            ] = online_store_compute_query_model.serving_names

        primary_entities = []
        async for entity_model in self.entity_service.list_documents_iterator(
            query_filter={"_id": {"$in": primary_entity_ids}}
        ):
            primary_entities.append(entity_model)

        primary_entity_serving_names = sorted(
            [entity.serving_names[0] for entity in primary_entities]
        )
        required_online_store_tables = []
        for (
            online_store_table_name,
            online_store_serving_names,
        ) in online_store_table_name_to_serving_names.items():
            if sorted(online_store_serving_names) == primary_entity_serving_names:
                required_online_store_tables.append(online_store_table_name)

        return OnlineEnabledFeaturesMetadata(
            features=online_enabled_features,
            online_store_table_names=required_online_store_tables,
            primary_entities=primary_entities,
        )
