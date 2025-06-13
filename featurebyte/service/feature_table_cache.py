"""
Module for managing physical feature table cache as well as metadata storage.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, AsyncGenerator, Callable, Coroutine, Dict, List, Optional, Tuple, cast

import pandas as pd
from bson import ObjectId
from pyarrow import RecordBatch
from redis import Redis
from sqlglot import expressions

from featurebyte.common.progress import get_ranged_progress_callback
from featurebyte.common.utils import timer
from featurebyte.enum import InternalName, MaterializedTableNamePrefix
from featurebyte.logging import get_logger
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.feature_table_cache_metadata import (
    CachedDefinitionWithTable,
    CachedFeatureDefinition,
    FeatureTableCacheMetadataModel,
)
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.models.system_metrics import HistoricalFeaturesMetrics
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.common import (
    get_qualified_column_identifier,
    quoted_identifier,
    sql_to_string,
)
from featurebyte.query_graph.sql.materialisation import get_source_expr
from featurebyte.query_graph.transform.definition import DefinitionHashExtractor
from featurebyte.query_graph.transform.offline_store_ingest import extract_dtype_info_from_graph
from featurebyte.service.column_statistics import ColumnStatisticsService
from featurebyte.service.cron_helper import CronHelper
from featurebyte.service.entity_validation import EntityValidationService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_table_cache_metadata import FeatureTableCacheMetadataService
from featurebyte.service.historical_features_and_target import (
    FeaturesComputationResult,
    get_historical_features,
    get_target,
)
from featurebyte.service.namespace_handler import NamespaceHandler
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.system_metrics import SystemMetricsService
from featurebyte.service.tile_cache import TileCacheService
from featurebyte.service.warehouse_table_service import WarehouseTableService
from featurebyte.session.base import LONG_RUNNING_EXECUTE_QUERY_TIMEOUT_SECONDS, BaseSession
from featurebyte.utils.redis import acquire_lock

FEATURE_TABLE_CACHE_CHECK_PROGRESS_PERCENTAGE = 10

logger = get_logger(__name__)


@dataclass
class UpdateFeatureTableCacheResult:
    """
    Result of updating feature table cache
    """

    hashes: List[str]
    session: BaseSession
    features_computation_result: FeaturesComputationResult

    @property
    def historical_features_metrics(self) -> HistoricalFeaturesMetrics:
        """
        Get historical features metrics

        Returns
        -------
        HistoricalFeaturesMetrics
        """
        return self.features_computation_result.historical_features_metrics


class FeatureTableCacheService:
    """
    Service for managing physical feature table cache as well as metadata storage.
    """

    def __init__(
        self,
        feature_table_cache_metadata_service: FeatureTableCacheMetadataService,
        namespace_handler: NamespaceHandler,
        session_manager_service: SessionManagerService,
        entity_validation_service: EntityValidationService,
        tile_cache_service: TileCacheService,
        feature_list_service: FeatureListService,
        warehouse_table_service: WarehouseTableService,
        cron_helper: CronHelper,
        column_statistics_service: ColumnStatisticsService,
        system_metrics_service: SystemMetricsService,
        redis: Redis[Any],
    ):
        self.feature_table_cache_metadata_service = feature_table_cache_metadata_service
        self.namespace_handler = namespace_handler
        self.session_manager_service = session_manager_service
        self.entity_validation_service = entity_validation_service
        self.tile_cache_service = tile_cache_service
        self.feature_list_service = feature_list_service
        self.warehouse_table_service = warehouse_table_service
        self.cron_helper = cron_helper
        self.column_statistics_service = column_statistics_service
        self.system_metrics_service = system_metrics_service
        self.redis = redis

    async def definition_hashes_for_nodes(
        self,
        graph: QueryGraph,
        nodes: List[Node],
    ) -> List[str]:
        """
        Compute definition hashes for list of nodes

        Parameters
        ----------
        graph: QueryGraph
            Graph definition
        nodes: List[Node]
            Input node names

        Returns
        -------
        List[str]
            List of definition hashes corresponding to nodes
        """
        node_names = [node.name for node in nodes]
        pruned_graph, node_name_map = graph.quick_prune(target_node_names=node_names)

        hashes = []
        for node in nodes:
            (
                prepared_graph,
                prepared_node_name,
            ) = await self.namespace_handler.prepare_graph_to_store(
                graph=pruned_graph,
                node=pruned_graph.get_node_by_name(node_name_map[node.name]),
                sanitize_for_definition=True,
            )
            definition_hash_extractor = DefinitionHashExtractor(graph=prepared_graph)
            definition_hash = definition_hash_extractor.extract(
                prepared_graph.get_node_by_name(prepared_node_name)
            ).definition_hash
            hashes.append(definition_hash)
        return hashes

    async def _get_definition_hashes_mapping_from_feature_list_id(
        self, feature_list_id: ObjectId
    ) -> Dict[str, str]:
        definition_hashes_mapping = {}
        feature_list = await self.feature_list_service.get_document(feature_list_id)
        if feature_list.feature_clusters is not None:
            stored_hashes = feature_list.feature_clusters[0].feature_node_definition_hashes
            if stored_hashes is not None:
                for info in stored_hashes:
                    if info.definition_hash is not None:
                        definition_hashes_mapping[info.node_name] = info.definition_hash
        return definition_hashes_mapping

    async def get_feature_definition_hashes(
        self,
        graph: QueryGraph,
        nodes: List[Node],
        definition_hashes_mapping: Optional[Dict[str, str]] = None,
    ) -> List[str]:
        """
        Get definition hashes for list of nodes. Retrieve the result from feature list if available.

        Parameters
        ----------
        graph: QueryGraph
            Query graph
        nodes: List[Node]
            Nodes
        definition_hashes_mapping: Optional[Dict[str, str]]
            Optional definition hashes. If specified, the hashes will be used instead of being
            computed from the graph

        Returns
        -------
        List[str]
        """
        with timer("get_feature_definition_hashes", logger):
            definition_hashes_mapping = definition_hashes_mapping or {}

            # Fallback to deriving the hashes from scratch
            missing_nodes = [node for node in nodes if node.name not in definition_hashes_mapping]
            if missing_nodes:
                missing_definition_hashes = await self.definition_hashes_for_nodes(
                    graph, missing_nodes
                )
                for node, definition_hash in zip(missing_nodes, missing_definition_hashes):
                    definition_hashes_mapping[node.name] = definition_hash

            return [definition_hashes_mapping[node.name] for node in nodes]

    async def get_feature_query(
        self,
        db_session: BaseSession,
        observation_table: ObservationTableModel,
        hashes: List[str],
        output_column_names: List[str],
        additional_columns: List[str],
        sample_size: Optional[int] = None,
        seed: int = 42,
    ) -> expressions.Select:
        """
        Get sql query to retrieve cached feature values from feature cache table(s)

        Parameters
        ----------
        db_session: BaseSession
            Database session
        observation_table: ObservationTableModel
            Observation table
        hashes: List[str]
            Definition hashes corresponding to the list of nodes
        output_column_names: List[str]
            Output column names corresponding to the list of hashes
        additional_columns: List[str]
            Additional columns to include in the select query
        sample_size: Optional[int]
            Optional sample size to read from the cache table
        seed: int
            Seed for random sampling

        Returns
        -------
        expressions.Select
        """
        # Retrieve cached definitions
        hashes_set = set(hashes)
        cached_definitions = await self.feature_table_cache_metadata_service.get_cached_definitions(
            observation_table_id=observation_table.id
        )
        cached_definition_hash_mapping = {}
        for definition in cached_definitions:
            assert definition.feature_name is not None
            if definition.definition_hash in hashes_set:
                cached_definition_hash_mapping[definition.definition_hash] = (
                    definition.table_name,
                    definition.feature_name,
                )

        # Get the required cache tables
        required_cache_tables = set()
        for table_name, feature_name in cached_definition_hash_mapping.values():
            required_cache_tables.add(table_name)

        # Join necessary feature cache tables based on the definition hashes
        select_expr = expressions.select()
        non_feature_columns = [InternalName.TABLE_ROW_INDEX] + additional_columns

        table_name_to_alias = {}
        for i, table_name in enumerate(sorted(required_cache_tables)):
            table_alias = f"T{i}"
            table_name_to_alias[table_name] = table_alias
            if i == 0:
                # Add random sampling if sample_size is specified
                if sample_size is not None:
                    feature_exprs = []
                    for output_feature_name, definition_hash in zip(output_column_names, hashes):
                        _table_name, column_name = cached_definition_hash_mapping[definition_hash]
                        if _table_name == table_name:
                            feature_exprs.append(quoted_identifier(column_name))

                    select_expr = select_expr.from_(
                        expressions.Table(this=quoted_identifier(table_name))
                    )
                    select_expr = select_expr.select(
                        *[quoted_identifier(column_name) for column_name in non_feature_columns],
                        *feature_exprs,
                    )
                    select_expr = db_session.adapter.random_sample(
                        select_expr,
                        desired_row_count=sample_size,
                        total_row_count=observation_table.num_rows,
                        seed=seed,
                    )
                    select_expr = select_expr.limit(sample_size)
                    select_expr = expressions.select().from_(
                        select_expr.subquery(
                            alias=expressions.TableAlias(
                                this=expressions.Identifier(this=table_alias)
                            ),
                        )
                    )
                else:
                    select_expr = select_expr.from_(
                        expressions.Table(
                            this=quoted_identifier(table_name),
                            alias=expressions.TableAlias(
                                this=expressions.Identifier(this=table_alias)
                            ),
                        ),
                    )
            else:
                select_expr = select_expr.join(
                    expressions.Table(
                        this=quoted_identifier(table_name),
                        alias=expressions.TableAlias(this=expressions.Identifier(this=table_alias)),
                    ),
                    join_type="left",
                    on=expressions.EQ(
                        this=get_qualified_column_identifier(InternalName.TABLE_ROW_INDEX, "T0"),
                        expression=get_qualified_column_identifier(
                            InternalName.TABLE_ROW_INDEX, table_alias
                        ),
                    ),
                )

        # Add feature columns to the select expression
        feature_exprs = []
        for output_feature_name, definition_hash in zip(output_column_names, hashes):
            table_name, column_name = cached_definition_hash_mapping[definition_hash]
            table_alias = table_name_to_alias[table_name]
            feature_exprs.append(
                expressions.alias_(
                    get_qualified_column_identifier(column_name, table_alias),
                    alias=output_feature_name,
                    quoted=True,
                )
            )
        select_expr = select_expr.select(
            *[
                get_qualified_column_identifier(column_name, "T0")
                for column_name in non_feature_columns
            ],
            *feature_exprs,
        ).order_by(
            expressions.Order(
                expressions=[
                    expressions.Ordered(
                        this=quoted_identifier(InternalName.TABLE_ROW_INDEX), desc=False
                    )
                ]
            )
        )
        return select_expr

    @staticmethod
    async def get_non_cached_nodes(
        cached_definitions: List[CachedDefinitionWithTable],
        nodes: List[Node],
        hashes: List[str],
    ) -> List[Tuple[Node, CachedFeatureDefinition]]:
        """
        Given an observation table, graph and set of nodes
            - compute nodes definition hashes
            - lookup existing Feature Table Cache metadata
            - filter out nodes which are already added to the Feature Table Cache
            - return cached and non-cached nodes and their definition hashes.

        Parameters
        ----------
        cached_definitions: List[CachedDefinitionWithTable]
            List of cached feature definitions
        nodes: List[Node]
            Input node names
        hashes: List[str]
            Definition hashes corresponding to the list of nodes

        Returns
        -------
        List[Tuple[Node, CachedFeatureDefinition]]
            List of non cached nodes and respective newly-created cached feature definitions
        """
        cached_hashes = {feat.definition_hash: feat for feat in cached_definitions}
        added_hashes = set()
        non_cached_nodes = []
        for definition_hash, node in zip(hashes, nodes):
            if definition_hash in cached_hashes or definition_hash in added_hashes:
                continue
            added_hashes.add(definition_hash)
            non_cached_nodes.append((
                node,
                CachedFeatureDefinition(definition_hash=definition_hash),
            ))
        return non_cached_nodes

    async def _populate_intermediate_table(
        self,
        feature_store: FeatureStoreModel,
        observation_table: ObservationTableModel,
        db_session: BaseSession,
        intermediate_table_name: str,
        graph: QueryGraph,
        nodes: List[Tuple[Node, CachedFeatureDefinition]],
        raise_on_error: bool,
        is_target: bool = False,
        serving_names_mapping: Optional[Dict[str, str]] = None,
        progress_callback: Optional[
            Callable[[int, Optional[str]], Coroutine[Any, Any, None]]
        ] = None,
    ) -> FeaturesComputationResult:
        request_column_names = {col.name for col in observation_table.columns_info}
        nodes_only = [node for node, _ in nodes]
        parent_serving_preparation = (
            await self.entity_validation_service.validate_entities_or_prepare_for_parent_serving(
                graph_nodes=(graph, nodes_only),
                request_column_names=request_column_names,
                feature_store=feature_store,
                serving_names_mapping=serving_names_mapping,
            )
        )
        output_table_details = TableDetails(
            database_name=db_session.database_name,
            schema_name=db_session.schema_name,
            table_name=intermediate_table_name,
        )
        if is_target:
            return await get_target(
                session=db_session,
                redis=self.redis,
                graph=graph,
                nodes=nodes_only,
                observation_set=observation_table,
                feature_store=feature_store,
                output_table_details=output_table_details,
                serving_names_mapping=serving_names_mapping,
                parent_serving_preparation=parent_serving_preparation,
                progress_callback=progress_callback,
                system_metrics_service=self.system_metrics_service,
            )
        else:
            return await get_historical_features(
                session=db_session,
                tile_cache_service=self.tile_cache_service,
                warehouse_table_service=self.warehouse_table_service,
                cron_helper=self.cron_helper,
                column_statistics_service=self.column_statistics_service,
                system_metrics_service=self.system_metrics_service,
                graph=graph,
                nodes=nodes_only,
                observation_set=observation_table,
                feature_store=feature_store,
                output_table_details=output_table_details,
                serving_names_mapping=serving_names_mapping,
                parent_serving_preparation=parent_serving_preparation,
                progress_callback=progress_callback,
                raise_on_error=raise_on_error,
            )

    @classmethod
    async def _ensure_feature_cache_table_columns(
        cls,
        db_session: BaseSession,
        cache_metadata: FeatureTableCacheMetadataModel,
        graph: QueryGraph,
        non_cached_nodes: List[Tuple[Node, CachedFeatureDefinition]],
    ) -> None:
        # Only insert non-existing columns to the table
        adapter = db_session.adapter
        table_expr = expressions.Table(
            this=quoted_identifier(cache_metadata.table_name),
            db=quoted_identifier(db_session.schema_name),
            catalog=quoted_identifier(db_session.database_name),
        )
        existing_columns = set(
            col.upper()
            for col in (
                await db_session.list_table_schema(
                    table_name=cache_metadata.table_name,
                    database_name=db_session.database_name,
                    schema_name=db_session.schema_name,
                    timeout=LONG_RUNNING_EXECUTE_QUERY_TIMEOUT_SECONDS,
                )
            ).keys()
        )
        columns_expr = [
            expressions.ColumnDef(
                this=quoted_identifier(definition.feature_name),
                kind=adapter.get_physical_type_from_dtype(
                    extract_dtype_info_from_graph(graph, node).dtype
                ),
            )
            for node, definition in non_cached_nodes
            if definition.feature_name is not None
            and definition.feature_name.upper() not in existing_columns
        ]
        if not columns_expr:
            return
        # While "column already exist" error is not expected, still retry to handle other errors
        # such as rate limiting (occurring in tests for BigQuery)
        await db_session.retry_sql(
            adapter.alter_table_add_columns(table_expr, columns_expr),
        )

    async def _insert_features_into_cache(
        self,
        db_session: BaseSession,
        observation_table: ObservationTableModel,
        non_cached_nodes: List[Tuple[Node, CachedFeatureDefinition]],
        graph: QueryGraph,
        intermediate_table_name: str,
        features_computation_result: FeaturesComputationResult,
    ) -> None:
        # Skip inserting features if there are any failed nodes
        failed_node_names_set = set(features_computation_result.failed_node_names)
        non_cached_nodes = [
            node_and_def
            for node_and_def in non_cached_nodes
            if node_and_def[0].name not in failed_node_names_set
        ]
        if not non_cached_nodes:
            # No features to insert
            return

        # Get the feature table cache to insert to
        cache_metadata = (
            await self.feature_table_cache_metadata_service.get_or_create_feature_table_cache(
                observation_table_id=observation_table.id,
                num_columns_to_insert=len(non_cached_nodes),
                session=db_session,
            )
        )
        feature_table_cache_exists = await self._feature_table_cache_exists(
            cache_metadata, db_session
        )
        if not feature_table_cache_exists:
            # If cache table doesn't exist yet, create one by cloning from the observation table
            request_column_names = [col.name for col in observation_table.columns_info]
            await db_session.create_table_as(
                table_details=TableDetails(
                    database_name=db_session.database_name,
                    schema_name=db_session.schema_name,
                    table_name=cache_metadata.table_name,
                ),
                select_expr=get_source_expr(
                    observation_table.location.table_details,
                    column_names=[InternalName.TABLE_ROW_INDEX.value] + request_column_names,
                ),
                exists=True,
                retry=True,
            )

        # alter cached tables adding columns for new features
        await self._ensure_feature_cache_table_columns(
            db_session=db_session,
            cache_metadata=cache_metadata,
            graph=graph,
            non_cached_nodes=non_cached_nodes,
        )

        # merge temp table into cache table
        merge_target_table_alias = "feature_table_cache"
        merge_source_table_alias = "partial_features"

        tic = time.time()
        merge_conditions = [
            expressions.EQ(
                this=get_qualified_column_identifier(
                    InternalName.TABLE_ROW_INDEX, merge_target_table_alias
                ),
                expression=get_qualified_column_identifier(
                    InternalName.TABLE_ROW_INDEX, merge_source_table_alias
                ),
            )
        ]
        update_expr = expressions.Update(
            expressions=[
                expressions.EQ(
                    this=get_qualified_column_identifier(
                        cast(str, definition.feature_name), merge_target_table_alias
                    ),
                    expression=get_qualified_column_identifier(
                        cast(str, graph.get_node_output_column_name(node.name)),
                        merge_source_table_alias,
                    ),
                )
                for node, definition in non_cached_nodes
            ]
        )
        merge_expr = expressions.Merge(
            this=expressions.Table(
                this=quoted_identifier(cache_metadata.table_name),
                db=quoted_identifier(db_session.schema_name),
                catalog=quoted_identifier(db_session.database_name),
                alias=expressions.TableAlias(this=merge_target_table_alias),
            ),
            using=expressions.Table(
                this=quoted_identifier(intermediate_table_name),
                db=quoted_identifier(db_session.schema_name),
                catalog=quoted_identifier(db_session.database_name),
                alias=expressions.TableAlias(this=merge_source_table_alias),
            ),
            on=expressions.and_(*merge_conditions),
            expressions=[expressions.When(matched=True, then=update_expr)],
        )
        await db_session.retry_sql(sql_to_string(merge_expr, source_type=db_session.source_type))
        metrics = features_computation_result.historical_features_metrics
        metrics.feature_cache_update_seconds = time.time() - tic

        # Update feature table cache metadata
        await self.feature_table_cache_metadata_service.update_feature_table_cache(
            cache_metadata_id=cache_metadata.id,
            feature_definitions=[definition for _, definition in non_cached_nodes],
        )

    async def _materialize_and_update_cache(  # pylint: disable=too-many-arguments
        self,
        feature_store: FeatureStoreModel,
        observation_table: ObservationTableModel,
        db_session: BaseSession,
        graph: QueryGraph,
        non_cached_nodes: List[Tuple[Node, CachedFeatureDefinition]],
        raise_on_error: bool,
        is_target: bool = False,
        serving_names_mapping: Optional[Dict[str, str]] = None,
        progress_callback: Optional[
            Callable[[int, Optional[str]], Coroutine[Any, Any, None]]
        ] = None,
    ) -> FeaturesComputationResult:
        # create temporary table with features
        intermediate_table_name = (
            f"__TEMP__{MaterializedTableNamePrefix.FEATURE_TABLE_CACHE}_{ObjectId()}"
        )

        try:
            features_computation_result = await self._populate_intermediate_table(
                feature_store=feature_store,
                observation_table=observation_table,
                db_session=db_session,
                intermediate_table_name=intermediate_table_name,
                graph=graph,
                nodes=non_cached_nodes,
                is_target=is_target,
                serving_names_mapping=serving_names_mapping,
                progress_callback=progress_callback,
                raise_on_error=raise_on_error,
            )
            async with acquire_lock(
                self.redis,
                f"feature_table_cache_update:obs:{observation_table.id}",
                timeout=LONG_RUNNING_EXECUTE_QUERY_TIMEOUT_SECONDS,
            ):
                # We need to acquire a lock to handle the case where multiple requests are trying to
                # add the features to the cache at the same time.
                await self._insert_features_into_cache(
                    db_session=db_session,
                    observation_table=observation_table,
                    non_cached_nodes=non_cached_nodes,
                    graph=graph,
                    intermediate_table_name=intermediate_table_name,
                    features_computation_result=features_computation_result,
                )
        finally:
            await db_session.drop_table(
                database_name=db_session.database_name,
                schema_name=db_session.schema_name,
                table_name=intermediate_table_name,
                if_exists=True,
            )
        return features_computation_result

    @staticmethod
    async def _feature_table_cache_exists(
        cache_metadata: FeatureTableCacheMetadataModel, session: BaseSession
    ) -> bool:
        try:
            query = sql_to_string(
                expressions.select(expressions.Count(this=expressions.Star()))
                .from_(quoted_identifier(cache_metadata.table_name))
                .limit(1),
                source_type=session.source_type,
            )
            _ = await session.execute_query_long_running(query)
            return True
        except session._no_schema_error:
            return False

    async def create_or_update_feature_table_cache(
        self,
        feature_store: FeatureStoreModel,
        observation_table: ObservationTableModel,
        graph: QueryGraph,
        nodes: List[Node],
        definition_hashes_mapping: Optional[Dict[str, str]] = None,
        is_target: bool = False,
        feature_list_id: Optional[PydanticObjectId] = None,
        serving_names_mapping: Optional[Dict[str, str]] = None,
        progress_callback: Optional[
            Callable[[int, Optional[str]], Coroutine[Any, Any, None]]
        ] = None,
        raise_on_error: bool = True,
    ) -> UpdateFeatureTableCacheResult:
        """
        Create or update feature table cache

        Parameters
        ----------
        feature_store: FeatureStoreModel
            Feature Store object
        observation_table: ObservationTableModel
            Observation table object
        graph: QueryGraph
            Graph definition
        nodes: List[Node]
            Input node names
        definition_hashes_mapping: Optional[Dict[str, str]]
            Optional definition hashes. If specified, the hashes will be used instead of being
            computed from the graph
        is_target : bool
            Whether it is a target computation call
        feature_list_id: Optional[PydanticObjectId]
            Optional feature list id
        serving_names_mapping: Optional[Dict[str, str]]
            Optional serving names mapping if the observations set has different serving name columns
            than those defined in Entities
        progress_callback: Optional[Callable[[int, Optional[str]], Coroutine[Any, Any, None]]]
            Optional progress callback function
        raise_on_error: bool
            Whether to raise an error if the computation fails

        Returns
        -------
        UpdateFeatureTableCacheResult
        """
        if progress_callback:
            await progress_callback(1, "Checking feature table cache status")

        assert (
            observation_table.has_row_index
        ), "Observation Tables without row index are not supported"

        cached_definitions = await self.feature_table_cache_metadata_service.get_cached_definitions(
            observation_table_id=observation_table.id
        )
        db_session = await self.session_manager_service.get_feature_store_session(
            feature_store=feature_store
        )

        if definition_hashes_mapping is None and feature_list_id is not None:
            # Retrieve definition hashes if stored in feature list
            definition_hashes_mapping = (
                await self._get_definition_hashes_mapping_from_feature_list_id(feature_list_id)
            )

        hashes = await self.get_feature_definition_hashes(graph, nodes, definition_hashes_mapping)
        non_cached_nodes = await self.get_non_cached_nodes(cached_definitions, nodes, hashes)

        if progress_callback:
            await progress_callback(
                FEATURE_TABLE_CACHE_CHECK_PROGRESS_PERCENTAGE,
                "Feature table cache status check completed",
            )
            remaining_progress_callback = get_ranged_progress_callback(
                progress_callback, FEATURE_TABLE_CACHE_CHECK_PROGRESS_PERCENTAGE, 100
            )
        else:
            remaining_progress_callback = None

        if non_cached_nodes:
            features_computation_result = await self._materialize_and_update_cache(
                feature_store=feature_store,
                observation_table=observation_table,
                db_session=db_session,
                graph=graph,
                non_cached_nodes=non_cached_nodes,
                is_target=is_target,
                serving_names_mapping=serving_names_mapping,
                progress_callback=remaining_progress_callback,
                raise_on_error=raise_on_error,
            )
        else:
            features_computation_result = FeaturesComputationResult(
                historical_features_metrics=HistoricalFeaturesMetrics(
                    tile_compute_seconds=0,
                    feature_compute_seconds=0,
                    feature_cache_update_seconds=0,
                )
            )

        return UpdateFeatureTableCacheResult(
            hashes=hashes,
            session=db_session,
            features_computation_result=features_computation_result,
        )

    async def _construct_read_from_cache_query(
        self,
        feature_store: FeatureStoreModel,
        observation_table: ObservationTableModel,
        graph: QueryGraph,
        nodes: List[Node],
        definition_hashes_mapping: Optional[Dict[str, str]] = None,
        columns: Optional[List[str]] = None,
        sample_size: Optional[int] = None,
    ) -> tuple[BaseSession, str]:
        """
        Given the graph and set of nodes, get database session and SQL to read respective cached features from feature table cache.

        Parameters
        ----------
        feature_store: FeatureStoreModel
            Feature Store object
        observation_table: ObservationTableModel
            Observation table object
        graph: QueryGraph
            Graph definition
        nodes: List[Node]
            Node names
        definition_hashes_mapping: Optional[Dict[str, str]]
            Optional definition hashes. If specified, the hashes will be used instead of being
            computed from the graph
        columns: Optional[List[str]]
            Optional list of columns to read from the cache table
        sample_size: Optional[int]
            Optional sample size to read from the cache table

        Returns
        -------
        tuple[BaseSession, str]
            Tuple of database session and SQL query string
        """
        hashes = await self.get_feature_definition_hashes(graph, nodes, definition_hashes_mapping)
        db_session = await self.session_manager_service.get_feature_store_session(
            feature_store=feature_store
        )
        select_expr = await self.get_feature_query(
            db_session=db_session,
            observation_table=observation_table,
            hashes=hashes,
            output_column_names=[
                cast(str, graph.get_node_output_column_name(node.name)) for node in nodes
            ],
            additional_columns=columns or [],
            sample_size=sample_size,
        )
        sql = sql_to_string(select_expr, source_type=db_session.source_type)
        return db_session, sql

    async def read_from_cache(
        self,
        feature_store: FeatureStoreModel,
        observation_table: ObservationTableModel,
        graph: QueryGraph,
        nodes: List[Node],
        definition_hashes_mapping: Optional[Dict[str, str]] = None,
        columns: Optional[List[str]] = None,
        sample_size: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Given the graph and set of nodes, read respective cached features from feature table cache.

        Parameters
        ----------
        feature_store: FeatureStoreModel
            Feature Store object
        observation_table: ObservationTableModel
            Observation table object
        graph: QueryGraph
            Graph definition
        nodes: List[Node]
            Node names
        definition_hashes_mapping: Optional[Dict[str, str]]
            Optional definition hashes. If specified, the hashes will be used instead of being
            computed from the graph
        columns: Optional[List[str]]
            Optional list of columns to read from the cache table
        sample_size: Optional[int]
            Optional sample size to read from the cache table

        Returns
        -------
        pd.DataFrame
            Result data
        """
        db_session, sql = await self._construct_read_from_cache_query(
            feature_store=feature_store,
            observation_table=observation_table,
            graph=graph,
            nodes=nodes,
            definition_hashes_mapping=definition_hashes_mapping,
            columns=columns,
            sample_size=sample_size,
        )
        return await db_session.execute_query_long_running(sql)

    async def stream_from_cache(
        self,
        feature_store: FeatureStoreModel,
        observation_table: ObservationTableModel,
        graph: QueryGraph,
        nodes: List[Node],
        definition_hashes_mapping: Optional[Dict[str, str]] = None,
        columns: Optional[List[str]] = None,
        sample_size: Optional[int] = None,
    ) -> AsyncGenerator[RecordBatch, None]:
        """
        Given the graph and set of nodes, get respective cached features from feature table cache.

        Parameters
        ----------
        feature_store: FeatureStoreModel
            Feature Store object
        observation_table: ObservationTableModel
            Observation table object
        graph: QueryGraph
            Graph definition
        nodes: List[Node]
            Node names
        definition_hashes_mapping: Optional[Dict[str, str]]
            Optional definition hashes. If specified, the hashes will be used instead of being
            computed from the graph
        columns: Optional[List[str]]
            Optional list of columns to read from the cache table
        sample_size: Optional[int]
            Optional sample size to read from the cache table

        Returns
        -------
        AsyncGenerator[RecordBatch, None]
            Result data
        """
        db_session, sql = await self._construct_read_from_cache_query(
            feature_store=feature_store,
            observation_table=observation_table,
            graph=graph,
            nodes=nodes,
            definition_hashes_mapping=definition_hashes_mapping,
            columns=columns,
            sample_size=sample_size,
        )
        return db_session.get_async_query_generator(sql)

    async def create_view_or_table_from_cache(
        self,
        feature_store: FeatureStoreModel,
        observation_table: ObservationTableModel,
        graph: QueryGraph,
        nodes: List[Node],
        output_view_details: TableDetails,
        is_target: bool,
        feature_list_id: Optional[PydanticObjectId] = None,
        serving_names_mapping: Optional[Dict[str, str]] = None,
        progress_callback: Optional[
            Callable[[int, Optional[str]], Coroutine[Any, Any, None]]
        ] = None,
    ) -> Tuple[bool, HistoricalFeaturesMetrics]:
        """
        Create or update cache table and create a new view which refers to the cached table

        Parameters
        ----------
        feature_store: FeatureStoreModel
            Feature Store object
        observation_table: ObservationTableModel
            Observation table object
        graph: QueryGraph
            Graph definition
        nodes: List[Node]
            Input node names
        output_view_details: TableDetails
            Output table details
        is_target : bool
            Whether it is a target computation call
        feature_list_id: Optional[PydanticObjectId]
            Optional feature list id
        serving_names_mapping: Optional[Dict[str, str]]
            Optional serving names mapping if the observations set has different serving name columns
            than those defined in Entities
        progress_callback: Optional[Callable[[int, Optional[str]], Coroutine[Any, Any, None]]]
            Optional progress callback function

        Returns
        -------
        bool
            Whether the output is a view
        """
        first_tic = time.time()
        with timer(
            "Update feature table cache",
            logger,
            extra={"catalog_id": str(observation_table.catalog_id)},
        ):
            update_result = await self.create_or_update_feature_table_cache(
                feature_store=feature_store,
                observation_table=observation_table,
                graph=graph,
                nodes=nodes,
                is_target=is_target,
                feature_list_id=feature_list_id,
                serving_names_mapping=serving_names_mapping,
                progress_callback=progress_callback,
            )

        hashes = update_result.hashes
        db_session = update_result.session
        historical_features_metrics = update_result.historical_features_metrics
        request_column_names = [col.name for col in observation_table.columns_info]
        select_expr = await self.get_feature_query(
            db_session=db_session,
            observation_table=observation_table,
            hashes=hashes,
            output_column_names=[
                cast(str, graph.get_node_output_column_name(node.name)) for node in nodes
            ],
            additional_columns=request_column_names,
        )

        await db_session.create_table_as(
            table_details=output_view_details,
            select_expr=select_expr,
            kind="TABLE",
        )
        historical_features_metrics.total_seconds = time.time() - first_tic
        return False, historical_features_metrics
