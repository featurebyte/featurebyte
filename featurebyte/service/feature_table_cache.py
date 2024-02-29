"""
Module for managing physical feature table cache as well as metadata storage.
"""
from typing import Any, Callable, Coroutine, Dict, List, Optional, Tuple, cast

from collections import defaultdict

import pandas as pd
from bson import ObjectId
from sqlglot import expressions

from featurebyte.enum import InternalName, MaterializedTableNamePrefix
from featurebyte.exception import DocumentNotFoundError
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.feature_table_cache_metadata import (
    CachedFeatureDefinition,
    FeatureTableCacheMetadataModel,
)
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.common import (
    get_fully_qualified_table_name,
    get_qualified_column_identifier,
    quoted_identifier,
    sql_to_string,
)
from featurebyte.query_graph.transform.definition import DefinitionHashExtractor
from featurebyte.query_graph.transform.offline_store_ingest import extract_dtype_from_graph
from featurebyte.service.entity_validation import EntityValidationService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_table_cache_metadata import FeatureTableCacheMetadataService
from featurebyte.service.historical_features_and_target import get_historical_features, get_target
from featurebyte.service.namespace_handler import NamespaceHandler
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.tile_cache import TileCacheService
from featurebyte.session.base import BaseSession


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
    ):
        self.feature_table_cache_metadata_service = feature_table_cache_metadata_service
        self.namespace_handler = namespace_handler
        self.session_manager_service = session_manager_service
        self.entity_validation_service = entity_validation_service
        self.tile_cache_service = tile_cache_service
        self.feature_list_service = feature_list_service

    async def definition_hashes_for_nodes(
        self,
        graph: QueryGraph,
        nodes: List[Node],
    ) -> List[str]:
        """
        Compute definition caches for list of nodes

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

    def _get_column_exprs(
        self,
        graph: QueryGraph,
        nodes: List[Node],
        hashes: List[str],
        cached_features: Dict[str, str],
    ) -> List[expressions.Alias]:
        return [
            expressions.alias_(
                quoted_identifier(cached_features[definition_hash]),
                alias=graph.get_node_output_column_name(node.name),
                quoted=True,
            )
            for definition_hash, node in zip(hashes, nodes)
        ]

    async def get_non_cached_nodes(
        self,
        feature_table_cache_metadata: FeatureTableCacheMetadataModel,
        graph: QueryGraph,
        nodes: List[Node],
    ) -> List[Tuple[Node, CachedFeatureDefinition]]:
        """
        Given an observation table, graph and set of nodes
            - compute nodes definition hashes
            - lookup existing Feature Table Cache metadata
            - filter out nodes which are already added to the Feature Table Cache
            - return cached and non-cached nodes and their definition hashes.

        Parameters
        ----------
        feature_table_cache_metadata: FeatureTableCacheMetadataModel
            Feature table cache metadata
        graph: QueryGraph
            Graph definition
        nodes: List[Node]
            Input node names

        Returns
        -------
        List[Tuple[Node, CachedFeatureDefinition]]
            List of non cached nodes and respective newly-created cached feature definitions
        """
        hashes = await self.definition_hashes_for_nodes(graph, nodes)

        cached_hashes = {
            feat.definition_hash: feat for feat in feature_table_cache_metadata.feature_definitions
        }
        added_hashes = set()
        non_cached_nodes = []
        for definition_hash, node in zip(hashes, nodes):
            if definition_hash in cached_hashes or definition_hash in added_hashes:
                continue
            added_hashes.add(definition_hash)
            non_cached_nodes.append(
                (
                    node,
                    CachedFeatureDefinition(definition_hash=definition_hash),
                )
            )
        return non_cached_nodes

    async def _populate_intermediate_table(  # pylint: disable=too-many-arguments
        self,
        feature_store: FeatureStoreModel,
        observation_table: ObservationTableModel,
        db_session: BaseSession,
        intermediate_table_name: str,
        graph: QueryGraph,
        nodes: List[Tuple[Node, CachedFeatureDefinition]],
        is_target: bool = False,
        serving_names_mapping: Optional[Dict[str, str]] = None,
        is_feature_list_deployed: bool = False,
        progress_callback: Optional[
            Callable[[int, Optional[str]], Coroutine[Any, Any, None]]
        ] = None,
    ) -> None:
        request_column_names = {col.name for col in observation_table.columns_info}
        nodes_only = [node for node, _ in nodes]
        parent_serving_preparation = (
            await self.entity_validation_service.validate_entities_or_prepare_for_parent_serving(
                graph=graph,
                nodes=nodes_only,
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
            await get_target(
                session=db_session,
                graph=graph,
                nodes=nodes_only,
                observation_set=observation_table,
                feature_store=feature_store,
                output_table_details=output_table_details,
                serving_names_mapping=serving_names_mapping,
                parent_serving_preparation=parent_serving_preparation,
                progress_callback=progress_callback,
            )
        else:
            await get_historical_features(
                session=db_session,
                tile_cache_service=self.tile_cache_service,
                graph=graph,
                nodes=nodes_only,
                observation_set=observation_table,
                feature_store=feature_store,
                output_table_details=output_table_details,
                serving_names_mapping=serving_names_mapping,
                is_feature_list_deployed=is_feature_list_deployed,
                parent_serving_preparation=parent_serving_preparation,
                progress_callback=progress_callback,
            )

    async def _create_table(  # pylint: disable=too-many-arguments
        self,
        feature_store: FeatureStoreModel,
        observation_table: ObservationTableModel,
        db_session: BaseSession,
        final_table_name: str,
        graph: QueryGraph,
        nodes: List[Tuple[Node, CachedFeatureDefinition]],
        is_target: bool = False,
        serving_names_mapping: Optional[Dict[str, str]] = None,
        is_feature_list_deployed: bool = False,
        progress_callback: Optional[
            Callable[[int, Optional[str]], Coroutine[Any, Any, None]]
        ] = None,
    ) -> None:
        intermediate_table_name = (
            f"__TEMP__{MaterializedTableNamePrefix.FEATURE_TABLE_CACHE}_{ObjectId()}"
        )
        try:
            await self._populate_intermediate_table(
                feature_store=feature_store,
                observation_table=observation_table,
                db_session=db_session,
                intermediate_table_name=intermediate_table_name,
                graph=graph,
                nodes=nodes,
                is_target=is_target,
                serving_names_mapping=serving_names_mapping,
                is_feature_list_deployed=is_feature_list_deployed,
                progress_callback=progress_callback,
            )

            request_column_names = [col.name for col in observation_table.columns_info]
            request_columns = [quoted_identifier(col) for col in request_column_names]
            feature_names = [
                expressions.alias_(
                    quoted_identifier(cast(str, graph.get_node_output_column_name(node.name))),
                    alias=feature_definition.feature_name,
                    quoted=True,
                )
                for node, feature_definition in nodes
            ]
            adapter = get_sql_adapter(db_session.source_type)
            query = sql_to_string(
                adapter.create_table_as(
                    table_details=TableDetails(
                        database_name=db_session.database_name,
                        schema_name=db_session.schema_name,
                        table_name=final_table_name,
                    ),
                    select_expr=(
                        expressions.select(quoted_identifier(InternalName.TABLE_ROW_INDEX))
                        .select(*request_columns)
                        .select(*feature_names)
                        .from_(quoted_identifier(intermediate_table_name))
                    ),
                ),
                source_type=db_session.source_type,
            )
            await db_session.execute_query(query)
        finally:
            await db_session.drop_table(
                database_name=db_session.database_name,
                schema_name=db_session.schema_name,
                table_name=intermediate_table_name,
                if_exists=True,
            )

    async def _update_table(  # pylint: disable=too-many-arguments
        self,
        feature_store: FeatureStoreModel,
        observation_table: ObservationTableModel,
        cache_metadata: FeatureTableCacheMetadataModel,
        db_session: BaseSession,
        graph: QueryGraph,
        non_cached_nodes: List[Tuple[Node, CachedFeatureDefinition]],
        is_target: bool = False,
        serving_names_mapping: Optional[Dict[str, str]] = None,
        is_feature_list_deployed: bool = False,
        progress_callback: Optional[
            Callable[[int, Optional[str]], Coroutine[Any, Any, None]]
        ] = None,
    ) -> None:
        # create temporary table with features
        intermediate_table_name = (
            f"__TEMP__{MaterializedTableNamePrefix.FEATURE_TABLE_CACHE}_{ObjectId()}"
        )

        merge_target_table_alias = "feature_table_cache"
        merge_source_table_alias = "partial_features"

        try:
            await self._populate_intermediate_table(
                feature_store=feature_store,
                observation_table=observation_table,
                db_session=db_session,
                intermediate_table_name=intermediate_table_name,
                graph=graph,
                nodes=non_cached_nodes,
                is_target=is_target,
                serving_names_mapping=serving_names_mapping,
                is_feature_list_deployed=is_feature_list_deployed,
                progress_callback=progress_callback,
            )

            # alter cached tables adding columns for new features
            adapter = get_sql_adapter(db_session.source_type)
            table_exr = expressions.Table(
                this=quoted_identifier(cache_metadata.table_name),
                db=quoted_identifier(db_session.schema_name),
                catalog=quoted_identifier(db_session.database_name),
            )
            columns_expr = [
                expressions.ColumnDef(
                    this=quoted_identifier(cast(str, definition.feature_name)),
                    kind=adapter.get_physical_type_from_dtype(
                        extract_dtype_from_graph(graph, node)
                    ),
                )
                for node, definition in non_cached_nodes
            ]
            await db_session.execute_query(adapter.alter_table_add_columns(table_exr, columns_expr))

            # merge temp table into cache table
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
                expressions=[
                    expressions.When(
                        this=expressions.Column(this=expressions.Identifier(this="MATCHED")),
                        then=update_expr,
                    ),
                ],
            )
            await db_session.execute_query(
                sql_to_string(merge_expr, source_type=db_session.source_type)
            )
        finally:
            await db_session.drop_table(
                database_name=db_session.database_name,
                schema_name=db_session.schema_name,
                table_name=intermediate_table_name,
                if_exists=True,
            )

    async def create_or_update_feature_table_cache(
        self,
        feature_store: FeatureStoreModel,
        observation_table: ObservationTableModel,
        graph: QueryGraph,
        nodes: List[Node],
        is_target: bool = False,
        feature_list_id: Optional[PydanticObjectId] = None,
        serving_names_mapping: Optional[Dict[str, str]] = None,
        progress_callback: Optional[
            Callable[[int, Optional[str]], Coroutine[Any, Any, None]]
        ] = None,
    ) -> None:
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
        is_target : bool
            Whether it is a target computation call
        feature_list_id: Optional[PydanticObjectId]
            Optional feature list id
        serving_names_mapping: Optional[Dict[str, str]]
            Optional serving names mapping if the observations set has different serving name columns
            than those defined in Entities
        progress_callback: Optional[Callable[[int, Optional[str]], Coroutine[Any, Any, None]]]
            Optional progress callback function
        """
        assert (
            observation_table.has_row_index
        ), "Observation Tables without row index are not supported"

        cache_metadata = (
            await self.feature_table_cache_metadata_service.get_or_create_feature_table_cache(
                observation_table_id=observation_table.id,
            )
        )
        feature_table_cache_exists = bool(cache_metadata.feature_definitions)

        non_cached_nodes = await self.get_non_cached_nodes(cache_metadata, graph, nodes)

        if non_cached_nodes:
            is_feature_list_deployed = False
            if feature_list_id:
                try:
                    feature_list = await self.feature_list_service.get_document(feature_list_id)
                    is_feature_list_deployed = feature_list.deployed
                except DocumentNotFoundError:
                    is_feature_list_deployed = False

            db_session = await self.session_manager_service.get_feature_store_session(
                feature_store=feature_store
            )

            if feature_table_cache_exists:
                # if feature table cache exists - update existing table with new features
                await self._update_table(
                    feature_store=feature_store,
                    observation_table=observation_table,
                    cache_metadata=cache_metadata,
                    db_session=db_session,
                    graph=graph,
                    non_cached_nodes=non_cached_nodes,
                    is_target=is_target,
                    serving_names_mapping=serving_names_mapping,
                    is_feature_list_deployed=is_feature_list_deployed,
                    progress_callback=progress_callback,
                )
            else:
                # if feature table doesn't exist yet - create from scratch
                await self._create_table(
                    feature_store=feature_store,
                    observation_table=observation_table,
                    db_session=db_session,
                    final_table_name=cache_metadata.table_name,
                    graph=graph,
                    nodes=non_cached_nodes,
                    is_target=is_target,
                    serving_names_mapping=serving_names_mapping,
                    is_feature_list_deployed=is_feature_list_deployed,
                    progress_callback=progress_callback,
                )

            await self.feature_table_cache_metadata_service.update_feature_table_cache(
                observation_table_id=observation_table.id,
                feature_definitions=[definition for _, definition in non_cached_nodes],
            )

    async def read_from_cache(
        self,
        feature_store: FeatureStoreModel,
        observation_table: ObservationTableModel,
        graph: QueryGraph,
        nodes: List[Node],
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

        Returns
        -------
        pd.DataFrame
            Result data
        """
        hashes = await self.definition_hashes_for_nodes(graph, nodes)
        cache_metadata = (
            await self.feature_table_cache_metadata_service.get_or_create_feature_table_cache(
                observation_table_id=observation_table.id,
            )
        )
        cached_hashes = set(
            feature_def.definition_hash for feature_def in cache_metadata.feature_definitions
        )
        assert set(hashes) <= cached_hashes, "All nodes must be cached"

        cached_features = {
            feature.definition_hash: feature.feature_name
            for feature in cache_metadata.feature_definitions
        }
        db_session = await self.session_manager_service.get_feature_store_session(
            feature_store=feature_store
        )
        columns_expr = self._get_column_exprs(
            graph, nodes, hashes, cast(Dict[str, str], cached_features)
        )
        select_expr = (
            expressions.select(quoted_identifier(InternalName.TABLE_ROW_INDEX))
            .select(*columns_expr)
            .from_(
                get_fully_qualified_table_name(
                    {
                        "database_name": db_session.database_name,
                        "schema_name": db_session.schema_name,
                        "table_name": cache_metadata.table_name,
                    }
                )
            )
        )
        sql = sql_to_string(select_expr, source_type=db_session.source_type)
        return await db_session.execute_query(sql)

    async def create_view_from_cache(
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
    ) -> None:
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
        """
        await self.create_or_update_feature_table_cache(
            feature_store=feature_store,
            observation_table=observation_table,
            graph=graph,
            nodes=nodes,
            is_target=is_target,
            feature_list_id=feature_list_id,
            serving_names_mapping=serving_names_mapping,
            progress_callback=progress_callback,
        )
        cache_metadata = (
            await self.feature_table_cache_metadata_service.get_or_create_feature_table_cache(
                observation_table_id=observation_table.id,
            )
        )
        cached_features = {
            feature.definition_hash: feature.feature_name
            for feature in cache_metadata.feature_definitions
        }
        hashes = await self.definition_hashes_for_nodes(graph, nodes)

        db_session = await self.session_manager_service.get_feature_store_session(
            feature_store=feature_store
        )

        request_column_names = [col.name for col in observation_table.columns_info]
        request_columns = [quoted_identifier(col) for col in request_column_names]
        columns_expr = self._get_column_exprs(
            graph, nodes, hashes, cast(Dict[str, str], cached_features)
        )
        select_expr = (
            expressions.select(quoted_identifier(InternalName.TABLE_ROW_INDEX))
            .select(*request_columns)
            .select(*columns_expr)
            .from_(
                get_fully_qualified_table_name(
                    {
                        "database_name": db_session.database_name,
                        "schema_name": db_session.schema_name,
                        "table_name": cache_metadata.table_name,
                    }
                )
            )
        )
        create_view_exr = expressions.Create(
            this=expressions.Table(
                this=get_fully_qualified_table_name(output_view_details.dict()),
            ),
            kind="VIEW",
            expression=select_expr,
            replace=False,
        )
        sql = sql_to_string(create_view_exr, source_type=db_session.source_type)
        await db_session.execute_query(sql)
