"""
FeatureMaterializeService class
"""

from __future__ import annotations

import json
import tempfile
import textwrap
import time
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any, AsyncIterator, Dict, Iterator, List, Optional, Tuple, cast

import pandas as pd
from bson import ObjectId
from redis import Redis
from redis.lock import Lock
from sqlglot import expressions

from featurebyte.common.env_util import set_environment_variables
from featurebyte.enum import DBVarType, InternalName, SourceType
from featurebyte.feast.infra.offline_stores.bigquery import FeatureByteBigQueryOfflineStoreConfig
from featurebyte.feast.service.feature_store import FeastFeatureStore, FeastFeatureStoreService
from featurebyte.feast.service.registry import FeastRegistryService
from featurebyte.feast.utils.materialize_helper import materialize_partial
from featurebyte.logging import get_logger
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.offline_store_feature_table import (
    OfflineLastMaterializedAtUpdate,
    OfflineStoreFeatureTableModel,
)
from featurebyte.models.system_metrics import ScheduledFeatureMaterializeMetrics
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.adapter import BaseAdapter
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import (
    get_qualified_column_identifier,
    quoted_identifier,
    sql_to_string,
)
from featurebyte.query_graph.sql.entity import (
    DUMMY_ENTITY_COLUMN_NAME,
    get_combined_serving_names,
    get_combined_serving_names_expr,
)
from featurebyte.query_graph.sql.online_serving import (
    TemporaryBatchRequestTable,
    get_online_features,
)
from featurebyte.service.catalog import CatalogService
from featurebyte.service.column_statistics import ColumnStatisticsService
from featurebyte.service.cron_helper import CronHelper
from featurebyte.service.deployed_tile_table import DeployedTileTableService
from featurebyte.service.deployment import DeploymentService
from featurebyte.service.entity_validation import EntityValidationService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_materialize_run import FeatureMaterializeRunService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.materialized_table import BaseMaterializedTableService
from featurebyte.service.offline_store_feature_table import OfflineStoreFeatureTableService
from featurebyte.service.online_store_table_version import OnlineStoreTableVersionService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.system_metrics import SystemMetricsService
from featurebyte.session.base import BaseSession
from featurebyte.session.session_helper import SessionHandler

OFFLINE_STORE_TABLE_REDIS_LOCK_TIMEOUT_SECONDS = 3600
NUM_COLUMNS_PER_MATERIALIZE = 50

logger = get_logger(__name__)


@contextmanager
def setup_online_materialize_environment(feature_store: FeastFeatureStore) -> Iterator[None]:
    """
    Setup environment for online materialization

    Parameters
    ----------
    feature_store: FeastFeatureStore
        Feast feature store

    Yields
    ------
    Iterator[None]
        The context manager
    """
    if isinstance(feature_store.config.offline_store, FeatureByteBigQueryOfflineStoreConfig):
        with tempfile.NamedTemporaryFile(mode="w") as temp_credentials_file:
            json.dump(
                feature_store.config.offline_store.database_credential.service_account_info,
                temp_credentials_file,
            )
            temp_credentials_file.flush()
            env_overrides = {"GOOGLE_APPLICATION_CREDENTIALS": temp_credentials_file.name}
            with set_environment_variables(env_overrides):
                yield
    else:
        yield


@dataclass
class MaterializedFeatures:
    """
    Information about materialised features ready to be published
    """

    materialized_table_name: str
    column_names: List[str]
    data_types: List[str]
    serving_names: List[str]
    feature_timestamp: datetime
    source_type: SourceType

    @property
    def serving_names_and_column_names(self) -> List[str]:
        """
        Returns serving names and column names in a combined list

        Returns
        -------
        List[str]
        """
        result = self.serving_names[:]
        if len(self.serving_names) > 1:
            result.append(get_combined_serving_names(self.serving_names))
        if len(self.serving_names) == 0 and self.source_type == SourceType.DATABRICKS_UNITY:
            result.append(DUMMY_ENTITY_COLUMN_NAME)
        result += self.column_names
        return result


@dataclass
class MaterializedFeaturesSet:
    """
    A set of materialized features for an offline store feature table.

    There can be multiple materialized features if precomputed lookup feature tables are required
    for the offline store feature table.
    """

    all_materialized_features: Dict[str, MaterializedFeatures]
    table_name_to_feature_table: Dict[str, OfflineStoreFeatureTableModel]

    @classmethod
    def create(
        cls,
        source_feature_table: OfflineStoreFeatureTableModel,
        source_materialized_features: MaterializedFeatures,
    ) -> MaterializedFeaturesSet:
        """
        Create an instance of MaterializedFeatureSet given the materialized features corresponding
        to the offline store feature table. Subsequent materialized features can be added.

        Parameters
        ----------
        source_feature_table: OfflineStoreFeatureTableModel
            Source feature table
        source_materialized_features: MaterializedFeatures
            Materialized features for the feature table

        Returns
        -------
        MaterializedFeaturesSet
        """
        return MaterializedFeaturesSet(
            all_materialized_features={source_feature_table.name: source_materialized_features},
            table_name_to_feature_table={source_feature_table.name: source_feature_table},
        )

    def add_lookup_materialized_features(
        self,
        feature_table: OfflineStoreFeatureTableModel,
        materialized_features: MaterializedFeatures,
    ) -> None:
        """
        Add a MaterializedFeatures

        Parameters
        ----------
        feature_table: OfflineStoreFeatureTableModel
            A precomputed lookup feature table that the materialized features correspond to
        materialized_features: MaterializedFeatures
            Materialized features for the feature table
        """
        self.all_materialized_features[feature_table.name] = materialized_features
        self.table_name_to_feature_table[feature_table.name] = feature_table

    def iterate_materialized_features(
        self,
    ) -> Iterator[Tuple[OfflineStoreFeatureTableModel, MaterializedFeatures]]:
        """
        Iterate over materialized features

        Yields
        ------
        Tuple[OfflineStoreFeatureTableModel, MaterializedFeatures]
            Tuple of feature table object and materialized features
        """
        for table_name, materialized_features in self.all_materialized_features.items():
            yield self.table_name_to_feature_table[table_name], materialized_features


class FeatureMaterializeService:
    """
    FeatureMaterializeService is responsible for materialising a set of currently online enabled
    features so that they can be published to an external feature store. These features are
    materialised into a new table in the data warehouse.
    """

    def __init__(
        self,
        feature_service: FeatureService,
        online_store_table_version_service: OnlineStoreTableVersionService,
        feature_store_service: FeatureStoreService,
        session_manager_service: SessionManagerService,
        feast_registry_service: FeastRegistryService,
        feast_feature_store_service: FeastFeatureStoreService,
        offline_store_feature_table_service: OfflineStoreFeatureTableService,
        entity_validation_service: EntityValidationService,
        deployment_service: DeploymentService,
        feature_materialize_run_service: FeatureMaterializeRunService,
        cron_helper: CronHelper,
        system_metrics_service: SystemMetricsService,
        deployed_tile_table_service: DeployedTileTableService,
        catalog_service: CatalogService,
        column_statistics_service: ColumnStatisticsService,
        redis: Redis[Any],
    ):
        self.feature_service = feature_service
        self.online_store_table_version_service = online_store_table_version_service
        self.feature_store_service = feature_store_service
        self.session_manager_service = session_manager_service
        self.feast_registry_service = feast_registry_service
        self.feast_feature_store_service = feast_feature_store_service
        self.offline_store_feature_table_service = offline_store_feature_table_service
        self.entity_validation_service = entity_validation_service
        self.deployment_service = deployment_service
        self.feature_materialize_run_service = feature_materialize_run_service
        self.cron_helper = cron_helper
        self.system_metrics_service = system_metrics_service
        self.deployed_tile_table_service = deployed_tile_table_service
        self.catalog_service = catalog_service
        self.column_statistics_service = column_statistics_service
        self.redis = redis

    @asynccontextmanager
    async def materialize_features(
        self,
        feature_table_model: OfflineStoreFeatureTableModel,
        selected_columns: Optional[List[str]] = None,
        session: Optional[BaseSession] = None,
        use_last_materialized_timestamp: bool = True,
        empty_universe: bool = False,
        metrics: Optional[ScheduledFeatureMaterializeMetrics] = None,
    ) -> AsyncIterator[MaterializedFeaturesSet]:
        """
        Materialise features for the provided offline store feature table.

        Parameters
        ----------
        feature_table_model: OfflineStoreFeatureTableModel
            OfflineStoreFeatureTableModel object
        selected_columns: Optional[List[str]]
            Selected columns to materialize
        session: Optional[BaseSession]
            Session object
        use_last_materialized_timestamp: bool
            Whether to specify the last materialized timestamp of feature_table_model when creating
            the entity universe. This is set to True on scheduled task, and False on initialization
            of new columns.
        empty_universe: bool
            If True, the entity universe will be made empty on purpose, and no features will be
            materialized. This is used to retrieve the expected output columns for initialization
            purpose.
        metrics: Optional[ScheduledFeatureMaterializeMetrics]
            Metrics object to be updated in place if specified

        Yields
        ------
        MaterializedFeatures
            Metadata of the materialized features
        """
        assert feature_table_model.feature_cluster is not None, "Missing feature cluster"

        # Create temporary batch request table with the universe of entities
        if session is None:
            session = await self._get_session(feature_table_model)

        tic = time.time()
        unique_id = ObjectId()
        batch_request_table = TemporaryBatchRequestTable(
            table_details=TableDetails(
                database_name=session.database_name,
                schema_name=session.schema_name,
                table_name=f"TEMP_REQUEST_TABLE_{unique_id}".upper(),
            ),
            column_names=feature_table_model.serving_names,
        )
        feature_timestamp = datetime.utcnow()

        select_expr = feature_table_model.entity_universe.get_entity_universe_expr(
            current_feature_timestamp=feature_timestamp,
            last_materialized_timestamp=(
                feature_table_model.last_materialized_at
                if use_last_materialized_timestamp
                else None
            ),
        )
        if empty_universe:
            select_expr = select_expr.limit(0)
        await session.create_table_as(
            table_details=batch_request_table.table_details,
            select_expr=select_expr,
        )
        await BaseMaterializedTableService.add_row_index_column(
            session, batch_request_table.table_details
        )
        if metrics:
            metrics.generate_entity_universe_seconds = time.time() - tic

        # Materialize features
        output_table_details = TableDetails(
            database_name=session.database_name,
            schema_name=session.schema_name,
            table_name=f"TEMP_FEATURE_TABLE_{unique_id}".upper(),
        )
        tables_names_pending_cleanup = [
            batch_request_table.table_details.table_name,
            output_table_details.table_name,
        ]
        try:
            tic = time.time()
            if selected_columns is None:
                nodes = feature_table_model.feature_cluster.nodes
            else:
                nodes = feature_table_model.feature_cluster.get_nodes_for_feature_names(
                    selected_columns
                )
            feature_store = await self.feature_store_service.get_document(
                document_id=feature_table_model.feature_cluster.feature_store_id
            )
            parent_serving_preparation = await self.entity_validation_service.validate_entities_or_prepare_for_parent_serving(
                graph_nodes=(feature_table_model.feature_cluster.graph, nodes),
                feature_list_model=None,
                offline_store_feature_table_model=feature_table_model,
                request_column_names=set(feature_table_model.serving_names),
                feature_store=feature_store,
            )
            on_demand_tile_tables = (
                await self.deployed_tile_table_service.get_deployed_tile_table_info()
            ).on_demand_tile_tables
            await get_online_features(
                session_handler=SessionHandler(
                    session=session,
                    redis=self.redis,
                    feature_store=feature_store,
                    system_metrics_service=self.system_metrics_service,
                ),
                cron_helper=self.cron_helper,
                column_statistics_service=self.column_statistics_service,
                graph=feature_table_model.feature_cluster.graph,
                nodes=nodes,
                request_data=batch_request_table,
                source_info=session.get_source_info(),
                online_store_table_version_service=self.online_store_table_version_service,
                output_table_details=output_table_details,
                request_timestamp=feature_timestamp,
                parent_serving_preparation=parent_serving_preparation,
                concatenate_serving_names=feature_table_model.serving_names,
                on_demand_tile_tables=on_demand_tile_tables,
            )

            column_names = []
            column_dtypes = []
            for column_name, data_type in zip(
                feature_table_model.output_column_names, feature_table_model.output_dtypes
            ):
                if selected_columns is None or column_name in selected_columns:
                    column_names.append(column_name)
                    column_dtypes.append(data_type)

            adapter = session.adapter
            materialized_features = MaterializedFeatures(
                materialized_table_name=output_table_details.table_name,
                column_names=column_names,
                data_types=[adapter.get_physical_type_from_dtype(dtype) for dtype in column_dtypes],
                serving_names=feature_table_model.serving_names,
                feature_timestamp=feature_timestamp,
                source_type=session.source_type,
            )
            materialized_features_set = MaterializedFeaturesSet.create(
                feature_table_model, materialized_features
            )
            if metrics:
                metrics.generate_feature_table_seconds = time.time() - tic

            tic = time.time()
            for (
                lookup_feature_table,
                lookup_materialized_features,
            ) in await self._materialize_precomputed_lookup_feature_tables(
                session=session,
                adapter=adapter,
                feature_table_model=feature_table_model,
                feature_timestamp=feature_timestamp,
                materialized_feature_table_name=output_table_details.table_name,
                column_names=column_names,
                column_dtypes=column_dtypes,
                use_last_materialized_timestamp=use_last_materialized_timestamp,
            ):
                materialized_features_set.add_lookup_materialized_features(
                    lookup_feature_table, lookup_materialized_features
                )
                tables_names_pending_cleanup.append(
                    lookup_materialized_features.materialized_table_name
                )
            if metrics:
                metrics.generate_precomputed_lookup_feature_tables_seconds = time.time() - tic

            yield materialized_features_set

        finally:
            # Delete temporary batch request table and materialized feature table
            for table_name in tables_names_pending_cleanup:
                await session.drop_table(
                    table_name=table_name,
                    schema_name=session.schema_name,
                    database_name=session.database_name,
                    if_exists=True,
                )

    async def _get_precomputed_lookup_feature_tables(
        self, feature_table_model: OfflineStoreFeatureTableModel
    ) -> List[OfflineStoreFeatureTableModel]:
        return [
            doc
            async for doc in self.offline_store_feature_table_service.list_precomputed_lookup_feature_tables_from_source(
                feature_table_model.id
            )
        ]

    async def _materialize_precomputed_lookup_feature_tables(
        self,
        session: BaseSession,
        adapter: BaseAdapter,
        feature_table_model: OfflineStoreFeatureTableModel,
        feature_timestamp: datetime,
        materialized_feature_table_name: str,
        column_names: List[str],
        column_dtypes: List[DBVarType],
        use_last_materialized_timestamp: bool,
    ) -> List[Tuple[OfflineStoreFeatureTableModel, MaterializedFeatures]]:
        precomputed_lookup_feature_tables = await self._get_precomputed_lookup_feature_tables(
            feature_table_model
        )
        result = []
        for lookup_feature_table in precomputed_lookup_feature_tables:
            lookup_materialized_features = await self._materialize_precomputed_lookup_feature_table(
                session=session,
                adapter=adapter,
                feature_timestamp=feature_timestamp,
                materialized_feature_table_name=materialized_feature_table_name,
                column_names=column_names,
                column_dtypes=column_dtypes,
                source_feature_table_serving_names=feature_table_model.serving_names,
                use_last_materialized_timestamp=(
                    use_last_materialized_timestamp and not feature_table_model.has_ttl
                ),
                lookup_feature_table=lookup_feature_table,
            )
            result.append((lookup_feature_table, lookup_materialized_features))
        return result

    @staticmethod
    async def _materialize_precomputed_lookup_feature_table(
        session: BaseSession,
        adapter: BaseAdapter,
        feature_timestamp: datetime,
        materialized_feature_table_name: str,
        column_names: List[str],
        column_dtypes: List[DBVarType],
        use_last_materialized_timestamp: bool,
        source_feature_table_serving_names: List[str],
        lookup_feature_table: OfflineStoreFeatureTableModel,
    ) -> MaterializedFeatures:
        # Construct a lookup universe table with both the child and parent entities to be joined
        # with the source feature table
        unique_id = ObjectId()
        lookup_universe_table_details = TableDetails(
            database_name=session.database_name,
            schema_name=session.schema_name,
            table_name=f"TEMP_LOOKUP_UNIVERSE_TABLE_{unique_id}".upper(),
        )
        create_entity_universe_table_query = sql_to_string(
            adapter.create_table_as(
                table_details=lookup_universe_table_details,
                select_expr=lookup_feature_table.entity_universe.get_entity_universe_expr(
                    current_feature_timestamp=feature_timestamp,
                    last_materialized_timestamp=(
                        lookup_feature_table.last_materialized_at
                        if use_last_materialized_timestamp
                        else None
                    ),
                ),
            ),
            source_type=session.source_type,
        )
        await session.execute_query_long_running(create_entity_universe_table_query)
        lookup_feature_table_details = TableDetails(
            database_name=session.database_name,
            schema_name=session.schema_name,
            table_name=f"TEMP_LOOKUP_FEATURE_TABLE_{unique_id}".upper(),
        )

        # Handle serving names and join conditions
        lookup_info = lookup_feature_table.precomputed_lookup_feature_table_info
        assert lookup_info is not None

        serving_name_exprs = {}
        join_conditions = []
        for source_serving_name in source_feature_table_serving_names:
            lookup_serving_name: Optional[str]
            if lookup_info.lookup_mapping is None:
                lookup_serving_name = source_serving_name  # backward compatibility
            else:
                lookup_serving_name = lookup_info.get_lookup_feature_table_serving_name(
                    source_serving_name
                )
            if lookup_serving_name is None:
                # source_serving_name is not part of the entity lookup, so it won't appear in the
                # entity universe. This can happen in composite entity where only one of the
                # entities are looked up in the precomputed lookup feature table.
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
                        expression=get_qualified_column_identifier(source_serving_name, "R"),
                    )
                )

        if len(lookup_feature_table.serving_names) > 1:
            combined_serving_names = get_combined_serving_names(lookup_feature_table.serving_names)
            serving_name_exprs[combined_serving_names] = expressions.alias_(
                get_combined_serving_names_expr([
                    serving_name_exprs[serving_name]
                    for serving_name in lookup_feature_table.serving_names
                ]),
                alias=combined_serving_names,
            )

        # Create the lookup feature table by joining the entity table with the source feature table
        create_lookup_feature_table_query = sql_to_string(
            adapter.create_table_as(
                table_details=lookup_feature_table_details,
                select_expr=expressions.select(
                    *(
                        list(serving_name_exprs.values())
                        + [
                            get_qualified_column_identifier(column_name, "R")
                            for column_name in column_names
                        ]
                    )
                )
                .from_(
                    expressions.Table(
                        this=quoted_identifier(lookup_universe_table_details.table_name),
                        alias=expressions.TableAlias(this="L"),
                    ),
                )
                .join(
                    expressions.Table(
                        this=quoted_identifier(materialized_feature_table_name),
                        alias=expressions.TableAlias(this="R"),
                    ),
                    join_type="left",
                    on=expressions.and_(*join_conditions),
                ),
            ),
            source_type=session.source_type,
        )
        await session.execute_query_long_running(create_lookup_feature_table_query)
        await session.drop_table(
            table_name=lookup_universe_table_details.table_name,
            schema_name=lookup_universe_table_details.schema_name,  # type: ignore
            database_name=lookup_universe_table_details.database_name,  # type: ignore
            if_exists=True,
        )
        return MaterializedFeatures(
            materialized_table_name=lookup_feature_table_details.table_name,
            column_names=column_names,
            data_types=[adapter.get_physical_type_from_dtype(dtype) for dtype in column_dtypes],
            serving_names=lookup_feature_table.serving_names,
            feature_timestamp=feature_timestamp,
            source_type=session.source_type,
        )

    def get_table_update_lock(self, offline_store_table_name: str) -> Lock:
        """
        Get offline store table update lock.

        This prevents concurrent writes to the same offline store table between scheduled task and
        deployment task.

        Parameters
        ----------
        offline_store_table_name: str
            Offline store table name

        Returns
        -------
        Lock
        """
        return self.redis.lock(
            f"offline_store_table_update:{offline_store_table_name}",
            timeout=OFFLINE_STORE_TABLE_REDIS_LOCK_TIMEOUT_SECONDS,
        )

    async def scheduled_materialize_features(
        self,
        feature_table_model: OfflineStoreFeatureTableModel,
        feature_materialize_run_id: Optional[ObjectId] = None,
    ) -> None:
        """
        Materialize features for the provided offline store feature table. This method is expected
        to be called by a scheduler on regular interval, and the feature table is assumed to have
        the correct schema already.

        Parameters
        ----------
        feature_table_model: OfflineStoreFeatureTableModel
            OfflineStoreFeatureTableModel object
        feature_materialize_run_id: Optional[ObjectId]
            Id of the feature materialize run

        Raises
        ------
        Exception
            If any error occurs during the materialization process
        """
        first_tic = time.time()

        if feature_materialize_run_id is not None:
            await self.feature_materialize_run_service.update_feature_materialize_ts(
                feature_materialize_run_id, datetime.utcnow()
            )

        try:
            metrics_data = await self._scheduled_materialize_features(feature_table_model)
        except Exception:
            if feature_materialize_run_id is not None:
                await self.feature_materialize_run_service.set_completion(
                    feature_materialize_run_id, datetime.utcnow(), "failure"
                )
            raise

        if feature_materialize_run_id:
            await self.feature_materialize_run_service.set_completion(
                feature_materialize_run_id, datetime.utcnow(), "success"
            )

        if metrics_data is not None:
            metrics_data.total_seconds = time.time() - first_tic
            await self.system_metrics_service.create_metrics(metrics_data)

    async def _scheduled_materialize_features(
        self,
        feature_table_model: OfflineStoreFeatureTableModel,
    ) -> Optional[ScheduledFeatureMaterializeMetrics]:
        # Exit early if the catalog does not require offline store feature tables to be populated
        need_offline_store_tables = await self._catalog_needs_offline_store_tables(
            feature_table_model
        )
        if not need_offline_store_tables:
            return None

        session = await self._get_session(feature_table_model)

        # Update offline store feature tables
        (
            feature_timestamp,
            feature_tables,
            metrics_data,
        ) = await self.scheduled_populate_offline_feature_table(
            feature_table_model=feature_table_model,
            session=session,
        )

        # Feast online materialize
        tic = time.time()
        for current_feature_table in feature_tables:
            if current_feature_table.deployment_ids:
                service = self.feast_feature_store_service
                feature_store = await service.get_feast_feature_store_for_feature_materialization(
                    feature_table_model=current_feature_table, online_store_id=None
                )
                if feature_store and feature_store.config.online_store is not None:
                    online_store_last_materialized_at = (
                        current_feature_table.get_online_store_last_materialized_at(
                            feature_store.online_store_id  # type: ignore
                        )
                    )
                    await self._materialize_online(
                        session=session,
                        feature_store=feature_store,
                        feature_table=current_feature_table,
                        columns=feature_table_model.output_column_names,
                        start_date=online_store_last_materialized_at,
                        end_date=feature_timestamp,  # type: ignore
                    )
        metrics_data.online_materialize_seconds = time.time() - tic
        return metrics_data

    async def scheduled_populate_offline_feature_table(
        self,
        feature_table_model: OfflineStoreFeatureTableModel,
        session: Optional[BaseSession] = None,
    ) -> Tuple[
        Optional[datetime], List[OfflineStoreFeatureTableModel], ScheduledFeatureMaterializeMetrics
    ]:
        if session is None:
            session = await self._get_session(feature_table_model)

        feature_timestamp = None
        feature_tables = []
        metrics_data = ScheduledFeatureMaterializeMetrics(
            offline_store_feature_table_id=feature_table_model.id,
            num_columns=len(feature_table_model.output_column_names),
        )
        async with self.materialize_features(
            feature_table_model,
            session=session,
            use_last_materialized_timestamp=True,
            metrics=metrics_data,
        ) as materialized_features_set:
            tic = time.time()
            for (
                current_feature_table,
                materialized_features,
            ) in materialized_features_set.iterate_materialized_features():
                with self.get_table_update_lock(
                    offline_store_table_name=current_feature_table.name
                ):
                    await self._insert_into_feature_table(
                        session,
                        current_feature_table.name,
                        materialized_features,
                    )
                feature_tables.append(current_feature_table)
                feature_timestamp = materialized_features.feature_timestamp
                # Update offline table last materialized timestamp
                await self._update_offline_last_materialized_at(
                    current_feature_table, materialized_features.feature_timestamp
                )
            metrics_data.update_feature_tables_seconds = time.time() - tic

        return feature_timestamp, feature_tables, metrics_data

    async def initialize_new_columns(
        self,
        feature_table_model: OfflineStoreFeatureTableModel,
    ) -> None:
        """
        Initialize new columns in the feature table. This is expected to be called when an
        OfflineStoreFeatureTable is updated to include new columns.

        Parameters
        ----------
        feature_table_model: OfflineStoreFeatureTableModel
            OfflineStoreFeatureTableModel object
        """
        session = await self._get_session(feature_table_model)
        num_rows_in_feature_table = await self._num_rows_in_feature_table(
            session, feature_table_model.name
        )
        if num_rows_in_feature_table is not None:
            selected_columns = await self._ensure_compatible_schema(
                session,
                feature_table_name=feature_table_model.name,
                feature_table_column_names=feature_table_model.output_column_names,
                feature_table_column_dtypes=feature_table_model.output_dtypes,
            )
        else:
            selected_columns = None

        need_offline_store_tables = await self._catalog_needs_offline_store_tables(
            feature_table_model
        )

        async with self.materialize_features(
            session=session,
            feature_table_model=feature_table_model,
            selected_columns=selected_columns,
            use_last_materialized_timestamp=False,
            empty_universe=not need_offline_store_tables,
        ) as materialized_features_set:
            for (
                current_feature_table,
                materialized_features,
            ) in materialized_features_set.iterate_materialized_features():
                if not materialized_features.column_names:
                    continue
                await self._initialize_new_columns_offline_and_online(
                    session=session,
                    current_feature_table=current_feature_table,
                    source_feature_table=feature_table_model,
                    materialized_features=materialized_features,
                    should_update_last_materialized_at=need_offline_store_tables,
                )

    async def initialize_precomputed_lookup_feature_table(
        self,
        source_feature_table_id: PydanticObjectId,
        lookup_feature_tables: List[OfflineStoreFeatureTableModel],
    ) -> None:
        """
        Initialize a precomputed lookup feature table by creating it and backfill using the source
        feature table

        Parameters
        ----------
        source_feature_table_id: PydanticObjectId
            Id of the source feature table
        lookup_feature_tables: List[OfflineStoreFeatureTableModel]
            Precomputed lookup feature tables to be initialized
        """
        if not lookup_feature_tables:
            return

        source_feature_table = await self.offline_store_feature_table_service.get_document(
            source_feature_table_id
        )
        session = await self._get_session(source_feature_table)
        num_rows_in_feature_table = await self._num_rows_in_feature_table(
            session, source_feature_table.name
        )
        if num_rows_in_feature_table is None or num_rows_in_feature_table == 0:
            # The source feature table is empty as well, so all the tables can be initialized
            # together by initialize_new_columns() later.
            return

        need_offline_store_tables = await self._catalog_needs_offline_store_tables(
            source_feature_table
        )

        # The source feature table already exists, but precomputed lookup tables don't. In this
        # case, need to initialize lookup feature tables to have the same columns as the source
        # feature table.
        async with self._get_latest_from_feature_table(
            session, source_feature_table
        ) as source_features:
            for lookup_feature_table in lookup_feature_tables:
                materialized_lookup_features = (
                    await self._materialize_precomputed_lookup_feature_table(
                        session=session,
                        adapter=session.adapter,
                        feature_timestamp=source_features.feature_timestamp,
                        materialized_feature_table_name=source_feature_table.name,
                        column_names=source_features.column_names,
                        column_dtypes=source_feature_table.get_output_dtypes_for_columns(
                            source_features.column_names
                        ),
                        use_last_materialized_timestamp=False,
                        source_feature_table_serving_names=source_feature_table.serving_names,
                        lookup_feature_table=lookup_feature_table,
                    )
                )
                try:
                    await self._initialize_new_columns_offline_and_online(
                        session=session,
                        current_feature_table=lookup_feature_table,
                        source_feature_table=source_feature_table,
                        materialized_features=materialized_lookup_features,
                        should_update_last_materialized_at=need_offline_store_tables,
                    )
                finally:
                    await session.drop_table(
                        table_name=materialized_lookup_features.materialized_table_name,
                        schema_name=session.schema_name,
                        database_name=session.database_name,
                        if_exists=True,
                    )

    @asynccontextmanager
    async def _get_latest_from_feature_table(
        self,
        session: BaseSession,
        feature_table_model: OfflineStoreFeatureTableModel,
    ) -> AsyncIterator[MaterializedFeatures]:
        timestamp_cols = [quoted_identifier(InternalName.FEATURE_TIMESTAMP_COLUMN.value)]
        join_key_cols = [quoted_identifier(col) for col in feature_table_model.serving_names]

        # Retrieve columns that are actually available in the physical table. This should be the
        # same as feature_table_model.output_column_names most of the time.
        column_names_not_in_warehouse = set(
            await self._get_column_names_not_in_warehouse(
                session=session,
                feature_table_name=feature_table_model.name,
                feature_table_column_names=feature_table_model.output_column_names,
            )
        )
        available_column_names = [
            col
            for col in feature_table_model.output_column_names
            if col not in column_names_not_in_warehouse
        ]
        adapter = session.adapter
        column_name_to_data_type = {
            column_name: adapter.get_physical_type_from_dtype(dtype)
            for (column_name, dtype) in zip(
                feature_table_model.output_column_names, feature_table_model.output_dtypes
            )
        }
        feature_name_cols = [quoted_identifier(col) for col in available_column_names]

        row_number_expr = expressions.Window(
            this=expressions.RowNumber(),
            partition_by=join_key_cols,
            order=expressions.Order(
                expressions=[
                    expressions.Ordered(this=timestamp_col, desc=True)
                    for timestamp_col in timestamp_cols
                ]
            ),
        )
        select_fields = timestamp_cols + join_key_cols + feature_name_cols

        inner_expr = expressions.Select(
            expressions=select_fields
            + [expressions.alias_(row_number_expr, "_row_number", quoted=True)],
        ).from_(expressions.Table(this=quoted_identifier(feature_table_model.name)))
        expr = (
            expressions.Select(expressions=select_fields)
            .from_(inner_expr.subquery())
            .where(
                expressions.EQ(
                    this=quoted_identifier("_row_number"),
                    expression=make_literal_value(1),
                )
            )
        )
        unique_id = str(ObjectId())
        output_table_details = TableDetails(
            database_name=session.database_name,
            schema_name=session.schema_name,
            table_name=f"TEMP_FEATURE_TABLE_{unique_id}".upper(),
        )
        feature_timestamp = pd.Timestamp(
            await self._get_last_feature_timestamp(session, feature_table_model.name)
        ).to_pydatetime()
        try:
            await session.create_table_as(table_details=output_table_details, select_expr=expr)
            yield MaterializedFeatures(
                materialized_table_name=output_table_details.table_name,
                column_names=available_column_names,
                data_types=[column_name_to_data_type[col] for col in available_column_names],
                serving_names=feature_table_model.serving_names,
                feature_timestamp=feature_timestamp,
                source_type=session.source_type,
            )
        finally:
            await session.drop_table(
                table_name=output_table_details.table_name,
                schema_name=session.schema_name,
                database_name=session.database_name,
                if_exists=True,
            )

    async def _initialize_new_columns_offline_and_online(
        self,
        session: BaseSession,
        current_feature_table: OfflineStoreFeatureTableModel,
        source_feature_table: OfflineStoreFeatureTableModel,
        materialized_features: MaterializedFeatures,
        should_update_last_materialized_at: bool,
    ) -> None:
        with self.get_table_update_lock(current_feature_table.name):
            offline_info = await self._initialize_new_columns_offline(
                session=session,
                feature_table_name=current_feature_table.name,
                feature_table_column_names=materialized_features.column_names,
                feature_table_column_dtypes=source_feature_table.get_output_dtypes_for_columns(
                    materialized_features.column_names
                ),
                feature_table_serving_names=current_feature_table.serving_names,
                materialized_features=materialized_features,
            )
        if offline_info is not None:
            column_names, materialize_end_date, is_new_table = offline_info
            if is_new_table and should_update_last_materialized_at:
                await self._update_offline_last_materialized_at(
                    current_feature_table, materialized_features.feature_timestamp
                )
            await self._initialize_new_columns_online(
                session=session,
                feature_table=current_feature_table,
                column_names=column_names,
                end_date=materialize_end_date,
            )

    async def _initialize_new_columns_offline(
        self,
        session: BaseSession,
        feature_table_name: str,
        feature_table_column_names: List[str],
        feature_table_column_dtypes: List[DBVarType],
        feature_table_serving_names: List[str],
        materialized_features: MaterializedFeatures,
    ) -> Optional[Tuple[List[str], datetime, bool]]:
        num_rows_in_feature_table = await self._num_rows_in_feature_table(
            session, feature_table_name
        )

        if num_rows_in_feature_table is not None:
            await self._ensure_compatible_schema(
                session,
                feature_table_name=feature_table_name,
                feature_table_column_names=feature_table_column_names,
                feature_table_column_dtypes=feature_table_column_dtypes,
            )

        if num_rows_in_feature_table is None:
            # Create feature table is it doesn't exist yet
            await self._create_feature_table(
                session,
                feature_table_name=feature_table_name,
                feature_table_serving_names=feature_table_serving_names,
                materialized_features=materialized_features,
            )
            materialize_end_date = materialized_features.feature_timestamp
            is_new_table = True
        else:
            if num_rows_in_feature_table == 0:
                feature_timestamp_value = materialized_features.feature_timestamp.isoformat()
            else:
                feature_timestamp_value = await self._get_last_feature_timestamp(
                    session, feature_table_name
                )
            # Merge into existing feature table. If the table exists but is empty, do not
            # specify merge conditions so that the merge operation simply inserts rows with
            # new columns.
            await self._merge_into_feature_table(
                session=session,
                feature_table_name=feature_table_name,
                feature_table_serving_names=feature_table_serving_names,
                materialized_features=materialized_features,
                feature_timestamp_value=feature_timestamp_value,
                to_specify_merge_conditions=num_rows_in_feature_table > 0,
            )
            materialize_end_date = pd.Timestamp(feature_timestamp_value).to_pydatetime()
            is_new_table = False

        return materialized_features.column_names, materialize_end_date, is_new_table

    async def _initialize_new_columns_online(
        self,
        session: BaseSession,
        feature_table: OfflineStoreFeatureTableModel,
        column_names: List[str],
        end_date: datetime,
    ) -> None:
        # Feast online materialize. Start date is not set because these are new columns.
        updated_feature_table = await self.offline_store_feature_table_service.get_document(
            document_id=feature_table.id, populate_remote_attributes=False
        )
        if updated_feature_table.deployment_ids:
            service = self.feast_feature_store_service
            feature_store = await service.get_feast_feature_store_for_feature_materialization(
                feature_table_model=updated_feature_table, online_store_id=None
            )
            if feature_store is not None and feature_store.config.online_store is not None:
                assert feature_store.online_store_id is not None
                await self._materialize_online(
                    session=session,
                    feature_store=feature_store,
                    feature_table=feature_table,
                    columns=column_names,
                    end_date=end_date,
                )

    async def update_online_store(
        self,
        feature_store: FeastFeatureStore,
        feature_table_model: OfflineStoreFeatureTableModel,
        session: BaseSession,
    ) -> None:
        """
        Update an online store assuming the offline line feature table is already up-to-date. To be
        called when a catalog switches to a new online store.

        Parameters
        ----------
        feature_store: FeastFeatureStore
            Feast feature store
        feature_table_model: OfflineStoreFeatureTableModel
            Offline store feature table model
        session: BaseSession
            Data warehouse session object
        """
        if feature_table_model.last_materialized_at is None:
            # Skip if offline table is not computed yet as nothing to materialize
            return
        assert feature_store.online_store_id is not None
        end_date = pd.Timestamp(
            await self._get_last_feature_timestamp(session, feature_table_model.name)
        ).to_pydatetime()
        tables = [feature_table_model]
        tables.extend(await self._get_precomputed_lookup_feature_tables(feature_table_model))
        for current_feature_table in tables:
            start_date = current_feature_table.get_online_store_last_materialized_at(
                feature_store.online_store_id
            )
            await self._materialize_online(
                session=session,
                feature_store=feature_store,
                feature_table=current_feature_table,
                columns=feature_table_model.output_column_names,
                start_date=start_date,
                end_date=end_date,
            )

    async def drop_columns(
        self, feature_table_model: OfflineStoreFeatureTableModel, column_names: List[str]
    ) -> None:
        """
        Remove columns from the feature table. This is expected to be called when some features in
        the feature table model were just online disabled.

        Parameters
        ----------
        feature_table_model: OfflineStoreFeatureTableModel
            OfflineStoreFeatureTableModel object
        column_names: List[str]
            List of column names to drop
        """
        with self._must_not_fail(feature_table_model, "drop_columns"):
            session = await self._get_session(feature_table_model)
            for column_name in column_names:
                query = sql_to_string(
                    expressions.Alter(
                        this=expressions.Table(this=quoted_identifier(feature_table_model.name)),
                        kind="TABLE",
                        actions=[
                            expressions.Drop(
                                this=quoted_identifier(column_name), kind="COLUMN", exists=True
                            )
                        ],
                    ),
                    source_type=session.source_type,
                )
                await session.execute_query_long_running(query)

    async def drop_table(self, feature_table_model: OfflineStoreFeatureTableModel) -> None:
        """
        Drop the feature table. This is expected to be called when the feature table is deleted.

        Parameters
        ----------
        feature_table_model: OfflineStoreFeatureTableModel
            OfflineStoreFeatureTableModel object
        """
        with self._must_not_fail(feature_table_model, "drop_table"):
            session = await self._get_session(feature_table_model)
            await session.drop_table(
                feature_table_model.name,
                schema_name=session.schema_name,
                database_name=session.database_name,
                if_exists=True,
            )

    async def _materialize_online(
        self,
        session: BaseSession,
        feature_store: FeastFeatureStore,
        feature_table: OfflineStoreFeatureTableModel,
        columns: List[str],
        end_date: datetime,
        start_date: Optional[datetime] = None,
    ) -> None:
        """
        Calls materialize_partial and also updates online store last materialized at

        # noqa: DAR101
        """
        if not columns:
            return
        assert feature_store.online_store_id is not None
        with setup_online_materialize_environment(feature_store):
            for i in range(0, len(columns), NUM_COLUMNS_PER_MATERIALIZE):
                columns_batch = columns[i : i + NUM_COLUMNS_PER_MATERIALIZE]
                await materialize_partial(
                    session=session,
                    feature_store=feature_store,
                    feature_view=feature_store.get_feature_view(feature_table.name),
                    columns=columns_batch,
                    end_date=end_date,
                    start_date=start_date,
                    with_feature_timestamp=(
                        feature_table.has_ttl
                        if isinstance(feature_table, OfflineStoreFeatureTableModel)
                        else False
                    ),
                )
        await self.offline_store_feature_table_service.update_online_last_materialized_at(
            document_id=feature_table.id,
            online_store_id=feature_store.online_store_id,
            last_materialized_at=end_date,
        )

    async def _update_offline_last_materialized_at(
        self,
        feature_table_model: OfflineStoreFeatureTableModel,
        feature_timestamp: datetime,
    ) -> None:
        update_schema = OfflineLastMaterializedAtUpdate(last_materialized_at=feature_timestamp)
        await self.offline_store_feature_table_service.update_document(
            document_id=feature_table_model.id, data=update_schema
        )

    async def _get_session(self, feature_table_model: OfflineStoreFeatureTableModel) -> BaseSession:
        feature_store_id: Optional[PydanticObjectId]
        if feature_table_model.feature_cluster is not None:
            feature_store_id = feature_table_model.feature_cluster.feature_store_id
        else:
            feature_store_id = feature_table_model.feature_store_id
        assert feature_store_id is not None, "feature_store_id not available"
        feature_store = await self.feature_store_service.get_document(document_id=feature_store_id)
        session = await self.session_manager_service.get_feature_store_session(feature_store)
        return session

    @contextmanager
    def _must_not_fail(
        self,
        feature_table_model: OfflineStoreFeatureTableModel,
        method_name: str,
    ) -> Iterator[None]:
        try:
            yield
        except BaseException:
            logger.error(
                "Unexpected error when attempting to modify offline store feature table",
                extra={"method_name": method_name, "feature_table_id": str(feature_table_model.id)},
                exc_info=True,
            )

    @classmethod
    async def _create_feature_table(
        cls,
        session: BaseSession,
        feature_table_name: str,
        feature_table_serving_names: List[str],
        materialized_features: MaterializedFeatures,
    ) -> None:
        await session.create_table_as(
            table_details=TableDetails(
                database_name=session.database_name,
                schema_name=session.schema_name,
                table_name=feature_table_name,
            ),
            select_expr=expressions.select(
                expressions.alias_(
                    make_literal_value(
                        materialized_features.feature_timestamp.isoformat(),
                        cast_as_timestamp=True,
                    ),
                    alias=InternalName.FEATURE_TIMESTAMP_COLUMN,
                    quoted=True,
                )
            )
            .select(*[
                quoted_identifier(column)
                for column in materialized_features.serving_names_and_column_names
            ])
            .from_(quoted_identifier(materialized_features.materialized_table_name)),
        )
        await cls._add_primary_key_constraint_if_necessary(
            session, feature_table_name, feature_table_serving_names
        )

    @classmethod
    async def _add_primary_key_constraint_if_necessary(
        cls, session: BaseSession, feature_table_name: str, feature_table_serving_names: List[str]
    ) -> None:
        # Only needed for Databricks with Unity catalog for now
        if session.source_type != SourceType.DATABRICKS_UNITY:
            return

        # Table constraint syntax is only supported in newer versions of sqlglot, so the queries are
        # formatted manually here
        if len(feature_table_serving_names) > 0:
            primary_key_columns = feature_table_serving_names[:]
        else:
            primary_key_columns = [DUMMY_ENTITY_COLUMN_NAME]
        quoted_timestamp_column = f"`{InternalName.FEATURE_TIMESTAMP_COLUMN}`"
        quoted_primary_key_columns = [f"`{column_name}`" for column_name in primary_key_columns]
        for quoted_col in [quoted_timestamp_column] + quoted_primary_key_columns:
            await session.execute_query_long_running(
                f"ALTER TABLE `{feature_table_name}` ALTER COLUMN {quoted_col} SET NOT NULL"
            )
        primary_key_args = ", ".join(
            [f"{quoted_timestamp_column} TIMESERIES"] + quoted_primary_key_columns
        )
        await session.execute_query_long_running(
            textwrap.dedent(
                f"""
                ALTER TABLE `{feature_table_name}` ADD CONSTRAINT `pk_{feature_table_name}`
                PRIMARY KEY({primary_key_args})
                """
            ).strip()
        )

    @staticmethod
    async def _insert_into_feature_table(
        session: BaseSession,
        feature_table_name: str,
        materialized_features: MaterializedFeatures,
    ) -> None:
        query = sql_to_string(
            expressions.Insert(
                this=expressions.Schema(
                    this=expressions.Table(this=quoted_identifier(feature_table_name)),
                    expressions=[quoted_identifier(InternalName.FEATURE_TIMESTAMP_COLUMN)]
                    + [
                        quoted_identifier(column)
                        for column in materialized_features.serving_names_and_column_names
                    ],
                ),
                expression=expressions.select(
                    expressions.alias_(
                        make_literal_value(
                            materialized_features.feature_timestamp.isoformat(),
                            cast_as_timestamp=True,
                        ),
                        alias=InternalName.FEATURE_TIMESTAMP_COLUMN,
                        quoted=True,
                    )
                )
                .select(*[
                    quoted_identifier(column)
                    for column in materialized_features.serving_names_and_column_names
                ])
                .from_(quoted_identifier(materialized_features.materialized_table_name)),
            ),
            source_type=session.source_type,
        )
        await session.execute_query_long_running(query)

    @staticmethod
    async def _get_last_feature_timestamp(session: BaseSession, feature_table_name: str) -> str:
        query = sql_to_string(
            expressions.select(
                expressions.alias_(
                    expressions.Max(
                        this=expressions.Column(
                            this=quoted_identifier(InternalName.FEATURE_TIMESTAMP_COLUMN)
                        )
                    ),
                    alias="RESULT",
                )
            ).from_(quoted_identifier(feature_table_name)),
            source_type=session.source_type,
        )
        result = await session.execute_query_long_running(query)
        return str(result["RESULT"].iloc[0])  # type: ignore

    @staticmethod
    async def _merge_into_feature_table(
        session: BaseSession,
        feature_table_name: str,
        feature_table_serving_names: List[str],
        materialized_features: MaterializedFeatures,
        feature_timestamp_value: str,
        to_specify_merge_conditions: bool,
    ) -> None:
        feature_timestamp_value_expr = make_literal_value(
            feature_timestamp_value, cast_as_timestamp=True
        )
        if to_specify_merge_conditions:
            merge_conditions = [
                expressions.EQ(
                    this=get_qualified_column_identifier(serving_name, "offline_store_table"),
                    expression=get_qualified_column_identifier(
                        serving_name, "materialized_features"
                    ),
                )
                for serving_name in feature_table_serving_names
            ] + [
                expressions.EQ(
                    this=quoted_identifier(InternalName.FEATURE_TIMESTAMP_COLUMN),
                    expression=feature_timestamp_value_expr,
                ),
            ]
        else:
            merge_conditions = []

        update_expr = expressions.Update(
            expressions=[
                expressions.EQ(
                    this=get_qualified_column_identifier(column, "offline_store_table"),
                    expression=get_qualified_column_identifier(column, "materialized_features"),
                )
                for column in materialized_features.column_names
            ]
        )
        insert_expr = expressions.Insert(
            this=expressions.Tuple(
                expressions=[
                    quoted_identifier(col)
                    for col in [InternalName.FEATURE_TIMESTAMP_COLUMN.value]
                    + materialized_features.serving_names_and_column_names
                ]
            ),
            expression=expressions.Tuple(
                expressions=[feature_timestamp_value_expr]
                + [
                    get_qualified_column_identifier(col, "materialized_features")
                    for col in materialized_features.serving_names_and_column_names
                ]
            ),
        )
        merge_expr = expressions.Merge(
            this=expressions.Table(
                this=quoted_identifier(feature_table_name),
                alias=expressions.TableAlias(this="offline_store_table"),
            ),
            using=expressions.select(*[
                quoted_identifier(column)
                for column in materialized_features.serving_names_and_column_names
            ])
            .from_(quoted_identifier(materialized_features.materialized_table_name))
            .subquery(alias="materialized_features"),
            on=expressions.and_(*merge_conditions) if merge_conditions else expressions.false(),
            expressions=[
                expressions.When(matched=True, then=update_expr),
                expressions.When(matched=False, then=insert_expr),
            ],
        )
        query = sql_to_string(merge_expr, source_type=session.source_type)

        await session.execute_query_long_running(query)

    @classmethod
    async def _num_rows_in_feature_table(
        cls,
        session: BaseSession,
        feature_table_name: str,
    ) -> Optional[int]:
        try:
            query = sql_to_string(
                expressions.select(expressions.Count(this=expressions.Star())).from_(
                    quoted_identifier(feature_table_name)
                ),
                source_type=session.source_type,
            )
            result = await session.execute_query_long_running(query)
            return cast(int, result.iloc[0, 0])  # type: ignore[union-attr]
        except session._no_schema_error:
            return None

    @classmethod
    async def _get_column_names_not_in_warehouse(
        cls,
        session: BaseSession,
        feature_table_name: str,
        feature_table_column_names: List[str],
    ) -> List[str]:
        existing_columns = [
            c.upper()
            for c in (
                await session.list_table_schema(
                    feature_table_name, session.database_name, session.schema_name
                )
            ).keys()
        ]
        return [col for col in feature_table_column_names if col.upper() not in existing_columns]

    @classmethod
    async def _ensure_compatible_schema(
        cls,
        session: BaseSession,
        feature_table_name: str,
        feature_table_column_names: List[str],
        feature_table_column_dtypes: List[DBVarType],
    ) -> List[str]:
        assert len(feature_table_column_names) == len(feature_table_column_dtypes)

        new_columns = await cls._get_column_names_not_in_warehouse(
            session=session,
            feature_table_name=feature_table_name,
            feature_table_column_names=feature_table_column_names,
        )

        if new_columns:
            adapter = session.adapter
            query = adapter.alter_table_add_columns(
                expressions.Table(this=quoted_identifier(feature_table_name)),
                cls._get_column_defs(
                    feature_table_column_names,
                    feature_table_column_dtypes,
                    new_columns,
                    adapter,
                ),
            )
            await session.execute_query_long_running(query)

        return new_columns

    @staticmethod
    def _get_column_defs(
        column_names: List[str],
        column_dtypes: List[DBVarType],
        columns: List[str],
        adapter: BaseAdapter,
    ) -> List[expressions.ColumnDef]:
        columns_and_types = {}
        for column_name, column_data_type in zip(column_names, column_dtypes):
            if column_name in columns:
                columns_and_types[column_name] = column_data_type

        return [
            expressions.ColumnDef(
                this=quoted_identifier(column_name),
                kind=adapter.get_physical_type_from_dtype(column_data_type),
            )
            for column_name, column_data_type in columns_and_types.items()
        ]

    async def _catalog_needs_offline_store_tables(
        self, feature_table_model: OfflineStoreFeatureTableModel
    ) -> bool:
        """
        Check if the catalog requires offline store feature tables to be populated.

        Parameters
        ----------
        feature_table_model: OfflineStoreFeatureTableModel
            OfflineStoreFeatureTableModel object

        Returns
        -------
        bool
            True if offline store feature tables need to be populated, False otherwise.
        """
        catalog_model = await self.catalog_service.get_document(feature_table_model.catalog_id)
        return (
            catalog_model.populate_offline_feature_tables is True
            or catalog_model.online_store_id is not None
        )
