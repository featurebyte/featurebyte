"""
DeployedTileTableManagerService class
"""

from __future__ import annotations

from typing import Optional

from bson import ObjectId

from featurebyte.logging import get_logger
from featurebyte.models import FeatureModel
from featurebyte.models.deployed_tile_table import (
    DeployedTileTableModel,
    TileIdentifier,
)
from featurebyte.query_graph.sql.interpreter import GraphInterpreter
from featurebyte.query_graph.sql.tile_compute_combine import combine_tile_compute_specs
from featurebyte.service.deployed_tile_table import DeployedTileTableService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.tile_manager import TileManagerService
from featurebyte.session.base import BaseSession

logger = get_logger(__name__)


class DeployedTileTableManagerService:
    """
    DeployedTileTableManager is responsible for managing deployed tile tables during deployment's
    enablement / disablement.
    """

    def __init__(
        self,
        deployed_tile_table_service: DeployedTileTableService,
        feature_service: FeatureService,
        feature_store_service: FeatureStoreService,
        tile_manager_service: TileManagerService,
        session_manager_service: SessionManagerService,
    ) -> None:
        self.deployed_tile_table_service = deployed_tile_table_service
        self.feature_service = feature_service
        self.feature_store_service = feature_store_service
        self.tile_manager_service = tile_manager_service
        self.session_manager_service = session_manager_service

    async def handle_online_enabled_features(self, features: list[FeatureModel]) -> None:
        """
        Handle online enabled features by creating deployed tile tables for the features.

        Parameters
        ----------
        features: list[FeatureModel]
            List of features to handle
        """
        if not features:
            return

        # Retrieve the aggregation ids that are already deployed in DeployedTileTable collection
        all_aggregation_ids = set()
        for feature in features:
            all_aggregation_ids.update(feature.aggregation_ids)
        deployed_aggregation_ids = (
            await self.deployed_tile_table_service.get_deployed_aggregation_ids(
                aggregation_ids=all_aggregation_ids,
            )
        )

        # Extract aggregation_ids that are not deployed yet
        feature_store_id = features[0].tabular_source.feature_store_id
        source_info = (
            await self.feature_store_service.get_document(document_id=feature_store_id)
        ).get_source_info()
        unique_tile_infos = {}
        for feature in features:
            pending_aggregation_ids = []
            for aggregation_id in feature.aggregation_ids:
                if aggregation_id not in deployed_aggregation_ids:
                    pending_aggregation_ids.append(aggregation_id)
            if not pending_aggregation_ids:
                continue
            interpreter = GraphInterpreter(feature.graph, source_info=source_info)
            for tile_info in interpreter.construct_tile_gen_sql(feature.node, is_on_demand=False):
                if (
                    tile_info.aggregation_id not in deployed_aggregation_ids
                    and tile_info.aggregation_id not in unique_tile_infos
                ):
                    unique_tile_infos[tile_info.aggregation_id] = tile_info

        if not unique_tile_infos:
            return

        # Combine tile compute queries that are compatible and create a DeployedTileTable for each
        # combined query. It will remain immutable after creation.
        combined_infos = combine_tile_compute_specs(list(unique_tile_infos.values()))

        logger.info(
            "Number of tile compute queries before and after combining for deployment: %s -> %s",
            len(unique_tile_infos),
            len(combined_infos),
        )

        for combined_info in combined_infos:
            table_name = f"__FB_DEPLOYED_TILE_TABLE_{ObjectId()}".upper()
            tile_identifiers = [
                TileIdentifier(
                    tile_id=tile_info.tile_id,
                    aggregation_id=tile_info.aggregation_id,
                )
                for tile_info in combined_info.tile_table_groupings
            ]
            tile_info = combined_info.tile_info
            deployed_tile_table = DeployedTileTableModel(
                feature_store_id=feature_store_id,
                table_name=table_name,
                tile_identifiers=tile_identifiers,
                tile_compute_query=tile_info.tile_compute_spec.get_tile_compute_query(),
                entity_column_names=tile_info.entity_columns,
                value_column_names=tile_info.tile_value_columns,
                value_column_types=tile_info.tile_value_types,
                value_by_column=tile_info.value_by_column,
                frequency_minute=tile_info.frequency // 60,
                time_modulo_frequency_second=tile_info.time_modulo_frequency,
                blind_spot_second=tile_info.blind_spot,
            )
            await self.deployed_tile_table_service.create_document(
                deployed_tile_table,
            )

    async def handle_online_disabled_features(self) -> None:
        """
        Handle online disabled features by removing deployed tile tables that are no longer needed.
        """

        # Get the aggregation IDs across all online enabled features
        online_enabled_feature_ids = await self.feature_service.get_online_enabled_feature_ids()
        required_aggregation_ids = set()
        async for feature_doc in self.feature_service.list_documents_as_dict_iterator(
            query_filter={"_id": {"$in": online_enabled_feature_ids}},
            projection={"aggregation_ids": 1},
        ):
            required_aggregation_ids.update(feature_doc.get("aggregation_ids", []))

        # Remove deployed tile tables that are no longer needed
        async for deployed_tile_table in self.deployed_tile_table_service.list_documents_iterator(
            query_filter={}
        ):
            new_tile_identifiers = [
                tile_identifier
                for tile_identifier in deployed_tile_table.tile_identifiers
                if tile_identifier.aggregation_id in required_aggregation_ids
            ]
            if not new_tile_identifiers:
                # Only undeploy a deployed tile table if all tile identifiers are not needed
                await self._remove_deployed_tile_table(deployed_tile_table)

    async def _remove_deployed_tile_table(
        self, deployed_tile_table: DeployedTileTableModel
    ) -> None:
        # Remove the deployed tile table document
        await self.deployed_tile_table_service.delete_document(deployed_tile_table.id)

        # Remove scheduled tile jobs
        await self.tile_manager_service.remove_deployed_tile_table_jobs(
            deployed_tile_table_id=deployed_tile_table.id,
        )

        # Drop the tile table from the feature store
        session = await self._get_feature_store_session(deployed_tile_table.feature_store_id)
        if session is not None:
            await session.drop_table(
                deployed_tile_table.table_name,
                schema_name=session.schema_name,
                database_name=session.database_name,
                if_exists=True,
            )

    async def _get_feature_store_session(self, feature_store_id: ObjectId) -> Optional[BaseSession]:
        feature_store = await self.feature_store_service.get_document(document_id=feature_store_id)
        try:
            session = await self.session_manager_service.get_feature_store_session(feature_store)
        except ValueError:
            logger.warning(
                "Feature store session could not be created, skipping tile table removal"
            )
            return None
        return session
