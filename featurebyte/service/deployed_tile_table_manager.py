"""
DeployedTileTableManagerService class
"""

from __future__ import annotations

from bson import ObjectId

from featurebyte.logging import get_logger
from featurebyte.models import FeatureModel
from featurebyte.models.deployed_tile_table import (
    DeployedTileTableModel,
    DeployedTileTableUpdate,
    TileIdentifier,
)
from featurebyte.query_graph.sql.interpreter import GraphInterpreter
from featurebyte.query_graph.sql.source_info import SourceInfo
from featurebyte.query_graph.sql.tile_compute_combine import combine_tile_compute_specs
from featurebyte.service.deployed_tile_table import DeployedTileTableService
from featurebyte.service.feature import FeatureService

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
    ) -> None:
        self.deployed_tile_table_service = deployed_tile_table_service
        self.feature_service = feature_service

    async def handle_online_enabled_features(
        self, features: list[FeatureModel], source_info: SourceInfo
    ) -> None:
        """
        Handle online enabled features by creating deployed tile tables for the features.

        Parameters
        ----------
        features: list[FeatureModel]
            List of features to handle
        source_info: SourceInfo
            Source information for the features
        """

        all_aggregation_ids = set()
        for feature in features:
            all_aggregation_ids.update(feature.aggregation_ids)
        deployed_aggregation_ids = (
            await self.deployed_tile_table_service.get_deployed_aggregation_ids(
                aggregation_ids=all_aggregation_ids,
            )
        )

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
                if tile_info.aggregation_id not in unique_tile_infos:
                    unique_tile_infos[tile_info.aggregation_id] = tile_info

        if not unique_tile_infos:
            return

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
                table_name=table_name,
                tile_identifiers=tile_identifiers,
                tile_compute_query=tile_info.tile_compute_spec.get_tile_compute_query(),
                entity_column_names=tile_info.entity_columns,
                value_column_names=tile_info.tile_value_columns,
                tile_value_types=tile_info.tile_value_types,
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

        # Get the aggregation IDs of online disabled features
        online_disabled_feature_ids = await self.feature_service.get_online_disabled_feature_ids()
        aggregation_ids_to_remove = set()
        async for feature_doc in self.feature_service.list_documents_as_dict_iterator(
            query_filter={"_id": {"$in": online_disabled_feature_ids}},
            projection={"aggregation_ids": 1},
        ):
            aggregation_ids_to_remove.update(feature_doc.get("aggregation_ids", []))

        # Remove deployed tile tables that are no longer needed
        async for (
            deployed_tile_table
        ) in self.deployed_tile_table_service.list_deployed_tile_tables_by_aggregation_ids(
            aggregation_ids_to_remove,
        ):
            new_tile_identifiers = [
                tile_identifier
                for tile_identifier in deployed_tile_table.tile_identifiers
                if tile_identifier.aggregation_id not in aggregation_ids_to_remove
            ]
            if new_tile_identifiers:
                await self.deployed_tile_table_service.update_document(
                    deployed_tile_table.id,
                    DeployedTileTableUpdate(tile_identifiers=new_tile_identifiers),
                )
            else:
                await self.deployed_tile_table_service.delete_document(deployed_tile_table.id)
