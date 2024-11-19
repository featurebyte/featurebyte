"""
Tile cache related models
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from bson import ObjectId

from featurebyte.enum import InternalName
from featurebyte.models.tile import OnDemandTileSpec, TileSpec
from featurebyte.models.tile_compute_query import TileComputeQuery
from featurebyte.query_graph.sql.interpreter import TileGenSql
from featurebyte.query_graph.sql.tile_compute_combine import TileTableGrouping


@dataclass(frozen=True)
class TileInfoKey:
    """
    Represents a unique unit of work for tile cache check
    """

    aggregation_id: str
    tile_id_version: int

    @classmethod
    def from_tile_info(cls, tile_info: TileGenSql) -> TileInfoKey:
        """
        Get a tile key object from TileGenSql

        Parameters
        ----------
        tile_info: TileGenSql
            TileGenSql object

        Returns
        -------
        TileInfoKey
        """
        return cls(
            aggregation_id=tile_info.aggregation_id, tile_id_version=tile_info.tile_id_version
        )

    def get_entity_tracker_table_name(self) -> str:
        """
        Get entity tracker table name

        Returns
        -------
        str
        """
        aggregation_id, tile_id_version = self.aggregation_id, self.tile_id_version
        if tile_id_version == 1:
            return f"{aggregation_id}{InternalName.TILE_ENTITY_TRACKER_SUFFIX}".upper()
        return (
            f"{aggregation_id}_v{tile_id_version}{InternalName.TILE_ENTITY_TRACKER_SUFFIX}".upper()
        )

    def get_working_table_column_name(self) -> str:
        """
        Get the column name corresponding to this key in the tile cache working table

        This is transient, so we don't have to worry about backward compatibility.

        Returns
        -------
        str
        """
        return f"{self.aggregation_id}_v{self.tile_id_version}"


@dataclass
class OnDemandTileComputeRequest:
    """Information required to compute and update a single tile table"""

    tile_table_id: str
    aggregation_id: str
    tile_compute_query: TileComputeQuery
    tile_gen_info: TileGenSql
    tile_table_groupings: Optional[list[TileTableGrouping]]

    def to_tile_manager_input(self, feature_store_id: ObjectId) -> OnDemandTileSpec:
        """Returns a tuple required by FeatureListManager to compute tiles on-demand

        Parameters
        ----------
        feature_store_id: ObjectId
            Feature store id

        Returns
        -------
        OnDemandTileSpec
        """
        entity_column_names = self.tile_gen_info.entity_columns[:]
        if self.tile_gen_info.value_by_column is not None:
            entity_column_names.append(self.tile_gen_info.value_by_column)
        tile_spec = TileSpec(
            time_modulo_frequency_second=self.tile_gen_info.time_modulo_frequency,
            blind_spot_second=self.tile_gen_info.blind_spot,
            frequency_minute=self.tile_gen_info.frequency // 60,
            tile_compute_query=self.tile_compute_query,
            column_names=self.tile_gen_info.columns,
            entity_column_names=entity_column_names,
            value_column_names=self.tile_gen_info.tile_value_columns,
            value_column_types=self.tile_gen_info.tile_value_types,
            tile_id=self.tile_table_id,
            aggregation_id=self.aggregation_id,
            category_column_name=self.tile_gen_info.value_by_column,
            feature_store_id=feature_store_id,
            entity_tracker_table_name=self.tile_info_key.get_entity_tracker_table_name(),
            windows=self.tile_gen_info.windows,
        )
        return OnDemandTileSpec(
            tile_spec=tile_spec,
            tile_table_groupings=self.tile_table_groupings,
        )

    @property
    def tile_info_key(self) -> TileInfoKey:
        """
        Returns a TileInfoKey object to uniquely identify a unit of tile compute work

        Returns
        -------
        TileInfoKey
        """
        return TileInfoKey.from_tile_info(self.tile_gen_info)


@dataclass
class OnDemandTileComputeRequestSet:
    """
    Represents a set of tile compute requests

    compute_requests: list[OnDemandTileComputeRequest]
        List of tile compute requests
    materialized_temp_table_names: set[str]
        Set of materialized temp table names for cleanup after computations are done
    """

    compute_requests: list[OnDemandTileComputeRequest]
    materialized_temp_table_names: set[str]

    @classmethod
    def merge(
        cls, request_sets: List[OnDemandTileComputeRequestSet]
    ) -> OnDemandTileComputeRequestSet:
        """
        Merge multiple OnDemandTileComputeRequestSet objects

        Parameters
        ----------
        request_sets: OnDemandTileComputeRequestSet
            Another OnDemandTileComputeRequestSet object

        Returns
        -------
        OnDemandTileComputeRequestSet
        """
        compute_requests = []
        materialized_temp_table_names = set()
        for request_set in request_sets:
            compute_requests.extend(request_set.compute_requests)
            materialized_temp_table_names.update(request_set.materialized_temp_table_names)
        return cls(
            compute_requests=compute_requests,
            materialized_temp_table_names=materialized_temp_table_names,
        )
