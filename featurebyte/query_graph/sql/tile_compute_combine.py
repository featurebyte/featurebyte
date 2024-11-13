"""
Helpers for combining tile compute queries
"""

from __future__ import annotations

import copy
from collections import defaultdict
from collections.abc import Hashable
from dataclasses import dataclass
from typing import Any, List, Optional

from featurebyte.query_graph.sql.interpreter import TileGenSql
from featurebyte.query_graph.sql.tile_compute_spec import TileComputeSpec


@dataclass
class TileTableGrouping:
    """
    Model for TileTableGrouping
    """

    value_column_names: List[str]
    value_column_types: List[str]
    tile_id: str
    aggregation_id: str


@dataclass
class CombinedTileGenSql:
    """
    Represents a combined tile info
    """

    tile_info: TileGenSql
    tile_table_groupings: list[TileTableGrouping]


def combine_tile_compute_specs(tile_infos: list[TileGenSql]) -> list[CombinedTileGenSql]:
    """
    Combine compatible tile compute queries into single queries. Takes a list of TileGenSql objects,
    each representing a tile compute query, and merges compatible tile queries into one. Two tile
    queries are compatible if they only differ in the produced aggregated tile columns, while all
    other parameters (e.g., source expression, groupby keys, etc.) remain the same.

    Each item in the output list is a CombinedTileGenSql object with the following fields:

    * tile_info: A new TileGenSql object representing the combined tile query. This is used to
      create a TileGenerate object and run compute_tiles() to generate a temporary tile table
      containing multiple columns.

    * tile_table_groupings: A list of TileTableGrouping objects, used to determine which columns
      in the resulting table should go to which actual tile table.

    Parameters
    ----------
    tile_infos: list[TileGenSql]
        List of tile compute queries

    Returns
    -------
    list[CombinedTileGenSql]
    """
    grouped_tile_infos = defaultdict(list)

    for tile_info in tile_infos:
        key = _get_key(tile_info.tile_compute_spec)
        grouped_tile_infos[key].append(tile_info)

    out = []
    for tile_infos_to_combine in grouped_tile_infos.values():
        out.append(_get_combined_tile_infos(tile_infos_to_combine))

    return out


def _get_key(tile_compute_spec: TileComputeSpec) -> Hashable:
    """
    Get a signature of the tile compute spec for combination

    Parameters
    ----------
    tile_compute_spec: TileComputeSpec
        Tile compute spec that fully describes a tile compute query

    Returns
    -------
    Hashable
    """
    signatures: list[Any] = [
        tile_compute_spec.source_expr,
        tile_compute_spec.entity_table_expr,
        tile_compute_spec.timestamp_column,
        tuple(tile_compute_spec.key_columns),
        tile_compute_spec.value_by_column,
        tile_compute_spec.tile_index_expr,
        tile_compute_spec.is_order_dependent,
    ]
    return tuple(signatures)


def _get_combined_tile_infos(tile_infos: list[TileGenSql]) -> CombinedTileGenSql:
    """
    Combines a list of TileGenSql objects into a single CombinedTileGenSql object.

    Parameters
    ----------
    tile_infos : list[TileGenSql]
        List of tile compute queries that can be merged based on compatible parameters.

    Returns
    -------
    CombinedTileGenSql
    """
    final_tile_info: Optional[TileGenSql] = None
    tile_table_groupings = []
    for tile_info in tile_infos:
        if final_tile_info is None:
            final_tile_info = copy.deepcopy(tile_info)
        else:
            final_tile_info.tile_value_columns.extend(tile_info.tile_value_columns)
            final_tile_info.tile_value_types.extend(tile_info.tile_value_types)
            final_tile_info.tile_compute_spec.value_columns.extend(
                tile_info.tile_compute_spec.value_columns
            )
            final_tile_info.tile_compute_spec.tile_column_specs.extend(
                tile_info.tile_compute_spec.tile_column_specs
            )
        tile_table_groupings.append(
            TileTableGrouping(
                value_column_names=tile_info.tile_value_columns,
                value_column_types=tile_info.tile_value_types,
                tile_id=tile_info.tile_table_id,
                aggregation_id=tile_info.aggregation_id,
            )
        )
    assert final_tile_info is not None
    return CombinedTileGenSql(tile_info=final_tile_info, tile_table_groupings=tile_table_groupings)
