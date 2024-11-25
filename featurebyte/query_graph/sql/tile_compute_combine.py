"""
Helpers for combining tile compute queries
"""

from __future__ import annotations

import copy
import os
from collections import defaultdict
from collections.abc import Hashable
from dataclasses import dataclass
from typing import Any, List, Optional, cast

from sqlglot import expressions

from featurebyte.query_graph.sql.common import sql_to_string
from featurebyte.query_graph.sql.interpreter import TileGenSql
from featurebyte.query_graph.sql.tile_compute_spec import TileComputeSpec, TileTableInputColumn

MAX_NUM_COLUMNS_PER_TILE_QUERY = int(os.getenv("MAX_NUM_COLUMNS_PER_TILE_QUERY", "500"))


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


def combine_tile_compute_specs(
    tile_infos: list[TileGenSql], max_num_columns_per_query: int = MAX_NUM_COLUMNS_PER_TILE_QUERY
) -> list[CombinedTileGenSql]:
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
    max_num_columns_per_query: int
        Maximum number of output columns per query

    Returns
    -------
    list[CombinedTileGenSql]
    """
    grouped_tile_infos = defaultdict(list)

    for tile_info in tile_infos:
        key = get_tile_compute_spec_signature(tile_info.tile_compute_spec)
        grouped_tile_infos[key].append(tile_info)

    out = []
    for all_infos in grouped_tile_infos.values():
        for tile_infos_to_combine in _split_if_too_many_columns(
            all_infos, max_num_columns_per_query
        ):
            out.append(_get_combined_tile_infos(tile_infos_to_combine))

    return out


def get_tile_compute_spec_signature(tile_compute_spec: TileComputeSpec) -> Hashable:
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
    items: list[Any] = [
        tile_compute_spec.source_expr,
        tile_compute_spec.entity_table_expr,
        tile_compute_spec.timestamp_column,
        tile_compute_spec.key_columns,
        tile_compute_spec.value_by_column,
        tile_compute_spec.tile_index_expr,
        tile_compute_spec.is_order_dependent,
    ]

    source_type = tile_compute_spec.source_info.source_type

    def _to_serializable_and_hashable(item: Any) -> Hashable:
        if isinstance(item, expressions.Expression):
            return sql_to_string(item, source_type)
        elif isinstance(item, TileTableInputColumn):
            return item.get_signature(source_type)
        elif isinstance(item, list):
            return tuple(_to_serializable_and_hashable(i) for i in item)
        return cast(Hashable, item)

    signatures = []
    for _item in items:
        signatures.append(_to_serializable_and_hashable(_item))

    return tuple(signatures)


def _split_if_too_many_columns(
    tile_infos: list[TileGenSql], max_columns: int
) -> list[list[TileGenSql]]:
    """
    Cap the number of output columns before combining tile compute queries. If the limit is
    exceeded, split the group into smaller groups.

    Parameters
    ----------
    tile_infos: list[TileGenSql]
        List of tile compute queries
    max_columns: int
        Maximum number of columns before splitting

    Returns
    -------
    list[list[TileGenSql]]
    """
    output = []
    current_group = []
    current_column_count = 0
    for tile_info in tile_infos:
        current_group.append(tile_info)
        current_column_count += len(tile_info.tile_value_columns)
        if current_column_count >= max_columns:
            output.append(current_group)
            current_group = []
            current_column_count = 0
    if current_group:
        output.append(current_group)
    return output


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
