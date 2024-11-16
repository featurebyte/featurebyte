"""
Tests for tile cache
"""

import pytest
from sqlglot import expressions

from featurebyte.query_graph.sql.common import quoted_identifier
from featurebyte.query_graph.sql.interpreter import TileGenSql
from featurebyte.query_graph.sql.tile_compute_spec import TileComputeSpec, TileTableInputColumn
from featurebyte.service.tile_cache_query_by_entity import TileCacheStatus, TileInfoKey


def create_tile_gen_sql(aggregation_id, source_info):
    """
    Helper function to create a TileGenSql object
    """
    return TileGenSql(
        tile_table_id=aggregation_id,
        tile_id_version=2,
        aggregation_id=aggregation_id,
        tile_compute_spec=TileComputeSpec(
            source_expr=expressions.select().from_("some_table"),
            entity_table_expr=None,
            timestamp_column=TileTableInputColumn(
                name="timestamp", expr=quoted_identifier("timestamp")
            ),
            key_columns=[TileTableInputColumn(name="cust_id", expr=quoted_identifier("cust_id"))],
            value_by_column=None,
            value_columns=[],
            tile_index_expr=quoted_identifier("tile_index"),
            tile_column_specs=[],
            is_order_dependent=False,
            is_on_demand=False,
            source_info=source_info,
        ),
        columns=["a"],
        entity_columns=["a"],
        tile_value_columns=["a"],
        tile_value_types=["FLOAT"],
        time_modulo_frequency=0,
        frequency=3600,
        blind_spot=0,
        windows=["7d"],
        offset=None,
        serving_names=["a"],
        value_by_column=None,
        parent=None,
        agg_func=None,
    )


@pytest.fixture
def tile_info_1(source_info):
    """
    Tile info 1 fixture
    """
    return create_tile_gen_sql("agg_id_1", source_info)


@pytest.fixture
def tile_info_2(source_info):
    """
    Tile info 2 fixture
    """
    return create_tile_gen_sql("agg_id_2", source_info)


@pytest.fixture
def tile_info_3(source_info):
    """
    Tile info 3 fixture
    """
    return create_tile_gen_sql("agg_id_3", source_info)


@pytest.fixture
def tile_info_4(source_info):
    """
    Tile info 4 fixture
    """
    return create_tile_gen_sql("agg_id_4", source_info)


@pytest.fixture(name="tile_cache_status")
def tile_cache_status_fixture(tile_info_1, tile_info_2, tile_info_3, tile_info_4):
    """
    Fixture for a TileCacheStatus
    """
    return TileCacheStatus(
        unique_tile_infos={
            TileInfoKey.from_tile_info(tile_info_1): tile_info_1,
            TileInfoKey.from_tile_info(tile_info_2): tile_info_2,
            TileInfoKey.from_tile_info(tile_info_3): tile_info_3,
            TileInfoKey.from_tile_info(tile_info_4): tile_info_4,
        },
        keys_with_tracker=[TileInfoKey.from_tile_info(tile_info_1)],
    )


def test_tile_cache_status_split_batches(
    tile_cache_status, tile_info_1, tile_info_2, tile_info_3, tile_info_4
):
    """
    Test TileCacheStatus split_batches method
    """
    result = list(tile_cache_status.split_batches(batch_size=2))
    assert result == [
        TileCacheStatus(
            unique_tile_infos={
                TileInfoKey.from_tile_info(tile_info_1): tile_info_1,
                TileInfoKey.from_tile_info(tile_info_2): tile_info_2,
            },
            keys_with_tracker=[TileInfoKey.from_tile_info(tile_info_1)],
        ),
        TileCacheStatus(
            unique_tile_infos={
                TileInfoKey.from_tile_info(tile_info_3): tile_info_3,
                TileInfoKey.from_tile_info(tile_info_4): tile_info_4,
            },
            keys_with_tracker=[],
        ),
    ]
