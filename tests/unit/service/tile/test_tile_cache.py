"""
Tests for tile cache
"""

import pytest
from sqlglot import expressions

from featurebyte.query_graph.sql.interpreter import TileGenSql
from featurebyte.query_graph.sql.template import SqlExpressionTemplate
from featurebyte.tile.tile_cache import TileCacheStatus, TileInfoKey


def create_tile_gen_sql(aggregation_id):
    """
    Helper function to create a TileGenSql object
    """
    return TileGenSql(
        tile_table_id=aggregation_id,
        tile_id_version=2,
        aggregation_id=aggregation_id,
        sql_template=SqlExpressionTemplate(expressions.select("a")),
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
def tile_info_1():
    """
    Tile info 1 fixture
    """
    return create_tile_gen_sql("agg_id_1")


@pytest.fixture
def tile_info_2():
    """
    Tile info 2 fixture
    """
    return create_tile_gen_sql("agg_id_2")


@pytest.fixture
def tile_info_3():
    """
    Tile info 3 fixture
    """
    return create_tile_gen_sql("agg_id_3")


@pytest.fixture
def tile_info_4():
    """
    Tile info 4 fixture
    """
    return create_tile_gen_sql("agg_id_4")


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
