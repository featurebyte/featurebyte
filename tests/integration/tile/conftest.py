"""
Tile Tests for Spark Session
"""
from datetime import datetime

import pytest_asyncio
from bson import ObjectId

from featurebyte.app import get_celery
from featurebyte.enum import InternalName
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.models.base import DEFAULT_CATALOG_ID, User
from featurebyte.models.feature import FeatureReadiness
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.service.task_manager import TaskManager
from featurebyte.sql.base import BaselSqlModel
from featurebyte.tile.manager import TileManager


@pytest_asyncio.fixture(name="tile_task_prep_spark")
async def tile_task_online_store_prep(session, base_sql_model):
    entity_col_names = "__FB_TILE_START_DATE_COLUMN,PRODUCT_ACTION,CUST_ID"
    feature_name = "feature_1"
    feature_store_table_name = "fs_table_1"

    table_name = "TEMP_TABLE"
    suffix = datetime.now().strftime("%Y%m%d%H%M%S_%f")
    tile_id = f"TEMP_TABLE_{suffix}"
    aggregation_id = f"some_agg_id_{suffix}"

    number_records = 2
    quote_feature_name = base_sql_model.quote_column(feature_name)
    adapter = get_sql_adapter(session.source_type)
    sql_query = adapter.escape_quote_char(
        f"""
        select
          {entity_col_names},
          '{feature_name}' as {InternalName.ONLINE_STORE_RESULT_NAME_COLUMN},
          {quote_feature_name} as {InternalName.ONLINE_STORE_VALUE_COLUMN}
        FROM (
          select {entity_col_names}, cast(value_2 as float) as {quote_feature_name} from {table_name} limit {number_records}
        )
        """
    )
    insert_mapping_sql = f"""
            insert into ONLINE_STORE_MAPPING(
                TILE_ID,
                AGGREGATION_ID,
                RESULT_ID,
                RESULT_TYPE,
                SQL_QUERY,
                ONLINE_STORE_TABLE_NAME,
                ENTITY_COLUMN_NAMES,
                IS_DELETED,
                CREATED_AT
            )
            values (
                '{tile_id}',
                '{aggregation_id}',
                '{feature_name}',
                'FLOAT',
                '{sql_query}',
                '{feature_store_table_name}',
                '{entity_col_names}',
                false,
                current_timestamp()
            )
    """
    await session.execute_query(insert_mapping_sql)

    sql = f"SELECT * FROM ONLINE_STORE_MAPPING WHERE TILE_ID = '{tile_id}'"
    result = await session.execute_query(sql)
    assert len(result) == 1
    assert result["TILE_ID"].iloc[0] == tile_id
    assert result["RESULT_ID"].iloc[0] == feature_name

    yield tile_id, aggregation_id, feature_store_table_name, feature_name, entity_col_names

    await session.execute_query("DELETE FROM ONLINE_STORE_MAPPING")
    await session.execute_query(f"DROP TABLE IF EXISTS {feature_store_table_name}")


@pytest_asyncio.fixture(name="feature")
async def mock_feature_fixture(feature_model_dict, feature_store):
    """
    Fixture for a ExtendedFeatureModel object
    """

    feature_model_dict.update(
        {
            "user_id": None,
            "tabular_source": {
                "feature_store_id": feature_store.id,
                "table_details": TableDetails(table_name="some_random_table"),
            },
            "version": "v1",
            "readiness": FeatureReadiness.DRAFT,
            "online_enabled": False,
            "table_ids": [
                ObjectId("626bccb9697a12204fb22ea3"),
                ObjectId("726bccb9697a12204fb22ea3"),
            ],
        }
    )
    feature = ExtendedFeatureModel(**feature_model_dict)

    yield feature


@pytest_asyncio.fixture(name="tile_manager")
async def tile_manager_fixture(session, tile_spec, feature, persistent):
    task_manager = TaskManager(
        user=User(id=feature.user_id),
        persistent=persistent,
        celery=get_celery(),
        catalog_id=DEFAULT_CATALOG_ID,
    )
    yield TileManager(session=session, task_manager=task_manager)

    await session.execute_query(f"DROP TABLE IF EXISTS {tile_spec.aggregation_id}_ENTITY_TRACKER")


@pytest_asyncio.fixture(name="base_sql_model")
async def base_sql_model(session):
    return BaselSqlModel(session)
