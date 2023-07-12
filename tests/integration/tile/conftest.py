"""
Tile Tests for Spark Session
"""
from datetime import datetime

import pytest_asyncio
from bson import ObjectId

from featurebyte.enum import InternalName
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.models.feature_namespace import FeatureReadiness
from featurebyte.models.online_store_compute_query import OnlineStoreComputeQueryModel
from featurebyte.models.online_store_table_version import OnlineStoreTableVersion
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.sql.base import BaseSqlModel


@pytest_asyncio.fixture(name="tile_task_prep_spark")
async def tile_task_online_store_prep(
    session,
    base_sql_model,
    online_store_table_version_service,
    online_store_compute_query_service,
    user,
):
    entity_col_names = "__FB_TILE_START_DATE_COLUMN,PRODUCT_ACTION,CUST_ID"
    feature_name = "feature_1"
    feature_store_table_name = "fs_table_1"

    table_name = "TEMP_TABLE"
    suffix = datetime.now().strftime("%Y%m%d%H%M%S_%f")
    tile_id = f"TEMP_TABLE_{suffix}"
    aggregation_id = f"some_agg_id_{suffix}"

    number_records = 2
    quote_feature_name = base_sql_model.quote_column(feature_name)
    sql_query = f"""
        select
          {entity_col_names},
          '{feature_name}' as {InternalName.ONLINE_STORE_RESULT_NAME_COLUMN},
          {quote_feature_name} as {InternalName.ONLINE_STORE_VALUE_COLUMN}
        FROM (
          select {entity_col_names}, cast(value_2 as float) as {quote_feature_name} from {table_name} limit {number_records}
        )
        """
    await online_store_compute_query_service.create_document(
        OnlineStoreComputeQueryModel(
            tile_id=tile_id,
            aggregation_id=aggregation_id,
            result_name=feature_name,
            result_type="FLOAT",
            sql=sql_query,
            table_name=feature_store_table_name,
            serving_names=entity_col_names.split(","),
        )
    )

    yield tile_id, aggregation_id, feature_store_table_name, feature_name, entity_col_names

    await online_store_compute_query_service.persistent.delete_many(
        collection_name=OnlineStoreComputeQueryModel.collection_name(),
        query_filter={},
        user_id=user.id,
    )
    await session.execute_query(f"DROP TABLE IF EXISTS {feature_store_table_name}")

    await online_store_table_version_service.persistent.delete_many(
        collection_name=OnlineStoreTableVersion.collection_name(),
        query_filter={"aggregation_result_name": feature_name},
        user_id=user.id,
    )


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


@pytest_asyncio.fixture(name="tile_manager_service")
async def tile_manager_service_fixture(
    session,
    app_container,
    tile_spec,
):
    yield app_container.tile_manager_service
    await session.execute_query(f"DROP TABLE IF EXISTS {tile_spec.aggregation_id}_ENTITY_TRACKER")


@pytest_asyncio.fixture(name="base_sql_model")
async def base_sql_model(session):
    return BaseSqlModel(session)
