"""
Test distance operations
"""

import os

import pandas as pd
import pytest
import pytest_asyncio

from featurebyte import Entity
from featurebyte.core.distance import haversine
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting


@pytest.fixture(name="distance_event_data", scope="module")
def event_data_with_array_fixture():
    """
    Simulated data with an array column
    """
    df = pd.read_csv(os.path.join(os.path.dirname(__file__), "fixtures", "distance.csv"))
    df["EVENT_TIMESTAMP"] = pd.to_datetime(df["EVENT_TIMESTAMP"].astype(str))
    yield df


@pytest.fixture(name="distance_user_entity", scope="module")
def distance_user_entity_fixture(catalog):
    """
    Fixture for an Entity "User"
    """
    _ = catalog
    entity = Entity(name="Distance User", serving_names=["distance_user_id"])
    entity.save()
    return entity


@pytest.fixture(name="distance_order_entity", scope="module")
def distance_order_entity_fixture(catalog):
    """
    Fixture for an Entity "User"
    """
    _ = catalog
    entity = Entity(name="Distance Order", serving_names=["distance_order_id"])
    entity.save()
    return entity


@pytest_asyncio.fixture(name="event_table_with_distances", scope="module")
async def register_table_with_array_column(
    distance_event_data, session, data_source, catalog, distance_user_entity, distance_order_entity
):
    """
    Register a table with distance columns
    """
    _ = catalog
    table_name = "event_table_with_distances"
    await session.register_table(table_name, distance_event_data, temporary=False)

    database_table = data_source.get_source_table(
        database_name=session.database_name,
        schema_name=session.schema_name,
        table_name=table_name,
    )
    event_table = database_table.create_event_table(
        name=table_name,
        event_id_column="ORDER_ID",
        event_timestamp_column="EVENT_TIMESTAMP",
    )
    event_table.update_default_feature_job_setting(
        feature_job_setting=FeatureJobSetting(
            blind_spot="30m", frequency="1h", time_modulo_frequency="30m"
        )
    )
    event_table["USER_ID"].as_entity(distance_user_entity.name)
    event_table["ORDER_ID"].as_entity(distance_order_entity.name)
    return event_table


def test_haversine_operation(event_table_with_distances):
    """
    Test haversine operations
    """
    event_view = event_table_with_distances.get_view()
    event_view["haversine"] = haversine(
        event_view["LAT_1"],
        event_view["LON_1"],
        event_view["LAT_2"],
        event_view["LON_2"],
    )
    feature_name = "haversine_feature"
    haversine_feature = event_view["haversine"].as_feature(feature_name=feature_name)
    preview_params = {"POINT_IN_TIME": "2022-06-06 00:58:00", "distance_order_id": "1000"}
    feature_preview = haversine_feature.preview(pd.DataFrame([preview_params]))
    assert feature_preview.shape[0] == 1
    feature_values = feature_preview.iloc[0].to_dict()
    assert feature_values[feature_name] == pytest.approx(3936.3850)
    assert feature_values["distance_order_id"] == "1000"
