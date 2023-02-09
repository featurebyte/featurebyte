import numpy as np
import pandas as pd
import pytest
import pytest_asyncio

from featurebyte import (
    DimensionData,
    DimensionView,
    Entity,
    EventData,
    FeatureList,
    SlowlyChangingData,
)


@pytest_asyncio.fixture(name="feature_list_with_child_entities", scope="session")
async def feature_list_with_child_entities_fixture(snowflake_session, snowflake_feature_store):
    """
    Fixture for a feature that can be obtained from a child entity using one or more joins
    """
    df_events = pd.DataFrame(
        {
            "ts": pd.to_datetime(
                [
                    "2022-04-10 10:00:00",
                    "2022-04-15 10:00:00",
                    "2022-04-20 10:00:00",
                ]
            ),
            "cust_id": [1000, 1000, 1000],
            "event_id": [1, 2, 3],
        }
    )
    df_scd = pd.DataFrame(
        {
            "effective_ts": pd.to_datetime(
                ["2020-01-01 10:00:00", "2022-04-12 10:00:00", "2022-04-20 10:00:00"]
            ),
            "scd_cust_id": [1000, 1000, 1000],
            "scd_city": ["tokyo", "paris", "tokyo"],
        }
    )
    df_dimension_1 = pd.DataFrame(
        {
            "city": ["paris", "tokyo"],
            "state": ["île-de-france", "kanto"],
        }
    )
    df_dimension_2 = pd.DataFrame(
        {
            "state": ["île-de-france", "kanto"],
            "country": ["france", "japan"],
        }
    )
    table_prefix = "TEST_SERVING_PARENT_FEATURES"
    await snowflake_session.register_table(f"{table_prefix}_EVENT", df_events, temporary=False)
    await snowflake_session.register_table(f"{table_prefix}_SCD", df_scd, temporary=False)
    await snowflake_session.register_table(
        f"{table_prefix}_DIMENSION_1", df_dimension_1, temporary=False
    )
    await snowflake_session.register_table(
        f"{table_prefix}_DIMENSION_2", df_dimension_2, temporary=False
    )

    event_entity = Entity(name=f"{table_prefix}_event", serving_names=["serving_event_id"])
    event_entity.save()
    customer_entity = Entity(name=f"{table_prefix}_customer", serving_names=["serving_cust_id"])
    customer_entity.save()
    city_entity = Entity(name=f"{table_prefix}_city", serving_names=["serving_city_id"])
    city_entity.save()
    state_entity = Entity(name=f"{table_prefix}_state", serving_names=["serving_state_id"])
    state_entity.save()
    country_entity = Entity(name=f"{table_prefix}_country", serving_names=["country_id"])
    country_entity.save()

    event_data = EventData.from_tabular_source(
        tabular_source=snowflake_feature_store.get_table(
            table_name=f"{table_prefix}_EVENT",
            database_name=snowflake_session.database,
            schema_name=snowflake_session.sf_schema,
        ),
        name=f"{table_prefix}_event_data",
        event_id_column="event_id",
        event_timestamp_column="ts",
    )
    event_data.save()
    event_data["event_id"].as_entity(event_entity.name)
    event_data["cust_id"].as_entity(customer_entity.name)

    scd_data = SlowlyChangingData.from_tabular_source(
        tabular_source=snowflake_feature_store.get_table(
            table_name=f"{table_prefix}_SCD",
            database_name=snowflake_session.database,
            schema_name=snowflake_session.sf_schema,
        ),
        name=f"{table_prefix}_scd_data",
        natural_key_column="scd_cust_id",
        effective_timestamp_column="effective_ts",
        surrogate_key_column="scd_cust_id",
    )
    scd_data.save()
    scd_data["scd_cust_id"].as_entity(customer_entity.name)
    scd_data["scd_city"].as_entity(city_entity.name)

    dimension_data_1 = DimensionData.from_tabular_source(
        tabular_source=snowflake_feature_store.get_table(
            table_name=f"{table_prefix}_DIMENSION_1",
            database_name=snowflake_session.database,
            schema_name=snowflake_session.sf_schema,
        ),
        name=f"{table_prefix}_dimension_data_1",
        dimension_id_column="city",
    )
    dimension_data_1.save()
    dimension_data_1["city"].as_entity(city_entity.name)
    dimension_data_1["state"].as_entity(state_entity.name)

    dimension_data_2 = DimensionData.from_tabular_source(
        tabular_source=snowflake_feature_store.get_table(
            table_name=f"{table_prefix}_DIMENSION_2",
            database_name=snowflake_session.database,
            schema_name=snowflake_session.sf_schema,
        ),
        name=f"{table_prefix}_dimension_data_2",
        dimension_id_column="state",
    )
    dimension_data_2.save()
    dimension_data_2["state"].as_entity(state_entity.name)
    dimension_data_2["country"].as_entity(country_entity.name)

    dimension_view = DimensionView.from_dimension_data(dimension_data_2)
    feature = dimension_view["country"].as_feature("Country Name")

    feature_list = FeatureList([feature], name=f"{table_prefix}_feature_list")
    return feature_list


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "point_in_time, provided_entity, expected",
    [
        # event_id: 1 -> cust_id: 1000 -> city: tokyo -> state: kanto -> country: japan
        ("2022-05-01 10:00:00", {"serving_event_id": 1}, "japan"),
        # event_id: 1 -> cust_id: 1000 -> city: paris -> state: île-de-france -> country: france
        ("2022-04-16 10:00:00", {"serving_event_id": 1}, "france"),
        # nan because point in time is prior to the timestamp of event_id 1
        ("2022-01-01 10:00:00", {"serving_event_id": 1}, np.nan),
    ],
)
def test_preview(feature_list_with_child_entities, point_in_time, provided_entity, expected):
    """
    Test serving parent features requiring multiple joins with different types of data
    """
    preview_params = {"POINT_IN_TIME": point_in_time, **provided_entity}
    expected = pd.Series(
        {
            "POINT_IN_TIME": pd.Timestamp(point_in_time),
            "Country Name": expected,
            **provided_entity,
        }
    )

    # Preview feature
    feature = feature_list_with_child_entities["Country Name"]
    df = feature.preview(preview_params)
    pd.testing.assert_series_equal(df[expected.index].iloc[0], expected, check_names=False)

    # Preview feature list
    df = feature_list_with_child_entities.preview(preview_params)
    pd.testing.assert_series_equal(df[expected.index].iloc[0], expected, check_names=False)


def test_historical_features(feature_list_with_child_entities):
    """
    Test get historical features
    """
    observations_set_with_expected_feature = pd.DataFrame(
        [
            {"POINT_IN_TIME": "2022-01-01 10:00:00", "serving_event_id": 1, "Country Name": np.nan},
            {
                "POINT_IN_TIME": "2022-04-16 10:00:00",
                "serving_event_id": 1,
                "Country Name": "france",
            },
            {
                "POINT_IN_TIME": "2022-05-01 10:00:00",
                "serving_event_id": 1,
                "Country Name": "japan",
            },
        ]
    )

    observations_set = observations_set_with_expected_feature[["POINT_IN_TIME", "serving_event_id"]]
    df = feature_list_with_child_entities.get_historical_features(observations_set)

    df = df.sort_values(["POINT_IN_TIME", "serving_event_id"])
    observations_set_with_expected_feature = observations_set_with_expected_feature.sort_values(
        ["POINT_IN_TIME", "serving_event_id"]
    )
    observations_set_with_expected_feature["POINT_IN_TIME"] = pd.to_datetime(
        observations_set_with_expected_feature["POINT_IN_TIME"]
    )
    pd.testing.assert_frame_equal(df, observations_set_with_expected_feature)
