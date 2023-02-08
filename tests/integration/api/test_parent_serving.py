import pandas as pd
import pytest

from featurebyte import DimensionData, DimensionView, Entity, EventData, SlowlyChangingData


@pytest.mark.asyncio
async def test_serving_parent_features_multiple_joins(snowflake_session, snowflake_feature_store):
    """
    Test serving parent features requiring multiple joins with different types of data
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
            "effective_ts": pd.to_datetime(["2022-04-12 10:00:00", "2022-04-20 10:00:00"]),
            "scd_cust_id": [1000, 1000],
            "scd_city": ["paris", "tokyo"],
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

    df = feature.preview(
        {
            "POINT_IN_TIME": "2022-05-01 10:00:00",
            "serving_event_id": 1,
        }
    )
    expected = {
        "POINT_IN_TIME": pd.Timestamp("2022-05-01 10:00:00"),
        "serving_event_id": 1,
        "Country Name": "japan",
    }
    assert df.iloc[0].to_dict() == expected
