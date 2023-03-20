import numpy as np
import pandas as pd
import pytest
import pytest_asyncio

from featurebyte import Entity, FeatureList, Table
from featurebyte.schema.feature_list import FeatureListGetOnlineFeatures

table_prefix = "TEST_SERVING_PARENT_FEATURES"


@pytest_asyncio.fixture(name="tables", scope="session")
async def tables_fixture(session, data_source):
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
    await session.register_table(f"{table_prefix}_EVENT", df_events, temporary=False)
    await session.register_table(f"{table_prefix}_SCD", df_scd, temporary=False)
    await session.register_table(f"{table_prefix}_DIMENSION_1", df_dimension_1, temporary=False)
    await session.register_table(f"{table_prefix}_DIMENSION_2", df_dimension_2, temporary=False)

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

    event_source_table = data_source.get_table(
        table_name=f"{table_prefix}_EVENT",
        database_name=session.database_name,
        schema_name=session.schema_name,
    )
    event_table = event_source_table.create_event_table(
        name=f"{table_prefix}_event_table",
        event_id_column="event_id",
        event_timestamp_column="ts",
    )
    event_table.save()
    event_table["event_id"].as_entity(event_entity.name)
    event_table["cust_id"].as_entity(customer_entity.name)

    scd_source_table = data_source.get_table(
        table_name=f"{table_prefix}_SCD",
        database_name=session.database_name,
        schema_name=session.schema_name,
    )
    scd_table = scd_source_table.create_scd_table(
        name=f"{table_prefix}_scd_table",
        natural_key_column="scd_cust_id",
        effective_timestamp_column="effective_ts",
        surrogate_key_column="scd_cust_id",
    )
    scd_table.save()
    scd_table["scd_cust_id"].as_entity(customer_entity.name)
    scd_table["scd_city"].as_entity(city_entity.name)

    dimension_source_table_1 = data_source.get_table(
        table_name=f"{table_prefix}_DIMENSION_1",
        database_name=session.database_name,
        schema_name=session.schema_name,
    )
    dimension_table_1 = dimension_source_table_1.create_dimension_table(
        name=f"{table_prefix}_dimension_table_1",
        dimension_id_column="city",
    )
    dimension_table_1.save()
    dimension_table_1["city"].as_entity(city_entity.name)
    dimension_table_1["state"].as_entity(state_entity.name)

    dimension_source_table_2 = data_source.get_table(
        table_name=f"{table_prefix}_DIMENSION_2",
        database_name=session.database_name,
        schema_name=session.schema_name,
    )
    dimension_table_2 = dimension_source_table_2.create_dimension_table(
        name=f"{table_prefix}_dimension_table_2",
        dimension_id_column="state",
    )
    dimension_table_2.save()
    dimension_table_2["state"].as_entity(state_entity.name)
    dimension_table_2["country"].as_entity(country_entity.name)


@pytest.fixture(name="country_feature", scope="session")
def country_feature_fixture(tables):
    _ = tables
    view = Table.get(f"{table_prefix}_dimension_table_2").get_view()
    feature = view["country"].as_feature("Country Name")
    return feature


@pytest.fixture(name="city_feature", scope="session")
def city_feature_fixture(tables):
    _ = tables
    view = Table.get(f"{table_prefix}_scd_table").get_view()
    feature = view["scd_city"].as_feature("Customer City")
    return feature


@pytest.fixture(name="feature_list_with_child_entities", scope="module")
def feature_list_with_child_entities_fixture(country_feature):
    feature_list = FeatureList([country_feature], name=f"{table_prefix}_country_list")
    feature_list.save(conflict_resolution="retrieve")
    try:
        feature_list.deploy(enable=True, make_production_ready=True)
        yield feature_list
    finally:
        feature_list.deploy(enable=False)


@pytest.fixture(name="feature_list_with_parent_child_features", scope="module")
def feature_list_with_parent_child_features_fixture(country_feature, city_feature):
    feature_list = FeatureList(
        [city_feature, country_feature], name=f"{table_prefix}_city_country_list"
    )
    feature_list.save(conflict_resolution="retrieve")
    try:
        feature_list.deploy(enable=True, make_production_ready=True)
        yield feature_list
    finally:
        feature_list.deploy(enable=False)


@pytest.mark.asyncio
@pytest.mark.parametrize("source_type", ["snowflake", "spark"], indirect=True)
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
    Test serving parent features requiring multiple joins with different types of table
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
    df = feature.preview(pd.DataFrame([preview_params]))
    pd.testing.assert_series_equal(df[expected.index].iloc[0], expected, check_names=False)

    # Preview feature list
    df = feature_list_with_child_entities.preview(pd.DataFrame([preview_params]))
    pd.testing.assert_series_equal(df[expected.index].iloc[0], expected, check_names=False)


@pytest.fixture(name="observations_set_with_expected_features")
def observations_set_with_expected_features_fixture():
    observations_set_with_expected_features = pd.DataFrame(
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
    observations_set_with_expected_features["POINT_IN_TIME"] = pd.to_datetime(
        observations_set_with_expected_features["POINT_IN_TIME"]
    )
    observations_set_with_expected_features = observations_set_with_expected_features.sort_values(
        ["POINT_IN_TIME", "serving_event_id"]
    )
    return observations_set_with_expected_features


@pytest.mark.parametrize("source_type", ["snowflake", "spark"], indirect=True)
def test_historical_features(
    feature_list_with_child_entities,
    observations_set_with_expected_features,
):
    """
    Test get historical features
    """
    observations_set = observations_set_with_expected_features[
        ["POINT_IN_TIME", "serving_event_id"]
    ]
    df = feature_list_with_child_entities.get_historical_features(observations_set)
    df = df.sort_values(["POINT_IN_TIME", "serving_event_id"])
    pd.testing.assert_frame_equal(df, observations_set_with_expected_features, check_dtype=False)


@pytest.mark.parametrize("source_type", ["snowflake", "spark"], indirect=True)
def test_historical_features_with_serving_names_mapping(
    feature_list_with_child_entities,
    observations_set_with_expected_features,
):
    """
    Test get historical features with serving_names_mapping
    """
    observations_set_with_expected_features.rename(
        {"serving_event_id": "new_serving_event_id"}, axis=1, inplace=True
    )
    observations_set = observations_set_with_expected_features[
        ["POINT_IN_TIME", "new_serving_event_id"]
    ]
    df = feature_list_with_child_entities.get_historical_features(
        observations_set,
        serving_names_mapping={"serving_event_id": "new_serving_event_id"},
    )
    df = df.sort_values(["POINT_IN_TIME", "new_serving_event_id"])
    pd.testing.assert_frame_equal(df, observations_set_with_expected_features, check_dtype=False)


@pytest.mark.parametrize("source_type", ["snowflake", "spark"], indirect=True)
def test_online_features(config, feature_list_with_child_entities):
    """
    Test requesting online features
    """
    data = FeatureListGetOnlineFeatures(entity_serving_names=[{"serving_event_id": 1}])
    res = config.get_client().post(
        f"/feature_list/{str(feature_list_with_child_entities.id)}/online_features",
        json=data.json_dict(),
    )
    assert res.status_code == 200
    assert res.json() == {"features": [{"serving_event_id": 1, "Country Name": "japan"}]}


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
def test_feature_info_primary_entity(feature_list_with_parent_child_features):
    """
    Test that the primary_entity field is correctly populated in feature list info
    """
    info = feature_list_with_parent_child_features.info()
    assert info["entities"] == [
        {
            "name": "TEST_SERVING_PARENT_FEATURES_state",
            "serving_names": ["serving_state_id"],
            "catalog_name": "default",
        },
        {
            "name": "TEST_SERVING_PARENT_FEATURES_customer",
            "serving_names": ["serving_cust_id"],
            "catalog_name": "default",
        },
    ]
    assert info["primary_entity"] == [
        {
            "catalog_name": "default",
            "name": "TEST_SERVING_PARENT_FEATURES_customer",
            "serving_names": ["serving_cust_id"],
        },
    ]


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
def test_online_serving_code_uses_primary_entity(
    feature_list_with_parent_child_features, update_fixtures
):
    """
    Check that online serving code is based on primary entity
    """
    online_serving_code = feature_list_with_parent_child_features.get_online_serving_code("python")
    expected_signature = 'request_features([{"serving_cust_id": 1000}])'
    assert expected_signature in online_serving_code
