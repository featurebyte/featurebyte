from featurebyte.common.model_util import get_version
from featurebyte.feature_manager.model import ExtendedFeatureModel, TileSpec
from featurebyte.models.base import VersionIdentifier
from tests.util.helper import assert_equal_with_expected_fixture


def test_extended_feature_model__float_feature(
    float_feature, snowflake_feature_store, update_fixtures
):
    """Test ExtendedFeatureModel has correct tile_specs"""
    model = ExtendedFeatureModel(
        **float_feature.model_dump(exclude={"version": True}),
        version=VersionIdentifier(name=get_version()),
    )
    aggregation_id = "e8c51d7d1ec78e1f35195fc0cf61221b3f830295"
    tile_compute_query = model.tile_specs[0].tile_compute_query
    assert_equal_with_expected_fixture(
        tile_compute_query.get_combined_query_expr().sql(pretty=True),
        "tests/fixtures/extended_feature_model/float_feature_tile_query.sql",
        update_fixtures,
    )
    expected_tile_specs = [
        TileSpec(
            time_modulo_frequency_second=300,
            blind_spot_second=600,
            frequency_minute=30,
            tile_sql=None,
            tile_compute_query=tile_compute_query,
            entity_column_names=["cust_id"],
            value_column_names=[f"value_sum_{aggregation_id}"],
            value_column_types=["FLOAT"],
            tile_id="TILE_SUM_E8C51D7D1EC78E1F35195FC0CF61221B3F830295",
            aggregation_id=f"sum_{aggregation_id}",
            feature_store_id=snowflake_feature_store.id,
            parent_column_name="col_float",
            aggregation_function_name="sum",
            windows=["30m", "2h", "1d"],
        )
    ]
    assert model.tile_specs == expected_tile_specs


def test_extended_feature_model__agg_per_category_feature(
    agg_per_category_feature, snowflake_feature_store, update_fixtures
):
    """Test ExtendedFeatureModel has correct tile_specs for category groupby feature"""
    model = ExtendedFeatureModel(
        **agg_per_category_feature.model_dump(exclude={"version": True}),
        version=VersionIdentifier(name=get_version()),
    )
    aggregation_id = "254bde514925221168a524ba7467c9b6ef83685d"
    tile_compute_query = model.tile_specs[0].tile_compute_query
    assert_equal_with_expected_fixture(
        tile_compute_query.get_combined_query_expr().sql(pretty=True),
        "tests/fixtures/extended_feature_model/agg_per_cadtegory_feature_tile_query.sql",
        update_fixtures,
    )
    expected_tile_specs = [
        TileSpec(
            time_modulo_frequency_second=300,
            blind_spot_second=600,
            frequency_minute=30,
            tile_sql=None,
            tile_compute_query=tile_compute_query,
            entity_column_names=["cust_id", "col_int"],
            value_column_names=[f"value_sum_{aggregation_id}"],
            value_column_types=["FLOAT"],
            tile_id="TILE_SUM_254BDE514925221168A524BA7467C9B6EF83685D",
            aggregation_id=f"sum_{aggregation_id}",
            category_column_name="col_int",
            feature_store_id=snowflake_feature_store.id,
            parent_column_name="col_float",
            aggregation_function_name="sum",
            windows=["30m", "2h", "1d"],
        )
    ]
    assert model.tile_specs == expected_tile_specs
