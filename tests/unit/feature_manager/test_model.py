import textwrap

from featurebyte.common.model_util import get_version
from featurebyte.feature_manager.model import ExtendedFeatureModel, TileSpec
from featurebyte.models.base import VersionIdentifier


def test_extended_feature_model__float_feature(float_feature, snowflake_feature_store):
    """Test ExtendedFeatureModel has correct tile_specs"""
    model = ExtendedFeatureModel(
        **float_feature.dict(exclude={"version": True}),
        version=VersionIdentifier(name=get_version()),
    )
    aggregation_id = "e8c51d7d1ec78e1f35195fc0cf61221b3f830295"
    expected_sql = textwrap.dedent(
        f"""
        SELECT
          index,
          "cust_id",
          SUM("col_float") AS value_sum_{aggregation_id}
        FROM (
          SELECT
            *,
            F_TIMESTAMP_TO_INDEX(CONVERT_TIMEZONE('UTC', "event_timestamp"), 300, 600, 30) AS index
          FROM (
            SELECT
              *
            FROM (
              SELECT
                "col_int" AS "col_int",
                "col_float" AS "col_float",
                "col_char" AS "col_char",
                "col_text" AS "col_text",
                "col_binary" AS "col_binary",
                "col_boolean" AS "col_boolean",
                "event_timestamp" AS "event_timestamp",
                "cust_id" AS "cust_id"
              FROM "sf_database"."sf_schema"."sf_table"
              WHERE
                "event_timestamp" >= CAST(__FB_START_DATE AS TIMESTAMPNTZ)
                AND "event_timestamp" < CAST(__FB_END_DATE AS TIMESTAMPNTZ)
            )
            WHERE
              "event_timestamp" >= CAST(__FB_START_DATE AS TIMESTAMPNTZ)
              AND "event_timestamp" < CAST(__FB_END_DATE AS TIMESTAMPNTZ)
          )
        )
        GROUP BY
          index,
          "cust_id"
        """
    ).strip()
    expected_tile_specs = [
        TileSpec(
            time_modulo_frequency_second=300,
            blind_spot_second=600,
            frequency_minute=30,
            tile_sql=expected_sql,
            entity_column_names=["cust_id"],
            value_column_names=[f"value_sum_{aggregation_id}"],
            value_column_types=["FLOAT"],
            tile_id="TILE_F1800_M300_B600_8A209743FE8C9AD59ED6A9FE5E98977AB9A040DB",
            aggregation_id=f"sum_{aggregation_id}",
            feature_store_id=snowflake_feature_store.id,
            parent_column_name="col_float",
            aggregation_function_name="sum",
        )
    ]
    assert model.tile_specs == expected_tile_specs


def test_extended_feature_model__agg_per_category_feature(
    agg_per_category_feature, snowflake_feature_store
):
    """Test ExtendedFeatureModel has correct tile_specs for category groupby feature"""
    model = ExtendedFeatureModel(
        **agg_per_category_feature.dict(exclude={"version": True}),
        version=VersionIdentifier(name=get_version()),
    )
    aggregation_id = "254bde514925221168a524ba7467c9b6ef83685d"
    expected_sql = textwrap.dedent(
        f"""
        SELECT
          index,
          "cust_id",
          "col_int",
          SUM("col_float") AS value_sum_{aggregation_id}
        FROM (
          SELECT
            *,
            F_TIMESTAMP_TO_INDEX(CONVERT_TIMEZONE('UTC', "event_timestamp"), 300, 600, 30) AS index
          FROM (
            SELECT
              *
            FROM (
              SELECT
                "col_int" AS "col_int",
                "col_float" AS "col_float",
                "col_char" AS "col_char",
                "col_text" AS "col_text",
                "col_binary" AS "col_binary",
                "col_boolean" AS "col_boolean",
                "event_timestamp" AS "event_timestamp",
                "cust_id" AS "cust_id"
              FROM "sf_database"."sf_schema"."sf_table"
              WHERE
                "event_timestamp" >= CAST(__FB_START_DATE AS TIMESTAMPNTZ)
                AND "event_timestamp" < CAST(__FB_END_DATE AS TIMESTAMPNTZ)
            )
            WHERE
              "event_timestamp" >= CAST(__FB_START_DATE AS TIMESTAMPNTZ)
              AND "event_timestamp" < CAST(__FB_END_DATE AS TIMESTAMPNTZ)
          )
        )
        GROUP BY
          index,
          "cust_id",
          "col_int"
        """
    ).strip()
    expected_tile_specs = [
        TileSpec(
            time_modulo_frequency_second=300,
            blind_spot_second=600,
            frequency_minute=30,
            tile_sql=expected_sql,
            entity_column_names=["cust_id", "col_int"],
            value_column_names=[f"value_sum_{aggregation_id}"],
            value_column_types=["FLOAT"],
            tile_id="TILE_F1800_M300_B600_CFA168FB306C25C7A0F11C47778B85C5A0C0DD36",
            aggregation_id=f"sum_{aggregation_id}",
            category_column_name="col_int",
            feature_store_id=snowflake_feature_store.id,
            parent_column_name="col_float",
            aggregation_function_name="sum",
        )
    ]
    assert model.tile_specs == expected_tile_specs
