import textwrap

from featurebyte.common.model_util import get_version
from featurebyte.feature_manager.model import ExtendedFeatureModel, TileSpec
from featurebyte.models.base import VersionIdentifier


def test_extended_feature_model__float_feature(float_feature):
    """Test ExtendedFeatureModel has correct tile_specs"""
    model = ExtendedFeatureModel(
        **float_feature.dict(exclude={"version": True}),
        version=VersionIdentifier(name=get_version()),
    )
    aggregation_id = "a1a9657e29a711c4d09475bb8285da86250d2294"
    expected_sql = textwrap.dedent(
        f"""
        SELECT
          TO_TIMESTAMP(DATE_PART(EPOCH_SECOND, CAST(__FB_START_DATE AS TIMESTAMP)) + tile_index * 1800) AS __FB_TILE_START_DATE_COLUMN,
          "cust_id",
          SUM("col_float") AS value_sum_{aggregation_id}
        FROM (
          SELECT
            *,
            FLOOR(
              (
                DATE_PART(EPOCH_SECOND, "event_timestamp") - DATE_PART(EPOCH_SECOND, CAST(__FB_START_DATE AS TIMESTAMP))
              ) / 1800
            ) AS tile_index
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
                "created_at" AS "created_at",
                "cust_id" AS "cust_id"
              FROM "sf_database"."sf_schema"."sf_table"
            )
            WHERE
              "event_timestamp" >= CAST(__FB_START_DATE AS TIMESTAMP)
              AND "event_timestamp" < CAST(__FB_END_DATE AS TIMESTAMP)
          )
        )
        GROUP BY
          tile_index,
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
            tile_id="sf_table_f1800_m300_b600_f3822df3690ac033f56672194a2f224586d0a5bd",
            aggregation_id=f"sum_{aggregation_id}",
        )
    ]
    assert model.tile_specs == expected_tile_specs


def test_extended_feature_model__agg_per_category_feature(agg_per_category_feature):
    """Test ExtendedFeatureModel has correct tile_specs for category groupby feature"""
    model = ExtendedFeatureModel(
        **agg_per_category_feature.dict(exclude={"version": True}),
        version=VersionIdentifier(name=get_version()),
    )
    aggregation_id = "e7529955d0c7f03bff408e805f3ea3799fc01042"
    expected_sql = textwrap.dedent(
        f"""
        SELECT
          TO_TIMESTAMP(DATE_PART(EPOCH_SECOND, CAST(__FB_START_DATE AS TIMESTAMP)) + tile_index * 1800) AS __FB_TILE_START_DATE_COLUMN,
          "cust_id",
          "col_int",
          SUM("col_float") AS value_sum_{aggregation_id}
        FROM (
          SELECT
            *,
            FLOOR(
              (
                DATE_PART(EPOCH_SECOND, "event_timestamp") - DATE_PART(EPOCH_SECOND, CAST(__FB_START_DATE AS TIMESTAMP))
              ) / 1800
            ) AS tile_index
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
                "created_at" AS "created_at",
                "cust_id" AS "cust_id"
              FROM "sf_database"."sf_schema"."sf_table"
            )
            WHERE
              "event_timestamp" >= CAST(__FB_START_DATE AS TIMESTAMP)
              AND "event_timestamp" < CAST(__FB_END_DATE AS TIMESTAMP)
          )
        )
        GROUP BY
          tile_index,
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
            tile_id="sf_table_f1800_m300_b600_24dd90a763a6368c29540f4d8151f25da40456f3",
            aggregation_id=f"sum_{aggregation_id}",
            category_column_name="col_int",
        )
    ]
    assert model.tile_specs == expected_tile_specs
