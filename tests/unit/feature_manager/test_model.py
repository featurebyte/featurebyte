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
    aggregation_id = "60e19c3e160be7db3a64f2a828c1c7929543abb4"
    expected_sql = textwrap.dedent(
        f"""
        SELECT
          TO_TIMESTAMP(
            DATE_PART(EPOCH_SECOND, CAST(__FB_START_DATE AS TIMESTAMPNTZ)) + tile_index * 1800
          ) AS __FB_TILE_START_DATE_COLUMN,
          "cust_id",
          SUM("col_float") AS value_sum_{aggregation_id}
        FROM (
          SELECT
            *,
            FLOOR(
              (
                DATE_PART(EPOCH_SECOND, "event_timestamp") - DATE_PART(EPOCH_SECOND, CAST(__FB_START_DATE AS TIMESTAMPNTZ))
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
                "cust_id" AS "cust_id"
              FROM "sf_database"."sf_schema"."sf_table"
            )
            WHERE
              "event_timestamp" >= CAST(__FB_START_DATE AS TIMESTAMPNTZ)
              AND "event_timestamp" < CAST(__FB_END_DATE AS TIMESTAMPNTZ)
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
            value_column_types=["FLOAT"],
            tile_id="TILE_F1800_M300_B600_99CB16A0CBF5645D5C2D1DEA5CA74D4BD1660817",
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
    aggregation_id = "16e702a8a8a05816db2e6d5f19c271d78c2f59eb"
    expected_sql = textwrap.dedent(
        f"""
        SELECT
          TO_TIMESTAMP(
            DATE_PART(EPOCH_SECOND, CAST(__FB_START_DATE AS TIMESTAMPNTZ)) + tile_index * 1800
          ) AS __FB_TILE_START_DATE_COLUMN,
          "cust_id",
          "col_int",
          SUM("col_float") AS value_sum_{aggregation_id}
        FROM (
          SELECT
            *,
            FLOOR(
              (
                DATE_PART(EPOCH_SECOND, "event_timestamp") - DATE_PART(EPOCH_SECOND, CAST(__FB_START_DATE AS TIMESTAMPNTZ))
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
                "cust_id" AS "cust_id"
              FROM "sf_database"."sf_schema"."sf_table"
            )
            WHERE
              "event_timestamp" >= CAST(__FB_START_DATE AS TIMESTAMPNTZ)
              AND "event_timestamp" < CAST(__FB_END_DATE AS TIMESTAMPNTZ)
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
            value_column_types=["FLOAT"],
            tile_id="TILE_F1800_M300_B600_540E212D8EA456624A317D8F757B679EC5F9B65A",
            aggregation_id=f"sum_{aggregation_id}",
            category_column_name="col_int",
        )
    ]
    assert model.tile_specs == expected_tile_specs
