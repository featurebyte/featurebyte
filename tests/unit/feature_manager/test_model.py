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
    aggregation_id = "aed233b0e8a6e1c1e0d5427b126b03c949609481"
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
            tile_id="TILE_F1800_M300_B600_B5CAF33CCFEDA76C257EC2CB7F66C4AD22009B0F",
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
    aggregation_id = "f691705e069ebb67363cafadbea0942d51be4af7"
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
            tile_id="TILE_F1800_M300_B600_560AD973F72FB4645D798EBD83E4B0450B2A5C7B",
            aggregation_id=f"sum_{aggregation_id}",
            category_column_name="col_int",
        )
    ]
    assert model.tile_specs == expected_tile_specs
