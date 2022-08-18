import textwrap

from featurebyte.feature_manager.model import ExtendedFeatureModel, TileSpec


def test_extended_feature_model__float_feature(float_feature, snowflake_feature_store):
    """Test ExtendedFeatureModel has correct tile_specs"""
    model = ExtendedFeatureModel(**float_feature.dict(), feature_store=snowflake_feature_store)
    expected_sql = textwrap.dedent(
        """
        SELECT
          TO_TIMESTAMP(DATE_PART(EPOCH_SECOND, CAST(__FB_START_DATE AS TIMESTAMP)) + tile_index * 1800) AS __FB_TILE_START_DATE_COLUMN,
          "cust_id",
          SUM("col_float") AS value_sum_afb4d56e30a685ee9128bfa58fe4ad76d32af512
        FROM (
            SELECT
              *,
              FLOOR((DATE_PART(EPOCH_SECOND, "event_timestamp") - DATE_PART(EPOCH_SECOND, CAST(__FB_START_DATE AS TIMESTAMP))) / 1800) AS tile_index
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
                WHERE
                  "event_timestamp" >= CAST(__FB_START_DATE AS TIMESTAMP)
                  AND "event_timestamp" < CAST(__FB_END_DATE AS TIMESTAMP)
            )
        )
        GROUP BY
          tile_index,
          "cust_id"
        ORDER BY
          tile_index
        """
    ).strip()
    expected_tile_specs = [
        TileSpec(
            time_modulo_frequency_second=300,
            blind_spot_second=600,
            frequency_minute=30,
            tile_sql=expected_sql,
            entity_column_names=["cust_id"],
            value_column_names=["value_sum_afb4d56e30a685ee9128bfa58fe4ad76d32af512"],
            tile_id="sf_table_f1800_m300_b600_f3822df3690ac033f56672194a2f224586d0a5bd",
            aggregation_id="sum_afb4d56e30a685ee9128bfa58fe4ad76d32af512",
        )
    ]
    assert model.tile_specs == expected_tile_specs


def test_extended_feature_model__agg_per_category_feature(
    agg_per_category_feature, snowflake_feature_store
):
    """Test ExtendedFeatureModel has correct tile_specs for category groupby feature"""
    model = ExtendedFeatureModel(
        **agg_per_category_feature.dict(), feature_store=snowflake_feature_store
    )
    expected_sql = textwrap.dedent(
        """
        SELECT
          TO_TIMESTAMP(DATE_PART(EPOCH_SECOND, CAST(__FB_START_DATE AS TIMESTAMP)) + tile_index * 1800) AS __FB_TILE_START_DATE_COLUMN,
          "cust_id",
          "col_int",
          SUM("col_float") AS value
        FROM (
            SELECT
              *,
              FLOOR((DATE_PART(EPOCH_SECOND, "event_timestamp") - DATE_PART(EPOCH_SECOND, CAST(__FB_START_DATE AS TIMESTAMP))) / 1800) AS tile_index
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
                WHERE
                  "event_timestamp" >= CAST(__FB_START_DATE AS TIMESTAMP)
                  AND "event_timestamp" < CAST(__FB_END_DATE AS TIMESTAMP)
            )
        )
        GROUP BY
          tile_index,
          "cust_id",
          "col_int"
        ORDER BY
          tile_index
        """
    ).strip()
    expected_tile_specs = [
        TileSpec(
            time_modulo_frequency_second=300,
            blind_spot_second=600,
            frequency_minute=30,
            tile_sql=expected_sql,
            entity_column_names=["cust_id", "col_int"],
            value_column_names=["value"],
            tile_id="sum_f1800_m300_b600_b2cfe14613c88a50946d1b99fa0ae3ca5d89849d",
            category_column_name="col_int",
        )
    ]
    assert model.tile_specs == expected_tile_specs
