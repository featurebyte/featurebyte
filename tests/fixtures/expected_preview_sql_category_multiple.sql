WITH REQUEST_TABLE AS (
  SELECT
    CAST('2022-04-20 10:00:00' AS TIMESTAMP) AS "POINT_IN_TIME",
    'C1' AS "CUSTOMER_ID"
), TILE_F3600_M1800_B900_FEB86FDFF3B041DC98880F9B22EE9078FBCF5226 AS (
  SELECT
    max_02aed4a12b53bed22c075b1174077f99b0d63b1f.INDEX,
    max_02aed4a12b53bed22c075b1174077f99b0d63b1f."cust_id",
    max_02aed4a12b53bed22c075b1174077f99b0d63b1f."product_type",
    value_max_02aed4a12b53bed22c075b1174077f99b0d63b1f,
    value_min_77c9976115348ec6168bc0c69a7c3609e8f0ac67
  FROM (
    WITH __FB_ENTITY_TABLE_NAME AS (
      SELECT
        "CUSTOMER_ID" AS "cust_id",
        CAST(FLOOR((
          DATE_PART(EPOCH_SECOND, MAX(POINT_IN_TIME)) - 1800
        ) / 3600) * 3600 + 1800 - 900 AS TIMESTAMP) AS __FB_ENTITY_TABLE_END_DATE,
        DATEADD(
          MICROSECOND,
          (
            48 * 3600 * CAST(1000000 AS BIGINT) / CAST(1 AS BIGINT)
          ) * -1,
          CAST(FLOOR((
            DATE_PART(EPOCH_SECOND, MIN(POINT_IN_TIME)) - 1800
          ) / 3600) * 3600 + 1800 - 900 AS TIMESTAMP)
        ) AS __FB_ENTITY_TABLE_START_DATE
      FROM "REQUEST_TABLE"
      GROUP BY
        "CUSTOMER_ID"
    ), __FB_TILE_COMPUTE_INPUT_TABLE_NAME AS (
      SELECT
        R.*
      FROM __FB_ENTITY_TABLE_NAME
      INNER JOIN (
        SELECT
          "ts" AS "ts",
          "cust_id" AS "cust_id",
          "product_type" AS "product_type",
          "a" AS "input_col_max_02aed4a12b53bed22c075b1174077f99b0d63b1f"
        FROM "db"."public"."event_table"
      ) AS R
        ON R."cust_id" = __FB_ENTITY_TABLE_NAME."cust_id"
        AND R."ts" >= __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_START_DATE
        AND R."ts" < __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_END_DATE
    )
    SELECT
      index,
      "cust_id",
      "product_type",
      MAX("input_col_max_02aed4a12b53bed22c075b1174077f99b0d63b1f") AS value_max_02aed4a12b53bed22c075b1174077f99b0d63b1f
    FROM (
      SELECT
        *,
        F_TIMESTAMP_TO_INDEX(CONVERT_TIMEZONE('UTC', "ts"), 1800, 900, 60) AS index
      FROM __FB_TILE_COMPUTE_INPUT_TABLE_NAME
    )
    GROUP BY
      index,
      "cust_id",
      "product_type"
  ) AS max_02aed4a12b53bed22c075b1174077f99b0d63b1f
  RIGHT JOIN (
    WITH __FB_ENTITY_TABLE_NAME AS (
      SELECT
        "CUSTOMER_ID" AS "cust_id",
        CAST(FLOOR((
          DATE_PART(EPOCH_SECOND, MAX(POINT_IN_TIME)) - 1800
        ) / 3600) * 3600 + 1800 - 900 AS TIMESTAMP) AS __FB_ENTITY_TABLE_END_DATE,
        DATEADD(
          MICROSECOND,
          (
            48 * 3600 * CAST(1000000 AS BIGINT) / CAST(1 AS BIGINT)
          ) * -1,
          CAST(FLOOR((
            DATE_PART(EPOCH_SECOND, MIN(POINT_IN_TIME)) - 1800
          ) / 3600) * 3600 + 1800 - 900 AS TIMESTAMP)
        ) AS __FB_ENTITY_TABLE_START_DATE
      FROM "REQUEST_TABLE"
      GROUP BY
        "CUSTOMER_ID"
    ), __FB_TILE_COMPUTE_INPUT_TABLE_NAME AS (
      SELECT
        R.*
      FROM __FB_ENTITY_TABLE_NAME
      INNER JOIN (
        SELECT
          "ts" AS "ts",
          "cust_id" AS "cust_id",
          "product_type" AS "product_type",
          "a" AS "input_col_min_77c9976115348ec6168bc0c69a7c3609e8f0ac67"
        FROM "db"."public"."event_table"
      ) AS R
        ON R."cust_id" = __FB_ENTITY_TABLE_NAME."cust_id"
        AND R."ts" >= __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_START_DATE
        AND R."ts" < __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_END_DATE
    )
    SELECT
      index,
      "cust_id",
      "product_type",
      MIN("input_col_min_77c9976115348ec6168bc0c69a7c3609e8f0ac67") AS value_min_77c9976115348ec6168bc0c69a7c3609e8f0ac67
    FROM (
      SELECT
        *,
        F_TIMESTAMP_TO_INDEX(CONVERT_TIMEZONE('UTC', "ts"), 1800, 900, 60) AS index
      FROM __FB_TILE_COMPUTE_INPUT_TABLE_NAME
    )
    GROUP BY
      index,
      "cust_id",
      "product_type"
  ) AS min_77c9976115348ec6168bc0c69a7c3609e8f0ac67
    ON max_02aed4a12b53bed22c075b1174077f99b0d63b1f.INDEX = min_77c9976115348ec6168bc0c69a7c3609e8f0ac67.INDEX
    AND max_02aed4a12b53bed22c075b1174077f99b0d63b1f."cust_id" = min_77c9976115348ec6168bc0c69a7c3609e8f0ac67."cust_id"
), "REQUEST_TABLE_W7200_F3600_BS900_M1800_CUSTOMER_ID" AS (
  SELECT
    "POINT_IN_TIME",
    "CUSTOMER_ID",
    CAST(FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 3600) AS BIGINT) AS __FB_LAST_TILE_INDEX,
    CAST(FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 3600) AS BIGINT) - 2 AS __FB_FIRST_TILE_INDEX
  FROM (
    SELECT DISTINCT
      "POINT_IN_TIME",
      "CUSTOMER_ID"
    FROM REQUEST_TABLE
  )
), _FB_AGGREGATED AS (
  SELECT
    REQ."POINT_IN_TIME",
    REQ."CUSTOMER_ID",
    "T0"."_fb_internal_CUSTOMER_ID_window_w7200_max_02aed4a12b53bed22c075b1174077f99b0d63b1f" AS "_fb_internal_CUSTOMER_ID_window_w7200_max_02aed4a12b53bed22c075b1174077f99b0d63b1f",
    "T1"."_fb_internal_CUSTOMER_ID_window_w7200_min_77c9976115348ec6168bc0c69a7c3609e8f0ac67" AS "_fb_internal_CUSTOMER_ID_window_w7200_min_77c9976115348ec6168bc0c69a7c3609e8f0ac67"
  FROM REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      INNER_."POINT_IN_TIME",
      INNER_."CUSTOMER_ID",
      OBJECT_AGG(
        CASE
          WHEN INNER_."product_type" IS NULL
          THEN '__MISSING__'
          ELSE CAST(INNER_."product_type" AS TEXT)
        END,
        TO_VARIANT(
          INNER_."inner__fb_internal_CUSTOMER_ID_window_w7200_max_02aed4a12b53bed22c075b1174077f99b0d63b1f"
        )
      ) AS "_fb_internal_CUSTOMER_ID_window_w7200_max_02aed4a12b53bed22c075b1174077f99b0d63b1f"
    FROM (
      SELECT
        "POINT_IN_TIME",
        "CUSTOMER_ID",
        "product_type",
        "inner__fb_internal_CUSTOMER_ID_window_w7200_max_02aed4a12b53bed22c075b1174077f99b0d63b1f"
      FROM (
        SELECT
          "POINT_IN_TIME",
          "CUSTOMER_ID",
          "product_type",
          "inner__fb_internal_CUSTOMER_ID_window_w7200_max_02aed4a12b53bed22c075b1174077f99b0d63b1f",
          ROW_NUMBER() OVER (PARTITION BY "POINT_IN_TIME", "CUSTOMER_ID" ORDER BY "inner__fb_internal_CUSTOMER_ID_window_w7200_max_02aed4a12b53bed22c075b1174077f99b0d63b1f" DESC NULLS LAST) AS "__fb_object_agg_row_number"
        FROM (
          SELECT
            "POINT_IN_TIME",
            "CUSTOMER_ID",
            "product_type",
            MAX(value_max_02aed4a12b53bed22c075b1174077f99b0d63b1f) AS "inner__fb_internal_CUSTOMER_ID_window_w7200_max_02aed4a12b53bed22c075b1174077f99b0d63b1f"
          FROM (
            SELECT
              REQ."POINT_IN_TIME",
              REQ."CUSTOMER_ID",
              TILE.INDEX,
              TILE.value_max_02aed4a12b53bed22c075b1174077f99b0d63b1f,
              TILE."product_type"
            FROM "REQUEST_TABLE_W7200_F3600_BS900_M1800_CUSTOMER_ID" AS REQ
            INNER JOIN TILE_F3600_M1800_B900_FEB86FDFF3B041DC98880F9B22EE9078FBCF5226 AS TILE
              ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 2) = FLOOR(TILE.INDEX / 2)
              AND REQ."CUSTOMER_ID" = TILE."cust_id"
            WHERE
              TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
            UNION ALL
            SELECT
              REQ."POINT_IN_TIME",
              REQ."CUSTOMER_ID",
              TILE.INDEX,
              TILE.value_max_02aed4a12b53bed22c075b1174077f99b0d63b1f,
              TILE."product_type"
            FROM "REQUEST_TABLE_W7200_F3600_BS900_M1800_CUSTOMER_ID" AS REQ
            INNER JOIN TILE_F3600_M1800_B900_FEB86FDFF3B041DC98880F9B22EE9078FBCF5226 AS TILE
              ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 2) - 1 = FLOOR(TILE.INDEX / 2)
              AND REQ."CUSTOMER_ID" = TILE."cust_id"
            WHERE
              TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
          )
          GROUP BY
            "POINT_IN_TIME",
            "CUSTOMER_ID",
            "product_type"
        )
      )
      WHERE
        "__fb_object_agg_row_number" <= 50000
    ) AS INNER_
    GROUP BY
      INNER_."POINT_IN_TIME",
      INNER_."CUSTOMER_ID"
  ) AS T0
    ON REQ."POINT_IN_TIME" = T0."POINT_IN_TIME" AND REQ."CUSTOMER_ID" = T0."CUSTOMER_ID"
  LEFT JOIN (
    SELECT
      INNER_."POINT_IN_TIME",
      INNER_."CUSTOMER_ID",
      OBJECT_AGG(
        CASE
          WHEN INNER_."product_type" IS NULL
          THEN '__MISSING__'
          ELSE CAST(INNER_."product_type" AS TEXT)
        END,
        TO_VARIANT(
          INNER_."inner__fb_internal_CUSTOMER_ID_window_w7200_min_77c9976115348ec6168bc0c69a7c3609e8f0ac67"
        )
      ) AS "_fb_internal_CUSTOMER_ID_window_w7200_min_77c9976115348ec6168bc0c69a7c3609e8f0ac67"
    FROM (
      SELECT
        "POINT_IN_TIME",
        "CUSTOMER_ID",
        "product_type",
        "inner__fb_internal_CUSTOMER_ID_window_w7200_min_77c9976115348ec6168bc0c69a7c3609e8f0ac67"
      FROM (
        SELECT
          "POINT_IN_TIME",
          "CUSTOMER_ID",
          "product_type",
          "inner__fb_internal_CUSTOMER_ID_window_w7200_min_77c9976115348ec6168bc0c69a7c3609e8f0ac67",
          ROW_NUMBER() OVER (PARTITION BY "POINT_IN_TIME", "CUSTOMER_ID" ORDER BY "inner__fb_internal_CUSTOMER_ID_window_w7200_min_77c9976115348ec6168bc0c69a7c3609e8f0ac67" DESC NULLS LAST) AS "__fb_object_agg_row_number"
        FROM (
          SELECT
            "POINT_IN_TIME",
            "CUSTOMER_ID",
            "product_type",
            MIN(value_min_77c9976115348ec6168bc0c69a7c3609e8f0ac67) AS "inner__fb_internal_CUSTOMER_ID_window_w7200_min_77c9976115348ec6168bc0c69a7c3609e8f0ac67"
          FROM (
            SELECT
              REQ."POINT_IN_TIME",
              REQ."CUSTOMER_ID",
              TILE.INDEX,
              TILE.value_min_77c9976115348ec6168bc0c69a7c3609e8f0ac67,
              TILE."product_type"
            FROM "REQUEST_TABLE_W7200_F3600_BS900_M1800_CUSTOMER_ID" AS REQ
            INNER JOIN TILE_F3600_M1800_B900_FEB86FDFF3B041DC98880F9B22EE9078FBCF5226 AS TILE
              ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 2) = FLOOR(TILE.INDEX / 2)
              AND REQ."CUSTOMER_ID" = TILE."cust_id"
            WHERE
              TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
            UNION ALL
            SELECT
              REQ."POINT_IN_TIME",
              REQ."CUSTOMER_ID",
              TILE.INDEX,
              TILE.value_min_77c9976115348ec6168bc0c69a7c3609e8f0ac67,
              TILE."product_type"
            FROM "REQUEST_TABLE_W7200_F3600_BS900_M1800_CUSTOMER_ID" AS REQ
            INNER JOIN TILE_F3600_M1800_B900_FEB86FDFF3B041DC98880F9B22EE9078FBCF5226 AS TILE
              ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 2) - 1 = FLOOR(TILE.INDEX / 2)
              AND REQ."CUSTOMER_ID" = TILE."cust_id"
            WHERE
              TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
          )
          GROUP BY
            "POINT_IN_TIME",
            "CUSTOMER_ID",
            "product_type"
        )
      )
      WHERE
        "__fb_object_agg_row_number" <= 50000
    ) AS INNER_
    GROUP BY
      INNER_."POINT_IN_TIME",
      INNER_."CUSTOMER_ID"
  ) AS T1
    ON REQ."POINT_IN_TIME" = T1."POINT_IN_TIME" AND REQ."CUSTOMER_ID" = T1."CUSTOMER_ID"
)
SELECT
  AGG."POINT_IN_TIME",
  AGG."CUSTOMER_ID",
  CAST((
    F_COUNT_DICT_ENTROPY(
      "_fb_internal_CUSTOMER_ID_window_w7200_max_02aed4a12b53bed22c075b1174077f99b0d63b1f"
    ) / NULLIF(
      F_COUNT_DICT_ENTROPY(
        "_fb_internal_CUSTOMER_ID_window_w7200_min_77c9976115348ec6168bc0c69a7c3609e8f0ac67"
      ),
      0
    )
  ) AS DOUBLE) AS "complex_cross_aggregate"
FROM _FB_AGGREGATED AS AGG
