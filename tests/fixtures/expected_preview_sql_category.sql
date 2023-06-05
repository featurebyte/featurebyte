WITH REQUEST_TABLE AS (
  SELECT
    CAST('2022-04-20 10:00:00' AS TIMESTAMPNTZ) AS "POINT_IN_TIME",
    'C1' AS "CUSTOMER_ID"
), TILE_F3600_M1800_B900_FEB86FDFF3B041DC98880F9B22EE9078FBCF5226 AS (
  SELECT
    avg_828be81883198b473c3a5ac214dd4112d7559427.INDEX,
    avg_828be81883198b473c3a5ac214dd4112d7559427."cust_id",
    avg_828be81883198b473c3a5ac214dd4112d7559427."product_type",
    sum_value_avg_828be81883198b473c3a5ac214dd4112d7559427,
    count_value_avg_828be81883198b473c3a5ac214dd4112d7559427
  FROM (
    SELECT
      index,
      "cust_id",
      "product_type",
      SUM("a") AS sum_value_avg_828be81883198b473c3a5ac214dd4112d7559427,
      COUNT("a") AS count_value_avg_828be81883198b473c3a5ac214dd4112d7559427
    FROM (
      SELECT
        *,
        F_TIMESTAMP_TO_INDEX(CONVERT_TIMEZONE('UTC', "ts"), 1800, 900, 60) AS index
      FROM (
        WITH __FB_ENTITY_TABLE_NAME AS (
          (
            SELECT
              "CUSTOMER_ID" AS "cust_id",
              TO_TIMESTAMP(
                FLOOR((
                  DATE_PART(EPOCH_SECOND, MAX(POINT_IN_TIME)) - 1800
                ) / 3600) * 3600 + 1800 - 900
              ) AS "__FB_ENTITY_TABLE_END_DATE",
              DATEADD(
                microsecond,
                (
                  48 * 3600 * CAST(1000000 AS BIGINT) / CAST(1 AS BIGINT)
                ) * -1,
                TO_TIMESTAMP(
                  FLOOR((
                    DATE_PART(EPOCH_SECOND, MIN(POINT_IN_TIME)) - 1800
                  ) / 3600) * 3600 + 1800 - 900
                )
              ) AS "__FB_ENTITY_TABLE_START_DATE"
            FROM "REQUEST_TABLE"
            GROUP BY
              "CUSTOMER_ID"
          )
        )
        SELECT
          R.*
        FROM __FB_ENTITY_TABLE_NAME
        INNER JOIN (
          SELECT
            "ts" AS "ts",
            "cust_id" AS "cust_id",
            "a" AS "a",
            "b" AS "b",
            (
              "a" + "b"
            ) AS "c"
          FROM "db"."public"."event_table"
        ) AS R
          ON R."cust_id" = __FB_ENTITY_TABLE_NAME."cust_id"
          AND R."ts" >= __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_START_DATE
          AND R."ts" < __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_END_DATE
      )
    )
    GROUP BY
      index,
      "cust_id",
      "product_type"
  ) AS avg_828be81883198b473c3a5ac214dd4112d7559427
), "REQUEST_TABLE_W7200_F3600_BS900_M1800_CUSTOMER_ID" AS (
  SELECT
    "POINT_IN_TIME",
    "CUSTOMER_ID",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 3600) AS "__FB_LAST_TILE_INDEX",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 3600) - 2 AS "__FB_FIRST_TILE_INDEX"
  FROM (
    SELECT DISTINCT
      "POINT_IN_TIME",
      "CUSTOMER_ID"
    FROM REQUEST_TABLE
  )
), "REQUEST_TABLE_W172800_F3600_BS900_M1800_CUSTOMER_ID" AS (
  SELECT
    "POINT_IN_TIME",
    "CUSTOMER_ID",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 3600) AS "__FB_LAST_TILE_INDEX",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 3600) - 48 AS "__FB_FIRST_TILE_INDEX"
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
    "T0"."_fb_internal_window_w7200_avg_828be81883198b473c3a5ac214dd4112d7559427" AS "_fb_internal_window_w7200_avg_828be81883198b473c3a5ac214dd4112d7559427",
    "T1"."_fb_internal_window_w172800_avg_828be81883198b473c3a5ac214dd4112d7559427" AS "_fb_internal_window_w172800_avg_828be81883198b473c3a5ac214dd4112d7559427"
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
          INNER_."inner__fb_internal_window_w7200_avg_828be81883198b473c3a5ac214dd4112d7559427"
        )
      ) AS "_fb_internal_window_w7200_avg_828be81883198b473c3a5ac214dd4112d7559427"
    FROM (
      SELECT
        "POINT_IN_TIME",
        "CUSTOMER_ID",
        "product_type",
        SUM(sum_value_avg_828be81883198b473c3a5ac214dd4112d7559427) / SUM(count_value_avg_828be81883198b473c3a5ac214dd4112d7559427) AS "inner__fb_internal_window_w7200_avg_828be81883198b473c3a5ac214dd4112d7559427"
      FROM (
        SELECT
          REQ."POINT_IN_TIME",
          REQ."CUSTOMER_ID",
          TILE.INDEX,
          TILE.count_value_avg_828be81883198b473c3a5ac214dd4112d7559427,
          TILE.sum_value_avg_828be81883198b473c3a5ac214dd4112d7559427,
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
          TILE.count_value_avg_828be81883198b473c3a5ac214dd4112d7559427,
          TILE.sum_value_avg_828be81883198b473c3a5ac214dd4112d7559427,
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
          INNER_."inner__fb_internal_window_w172800_avg_828be81883198b473c3a5ac214dd4112d7559427"
        )
      ) AS "_fb_internal_window_w172800_avg_828be81883198b473c3a5ac214dd4112d7559427"
    FROM (
      SELECT
        "POINT_IN_TIME",
        "CUSTOMER_ID",
        "product_type",
        SUM(sum_value_avg_828be81883198b473c3a5ac214dd4112d7559427) / SUM(count_value_avg_828be81883198b473c3a5ac214dd4112d7559427) AS "inner__fb_internal_window_w172800_avg_828be81883198b473c3a5ac214dd4112d7559427"
      FROM (
        SELECT
          REQ."POINT_IN_TIME",
          REQ."CUSTOMER_ID",
          TILE.INDEX,
          TILE.count_value_avg_828be81883198b473c3a5ac214dd4112d7559427,
          TILE.sum_value_avg_828be81883198b473c3a5ac214dd4112d7559427,
          TILE."product_type"
        FROM "REQUEST_TABLE_W172800_F3600_BS900_M1800_CUSTOMER_ID" AS REQ
        INNER JOIN TILE_F3600_M1800_B900_FEB86FDFF3B041DC98880F9B22EE9078FBCF5226 AS TILE
          ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 48) = FLOOR(TILE.INDEX / 48)
          AND REQ."CUSTOMER_ID" = TILE."cust_id"
        WHERE
          TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
        UNION ALL
        SELECT
          REQ."POINT_IN_TIME",
          REQ."CUSTOMER_ID",
          TILE.INDEX,
          TILE.count_value_avg_828be81883198b473c3a5ac214dd4112d7559427,
          TILE.sum_value_avg_828be81883198b473c3a5ac214dd4112d7559427,
          TILE."product_type"
        FROM "REQUEST_TABLE_W172800_F3600_BS900_M1800_CUSTOMER_ID" AS REQ
        INNER JOIN TILE_F3600_M1800_B900_FEB86FDFF3B041DC98880F9B22EE9078FBCF5226 AS TILE
          ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 48) - 1 = FLOOR(TILE.INDEX / 48)
          AND REQ."CUSTOMER_ID" = TILE."cust_id"
        WHERE
          TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
      )
      GROUP BY
        "POINT_IN_TIME",
        "CUSTOMER_ID",
        "product_type"
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
  "_fb_internal_window_w7200_avg_828be81883198b473c3a5ac214dd4112d7559427" AS "a_2h_average",
  "_fb_internal_window_w172800_avg_828be81883198b473c3a5ac214dd4112d7559427" AS "a_48h_average"
FROM _FB_AGGREGATED AS AGG
