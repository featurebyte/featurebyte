WITH REQUEST_TABLE AS (
  SELECT
    CAST('2022-04-20 10:00:00' AS TIMESTAMPNTZ) AS "POINT_IN_TIME",
    'C1' AS "CUSTOMER_ID"
), TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS (
  SELECT
    avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb.INDEX,
    avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb."cust_id",
    sum_value_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb,
    count_value_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb,
    value_max_4541e14e85810b671a9b270f1f461f19e3bb6a64,
    value_sum_999015eb44ce788935f8962d3d34957b95bcf04f
  FROM (
    SELECT
      index,
      "cust_id",
      SUM("a") AS sum_value_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb,
      COUNT("a") AS count_value_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb
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
                    DATE_PART(EPOCH_SECOND, MAX(POINT_IN_TIME)) - 1800
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
      "cust_id"
  ) AS avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb
  RIGHT JOIN (
    SELECT
      index,
      "cust_id",
      MAX("a") AS value_max_4541e14e85810b671a9b270f1f461f19e3bb6a64
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
                    DATE_PART(EPOCH_SECOND, MAX(POINT_IN_TIME)) - 1800
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
      "cust_id"
  ) AS max_4541e14e85810b671a9b270f1f461f19e3bb6a64
    ON avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb.INDEX = max_4541e14e85810b671a9b270f1f461f19e3bb6a64.INDEX
    AND avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb."cust_id" = max_4541e14e85810b671a9b270f1f461f19e3bb6a64."cust_id"
  RIGHT JOIN (
    SELECT
      index,
      "cust_id",
      SUM("a") AS value_sum_999015eb44ce788935f8962d3d34957b95bcf04f
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
                    DATE_PART(EPOCH_SECOND, MAX(POINT_IN_TIME)) - 1800
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
      "cust_id"
  ) AS sum_999015eb44ce788935f8962d3d34957b95bcf04f
    ON avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb.INDEX = sum_999015eb44ce788935f8962d3d34957b95bcf04f.INDEX
    AND avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb."cust_id" = sum_999015eb44ce788935f8962d3d34957b95bcf04f."cust_id"
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
), "REQUEST_TABLE_W129600_F3600_BS900_M1800_CUSTOMER_ID" AS (
  SELECT
    "POINT_IN_TIME",
    "CUSTOMER_ID",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 3600) AS "__FB_LAST_TILE_INDEX",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 3600) - 36 AS "__FB_FIRST_TILE_INDEX"
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
    "T0"."_fb_internal_window_w7200_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb" AS "_fb_internal_window_w7200_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb",
    "T0"."_fb_internal_window_w7200_max_4541e14e85810b671a9b270f1f461f19e3bb6a64" AS "_fb_internal_window_w7200_max_4541e14e85810b671a9b270f1f461f19e3bb6a64",
    "T0"."_fb_internal_window_w7200_sum_999015eb44ce788935f8962d3d34957b95bcf04f" AS "_fb_internal_window_w7200_sum_999015eb44ce788935f8962d3d34957b95bcf04f",
    "T1"."_fb_internal_window_w172800_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb" AS "_fb_internal_window_w172800_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb",
    "T2"."_fb_internal_window_w129600_max_4541e14e85810b671a9b270f1f461f19e3bb6a64" AS "_fb_internal_window_w129600_max_4541e14e85810b671a9b270f1f461f19e3bb6a64",
    "T2"."_fb_internal_window_w129600_sum_999015eb44ce788935f8962d3d34957b95bcf04f" AS "_fb_internal_window_w129600_sum_999015eb44ce788935f8962d3d34957b95bcf04f"
  FROM REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      "POINT_IN_TIME",
      "CUSTOMER_ID",
      SUM(sum_value_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb) / SUM(count_value_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb) AS "_fb_internal_window_w7200_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb",
      MAX(value_max_4541e14e85810b671a9b270f1f461f19e3bb6a64) AS "_fb_internal_window_w7200_max_4541e14e85810b671a9b270f1f461f19e3bb6a64",
      SUM(value_sum_999015eb44ce788935f8962d3d34957b95bcf04f) AS "_fb_internal_window_w7200_sum_999015eb44ce788935f8962d3d34957b95bcf04f"
    FROM (
      SELECT
        REQ."POINT_IN_TIME",
        REQ."CUSTOMER_ID",
        TILE.INDEX,
        TILE.count_value_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb,
        TILE.sum_value_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb,
        TILE.value_max_4541e14e85810b671a9b270f1f461f19e3bb6a64,
        TILE.value_sum_999015eb44ce788935f8962d3d34957b95bcf04f
      FROM "REQUEST_TABLE_W7200_F3600_BS900_M1800_CUSTOMER_ID" AS REQ
      INNER JOIN TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 2) = FLOOR(TILE.INDEX / 2)
        AND REQ."CUSTOMER_ID" = TILE."cust_id"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
      UNION ALL
      SELECT
        REQ."POINT_IN_TIME",
        REQ."CUSTOMER_ID",
        TILE.INDEX,
        TILE.count_value_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb,
        TILE.sum_value_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb,
        TILE.value_max_4541e14e85810b671a9b270f1f461f19e3bb6a64,
        TILE.value_sum_999015eb44ce788935f8962d3d34957b95bcf04f
      FROM "REQUEST_TABLE_W7200_F3600_BS900_M1800_CUSTOMER_ID" AS REQ
      INNER JOIN TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 2) - 1 = FLOOR(TILE.INDEX / 2)
        AND REQ."CUSTOMER_ID" = TILE."cust_id"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
    )
    GROUP BY
      "POINT_IN_TIME",
      "CUSTOMER_ID"
  ) AS T0
    ON REQ."POINT_IN_TIME" = T0."POINT_IN_TIME" AND REQ."CUSTOMER_ID" = T0."CUSTOMER_ID"
  LEFT JOIN (
    SELECT
      "POINT_IN_TIME",
      "CUSTOMER_ID",
      SUM(sum_value_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb) / SUM(count_value_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb) AS "_fb_internal_window_w172800_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb"
    FROM (
      SELECT
        REQ."POINT_IN_TIME",
        REQ."CUSTOMER_ID",
        TILE.INDEX,
        TILE.count_value_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb,
        TILE.sum_value_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb
      FROM "REQUEST_TABLE_W172800_F3600_BS900_M1800_CUSTOMER_ID" AS REQ
      INNER JOIN TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 48) = FLOOR(TILE.INDEX / 48)
        AND REQ."CUSTOMER_ID" = TILE."cust_id"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
      UNION ALL
      SELECT
        REQ."POINT_IN_TIME",
        REQ."CUSTOMER_ID",
        TILE.INDEX,
        TILE.count_value_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb,
        TILE.sum_value_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb
      FROM "REQUEST_TABLE_W172800_F3600_BS900_M1800_CUSTOMER_ID" AS REQ
      INNER JOIN TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 48) - 1 = FLOOR(TILE.INDEX / 48)
        AND REQ."CUSTOMER_ID" = TILE."cust_id"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
    )
    GROUP BY
      "POINT_IN_TIME",
      "CUSTOMER_ID"
  ) AS T1
    ON REQ."POINT_IN_TIME" = T1."POINT_IN_TIME" AND REQ."CUSTOMER_ID" = T1."CUSTOMER_ID"
  LEFT JOIN (
    SELECT
      "POINT_IN_TIME",
      "CUSTOMER_ID",
      MAX(value_max_4541e14e85810b671a9b270f1f461f19e3bb6a64) AS "_fb_internal_window_w129600_max_4541e14e85810b671a9b270f1f461f19e3bb6a64",
      SUM(value_sum_999015eb44ce788935f8962d3d34957b95bcf04f) AS "_fb_internal_window_w129600_sum_999015eb44ce788935f8962d3d34957b95bcf04f"
    FROM (
      SELECT
        REQ."POINT_IN_TIME",
        REQ."CUSTOMER_ID",
        TILE.INDEX,
        TILE.value_max_4541e14e85810b671a9b270f1f461f19e3bb6a64,
        TILE.value_sum_999015eb44ce788935f8962d3d34957b95bcf04f
      FROM "REQUEST_TABLE_W129600_F3600_BS900_M1800_CUSTOMER_ID" AS REQ
      INNER JOIN TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 36) = FLOOR(TILE.INDEX / 36)
        AND REQ."CUSTOMER_ID" = TILE."cust_id"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
      UNION ALL
      SELECT
        REQ."POINT_IN_TIME",
        REQ."CUSTOMER_ID",
        TILE.INDEX,
        TILE.value_max_4541e14e85810b671a9b270f1f461f19e3bb6a64,
        TILE.value_sum_999015eb44ce788935f8962d3d34957b95bcf04f
      FROM "REQUEST_TABLE_W129600_F3600_BS900_M1800_CUSTOMER_ID" AS REQ
      INNER JOIN TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 36) - 1 = FLOOR(TILE.INDEX / 36)
        AND REQ."CUSTOMER_ID" = TILE."cust_id"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
    )
    GROUP BY
      "POINT_IN_TIME",
      "CUSTOMER_ID"
  ) AS T2
    ON REQ."POINT_IN_TIME" = T2."POINT_IN_TIME" AND REQ."CUSTOMER_ID" = T2."CUSTOMER_ID"
)
SELECT
  AGG."POINT_IN_TIME",
  AGG."CUSTOMER_ID",
  "_fb_internal_window_w7200_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb" AS "a_2h_average",
  "_fb_internal_window_w172800_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb" AS "a_48h_average",
  "_fb_internal_window_w7200_max_4541e14e85810b671a9b270f1f461f19e3bb6a64" AS "a_2h_max",
  "_fb_internal_window_w129600_max_4541e14e85810b671a9b270f1f461f19e3bb6a64" AS "a_36h_max",
  "_fb_internal_window_w7200_sum_999015eb44ce788935f8962d3d34957b95bcf04f" AS "a_2h_sum",
  "_fb_internal_window_w129600_sum_999015eb44ce788935f8962d3d34957b95bcf04f" AS "a_36h_sum"
FROM _FB_AGGREGATED AS AGG
