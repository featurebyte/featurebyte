WITH TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS (
  SELECT
    avg_e0c1a10c943c5125b7c394567b1958216cdd0f7a.INDEX,
    avg_e0c1a10c943c5125b7c394567b1958216cdd0f7a."cust_id",
    sum_value_avg_e0c1a10c943c5125b7c394567b1958216cdd0f7a,
    count_value_avg_e0c1a10c943c5125b7c394567b1958216cdd0f7a
  FROM (
    SELECT
      *,
      F_TIMESTAMP_TO_INDEX(__FB_TILE_START_DATE_COLUMN, 1800, 900, 60) AS "INDEX"
    FROM (
      SELECT
        TO_TIMESTAMP(
          DATE_PART(EPOCH_SECOND, CAST('2022-04-18 09:15:00' AS TIMESTAMPNTZ)) + tile_index * 3600
        ) AS __FB_TILE_START_DATE_COLUMN,
        "cust_id",
        SUM("a") AS sum_value_avg_e0c1a10c943c5125b7c394567b1958216cdd0f7a,
        COUNT("a") AS count_value_avg_e0c1a10c943c5125b7c394567b1958216cdd0f7a
      FROM (
        SELECT
          *,
          FLOOR(
            (
              DATE_PART(EPOCH_SECOND, "ts") - DATE_PART(EPOCH_SECOND, CAST('2022-04-18 09:15:00' AS TIMESTAMPNTZ))
            ) / 3600
          ) AS tile_index
        FROM (
          SELECT
            *
          FROM (
            SELECT
              "ts" AS "ts",
              "cust_id" AS "cust_id",
              CASE WHEN (
                "a" IS NULL
              ) THEN 0 ELSE "a" END AS "a",
              "b" AS "b"
            FROM "db"."public"."event_table"
          )
          WHERE
            "ts" >= CAST('2022-04-18 09:15:00' AS TIMESTAMPNTZ)
            AND "ts" < CAST('2022-04-20 09:15:00' AS TIMESTAMPNTZ)
        )
      )
      GROUP BY
        tile_index,
        "cust_id"
    )
  ) AS avg_e0c1a10c943c5125b7c394567b1958216cdd0f7a
), REQUEST_TABLE AS (
  SELECT
    CAST('2022-04-20 10:00:00' AS TIMESTAMPNTZ) AS "POINT_IN_TIME",
    'C1' AS "CUSTOMER_ID"
), JOINED_PARENTS_REQUEST_TABLE AS (
  SELECT
    REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
    REQ."CUSTOMER_ID" AS "CUSTOMER_ID",
    REQ."COL_INT" AS "COL_INT"
  FROM (
    SELECT
      REQ."POINT_IN_TIME",
      REQ."CUSTOMER_ID",
      "T0"."COL_INT" AS "COL_INT"
    FROM REQUEST_TABLE AS REQ
    LEFT JOIN (
      SELECT
        "col_text" AS "COL_TEXT",
        "col_int" AS "COL_INT"
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
        FROM "sf_database"."sf_schema"."dimension_table"
      )
    ) AS T0
      ON REQ."COL_TEXT" = T0."COL_TEXT"
  ) AS REQ
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
    FROM JOINED_PARENTS_REQUEST_TABLE
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
    FROM JOINED_PARENTS_REQUEST_TABLE
  )
), _FB_AGGREGATED AS (
  SELECT
    REQ."POINT_IN_TIME",
    REQ."CUSTOMER_ID",
    REQ."COL_INT",
    "T0"."_fb_internal_window_w7200_avg_e0c1a10c943c5125b7c394567b1958216cdd0f7a" AS "_fb_internal_window_w7200_avg_e0c1a10c943c5125b7c394567b1958216cdd0f7a",
    "T1"."_fb_internal_window_w172800_avg_e0c1a10c943c5125b7c394567b1958216cdd0f7a" AS "_fb_internal_window_w172800_avg_e0c1a10c943c5125b7c394567b1958216cdd0f7a"
  FROM JOINED_PARENTS_REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      REQ."POINT_IN_TIME",
      REQ."CUSTOMER_ID",
      SUM(sum_value_avg_e0c1a10c943c5125b7c394567b1958216cdd0f7a) / SUM(count_value_avg_e0c1a10c943c5125b7c394567b1958216cdd0f7a) AS "_fb_internal_window_w7200_avg_e0c1a10c943c5125b7c394567b1958216cdd0f7a"
    FROM "REQUEST_TABLE_W7200_F3600_BS900_M1800_CUSTOMER_ID" AS REQ
    INNER JOIN TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS TILE
      ON (
        FLOOR(REQ.__FB_LAST_TILE_INDEX / 2) = FLOOR(TILE.INDEX / 2)
        OR FLOOR(REQ.__FB_LAST_TILE_INDEX / 2) - 1 = FLOOR(TILE.INDEX / 2)
      )
      AND REQ."CUSTOMER_ID" = TILE."cust_id"
    WHERE
      TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
    GROUP BY
      REQ."POINT_IN_TIME",
      REQ."CUSTOMER_ID"
  ) AS T0
    ON REQ."POINT_IN_TIME" = T0."POINT_IN_TIME" AND REQ."CUSTOMER_ID" = T0."CUSTOMER_ID"
  LEFT JOIN (
    SELECT
      REQ."POINT_IN_TIME",
      REQ."CUSTOMER_ID",
      SUM(sum_value_avg_e0c1a10c943c5125b7c394567b1958216cdd0f7a) / SUM(count_value_avg_e0c1a10c943c5125b7c394567b1958216cdd0f7a) AS "_fb_internal_window_w172800_avg_e0c1a10c943c5125b7c394567b1958216cdd0f7a"
    FROM "REQUEST_TABLE_W172800_F3600_BS900_M1800_CUSTOMER_ID" AS REQ
    INNER JOIN TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS TILE
      ON (
        FLOOR(REQ.__FB_LAST_TILE_INDEX / 48) = FLOOR(TILE.INDEX / 48)
        OR FLOOR(REQ.__FB_LAST_TILE_INDEX / 48) - 1 = FLOOR(TILE.INDEX / 48)
      )
      AND REQ."CUSTOMER_ID" = TILE."cust_id"
    WHERE
      TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
    GROUP BY
      REQ."POINT_IN_TIME",
      REQ."CUSTOMER_ID"
  ) AS T1
    ON REQ."POINT_IN_TIME" = T1."POINT_IN_TIME" AND REQ."CUSTOMER_ID" = T1."CUSTOMER_ID"
)
SELECT
  AGG."POINT_IN_TIME",
  AGG."CUSTOMER_ID",
  "_fb_internal_window_w7200_avg_e0c1a10c943c5125b7c394567b1958216cdd0f7a" AS "a_2h_average",
  "_fb_internal_window_w172800_avg_e0c1a10c943c5125b7c394567b1958216cdd0f7a" AS "a_48h_average"
FROM _FB_AGGREGATED AS AGG
