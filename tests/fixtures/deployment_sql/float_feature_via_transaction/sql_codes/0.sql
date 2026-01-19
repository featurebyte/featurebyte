SELECT
  L."transaction_id",
  R."sum_1d",
  {{ CURRENT_TIMESTAMP }} AS "POINT_IN_TIME"
FROM (
  WITH ENTITY_UNIVERSE AS (
    SELECT
      {{ CURRENT_TIMESTAMP }} AS "POINT_IN_TIME",
      "transaction_id"
    FROM (
      SELECT DISTINCT
        "col_int" AS "transaction_id"
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
          "event_timestamp" >= CAST('1970-01-01 00:00:00' AS TIMESTAMP)
          AND "event_timestamp" < {{ CURRENT_TIMESTAMP }}
      )
      WHERE
        NOT "col_int" IS NULL
    )
  ), JOINED_PARENTS_ENTITY_UNIVERSE AS (
    SELECT
      REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
      REQ."transaction_id" AS "transaction_id",
      REQ."cust_id" AS "cust_id"
    FROM (
      SELECT
        REQ."POINT_IN_TIME",
        REQ."transaction_id",
        CASE
          WHEN REQ."POINT_IN_TIME" < "T0"."event_timestamp"
          THEN NULL
          ELSE "T0"."cust_id"
        END AS "cust_id"
      FROM "ENTITY_UNIVERSE" AS REQ
      LEFT JOIN (
        SELECT
          "transaction_id",
          ANY_VALUE("cust_id") AS "cust_id",
          ANY_VALUE("event_timestamp") AS "event_timestamp"
        FROM (
          SELECT
            "col_int" AS "transaction_id",
            "cust_id" AS "cust_id",
            "event_timestamp"
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
        )
        GROUP BY
          "transaction_id"
      ) AS T0
        ON REQ."transaction_id" = T0."transaction_id"
    ) AS REQ
  )
  SELECT
    "POINT_IN_TIME",
    "transaction_id",
    "cust_id"
  FROM JOINED_PARENTS_ENTITY_UNIVERSE
) AS L
LEFT JOIN (
  WITH DEPLOYMENT_REQUEST_TABLE AS (
    SELECT
      REQ."cust_id",
      {{ CURRENT_TIMESTAMP }} AS POINT_IN_TIME
    FROM (
      SELECT DISTINCT
        CAST("cust_id" AS BIGINT) AS "cust_id"
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
          "event_timestamp" >= CAST(FLOOR(
            (
              EXTRACT(epoch_second FROM CAST({{ CURRENT_TIMESTAMP }} AS TIMESTAMP)) - 300
            ) / 1800
          ) * 1800 + 300 - 600 - 86400 AS TIMESTAMP)
          AND "event_timestamp" < CAST(FLOOR(
            (
              EXTRACT(epoch_second FROM CAST({{ CURRENT_TIMESTAMP }} AS TIMESTAMP)) - 300
            ) / 1800
          ) * 1800 + 300 - 600 AS TIMESTAMP)
      )
      WHERE
        NOT "cust_id" IS NULL
    ) AS REQ
  ), _FB_AGGREGATED AS (
    SELECT
      REQ."cust_id",
      REQ."POINT_IN_TIME",
      "T0"."_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295" AS "_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295"
    FROM DEPLOYMENT_REQUEST_TABLE AS REQ
    LEFT JOIN (
      SELECT
        "cust_id" AS "cust_id",
        SUM("col_float") AS "_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295"
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
          "event_timestamp" >= CAST(DATE_PART(EPOCH_SECOND, {{ CURRENT_TIMESTAMP }}) - 600 - 86400 AS TIMESTAMP)
          AND "event_timestamp" < CAST(DATE_PART(EPOCH_SECOND, {{ CURRENT_TIMESTAMP }}) - 600 AS TIMESTAMP)
      )
      GROUP BY
        "cust_id"
    ) AS T0
      ON REQ."cust_id" = T0."cust_id"
  )
  SELECT
    AGG."cust_id",
    CAST("_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295" AS DOUBLE) AS "sum_1d"
  FROM _FB_AGGREGATED AS AGG
) AS R
  ON L."cust_id" = R."cust_id"