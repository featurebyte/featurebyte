WITH DEPLOYMENT_REQUEST_TABLE AS (
  SELECT
    REQ."cust_id",
    {{ CURRENT_TIMESTAMP }} AS POINT_IN_TIME
  FROM (
    SELECT DISTINCT
      CAST("cust_id" AS BIGINT) AS "cust_id"
    FROM (
      SELECT
        "cust_id" AS "cust_id"
      FROM "sf_database"."sf_schema"."sf_table"
      WHERE
        (
          "col_float" > 100
        )
        AND (
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
    )
    WHERE
      NOT "cust_id" IS NULL
    UNION
    SELECT DISTINCT
      CAST("cust_id" AS BIGINT) AS "cust_id"
    FROM (
      SELECT
        "cust_id" AS "cust_id"
      FROM "sf_database"."sf_schema"."sf_table"
      WHERE
        (
          "col_float" < 10
        )
        AND (
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
    )
    WHERE
      NOT "cust_id" IS NULL
  ) AS REQ
), _FB_AGGREGATED AS (
  SELECT
    REQ."cust_id",
    REQ."POINT_IN_TIME",
    "T0"."_fb_internal_cust_id_window_w86400_sum_58d51b03babb56d9c8829eb11257b24a7e088596" AS "_fb_internal_cust_id_window_w86400_sum_58d51b03babb56d9c8829eb11257b24a7e088596",
    "T1"."_fb_internal_cust_id_window_w86400_sum_25bf106eeade2e255561c765500361f0a061d889" AS "_fb_internal_cust_id_window_w86400_sum_25bf106eeade2e255561c765500361f0a061d889"
  FROM DEPLOYMENT_REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      "cust_id" AS "cust_id",
      SUM("col_float") AS "_fb_internal_cust_id_window_w86400_sum_58d51b03babb56d9c8829eb11257b24a7e088596"
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
        (
          "col_float" < 10
        )
        AND (
          "event_timestamp" >= CAST(DATE_PART(EPOCH_SECOND, {{ CURRENT_TIMESTAMP }}) - 600 - 86400 AS TIMESTAMP)
          AND "event_timestamp" < CAST(DATE_PART(EPOCH_SECOND, {{ CURRENT_TIMESTAMP }}) - 600 AS TIMESTAMP)
        )
    )
    GROUP BY
      "cust_id"
  ) AS T0
    ON REQ."cust_id" = T0."cust_id"
  LEFT JOIN (
    SELECT
      "cust_id" AS "cust_id",
      SUM("col_float") AS "_fb_internal_cust_id_window_w86400_sum_25bf106eeade2e255561c765500361f0a061d889"
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
        (
          "col_float" > 100
        )
        AND (
          "event_timestamp" >= CAST(DATE_PART(EPOCH_SECOND, {{ CURRENT_TIMESTAMP }}) - 600 - 86400 AS TIMESTAMP)
          AND "event_timestamp" < CAST(DATE_PART(EPOCH_SECOND, {{ CURRENT_TIMESTAMP }}) - 600 AS TIMESTAMP)
        )
    )
    GROUP BY
      "cust_id"
  ) AS T1
    ON REQ."cust_id" = T1."cust_id"
)
SELECT
  AGG."cust_id",
  CAST("_fb_internal_cust_id_window_w86400_sum_58d51b03babb56d9c8829eb11257b24a7e088596" AS DOUBLE) AS "sum_1d_lt_10",
  CAST("_fb_internal_cust_id_window_w86400_sum_25bf106eeade2e255561c765500361f0a061d889" AS DOUBLE) AS "sum_1d_gt_100",
  {{ CURRENT_TIMESTAMP }} AS "POINT_IN_TIME"
FROM _FB_AGGREGATED AS AGG