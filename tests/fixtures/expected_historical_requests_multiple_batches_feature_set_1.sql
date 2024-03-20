CREATE TABLE "__TEMP_646f1b781d1e7970788b32ec_1" AS
WITH _FB_AGGREGATED AS (
  SELECT
    REQ."__FB_TABLE_ROW_INDEX" AS "__FB_TABLE_ROW_INDEX",
    REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
    REQ."CUSTOMER_ID" AS "CUSTOMER_ID",
    REQ."_fb_internal_CUSTOMER_ID_BUSINESS_ID_latest_b4a6546e024f3a059bd67f454028e56c5a37826e" AS "_fb_internal_CUSTOMER_ID_BUSINESS_ID_latest_b4a6546e024f3a059bd67f454028e56c5a37826e",
    "T0"."_fb_internal_CUSTOMER_ID_lookup_cust_value_1_input_2" AS "_fb_internal_CUSTOMER_ID_lookup_cust_value_1_input_2",
    "T0"."_fb_internal_CUSTOMER_ID_lookup_cust_value_2_input_2" AS "_fb_internal_CUSTOMER_ID_lookup_cust_value_2_input_2"
  FROM (
    SELECT
      L."__FB_TABLE_ROW_INDEX" AS "__FB_TABLE_ROW_INDEX",
      L."POINT_IN_TIME" AS "POINT_IN_TIME",
      L."CUSTOMER_ID" AS "CUSTOMER_ID",
      R.value_latest_b4a6546e024f3a059bd67f454028e56c5a37826e AS "_fb_internal_CUSTOMER_ID_BUSINESS_ID_latest_b4a6546e024f3a059bd67f454028e56c5a37826e"
    FROM (
      SELECT
        "__FB_KEY_COL_0",
        "__FB_KEY_COL_1",
        "__FB_LAST_TS",
        "__FB_TABLE_ROW_INDEX",
        "POINT_IN_TIME",
        "CUSTOMER_ID"
      FROM (
        SELECT
          "__FB_KEY_COL_0",
          "__FB_KEY_COL_1",
          LAG("__FB_EFFECTIVE_TS_COL") IGNORE NULLS OVER (PARTITION BY "__FB_KEY_COL_0", "__FB_KEY_COL_1" ORDER BY "__FB_TS_COL" NULLS FIRST, "__FB_TS_TIE_BREAKER_COL") AS "__FB_LAST_TS",
          "__FB_TABLE_ROW_INDEX",
          "POINT_IN_TIME",
          "CUSTOMER_ID",
          "__FB_EFFECTIVE_TS_COL"
        FROM (
          SELECT
            FLOOR((
              DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
            ) / 3600) AS "__FB_TS_COL",
            "CUSTOMER_ID" AS "__FB_KEY_COL_0",
            "BUSINESS_ID" AS "__FB_KEY_COL_1",
            NULL AS "__FB_EFFECTIVE_TS_COL",
            0 AS "__FB_TS_TIE_BREAKER_COL",
            "__FB_TABLE_ROW_INDEX" AS "__FB_TABLE_ROW_INDEX",
            "POINT_IN_TIME" AS "POINT_IN_TIME",
            "CUSTOMER_ID" AS "CUSTOMER_ID"
          FROM (
            SELECT
              REQ."__FB_TABLE_ROW_INDEX",
              REQ."POINT_IN_TIME",
              REQ."CUSTOMER_ID"
            FROM REQUEST_TABLE AS REQ
          )
          UNION ALL
          SELECT
            "INDEX" AS "__FB_TS_COL",
            "cust_id" AS "__FB_KEY_COL_0",
            "biz_id" AS "__FB_KEY_COL_1",
            "INDEX" AS "__FB_EFFECTIVE_TS_COL",
            1 AS "__FB_TS_TIE_BREAKER_COL",
            NULL AS "__FB_TABLE_ROW_INDEX",
            NULL AS "POINT_IN_TIME",
            NULL AS "CUSTOMER_ID"
          FROM TILE_F3600_M1800_B900_AF1FD0AEE34EC80A96A6D5A486CE40F5A2267B4E
        )
      )
      WHERE
        "__FB_EFFECTIVE_TS_COL" IS NULL
    ) AS L
    LEFT JOIN TILE_F3600_M1800_B900_AF1FD0AEE34EC80A96A6D5A486CE40F5A2267B4E AS R
      ON L."__FB_LAST_TS" = R."INDEX"
      AND L."__FB_KEY_COL_0" = R."cust_id"
      AND L."__FB_KEY_COL_1" = R."biz_id"
  ) AS REQ
  LEFT JOIN (
    SELECT
      "CUSTOMER_ID",
      ANY_VALUE("_fb_internal_CUSTOMER_ID_lookup_cust_value_1_input_2") AS "_fb_internal_CUSTOMER_ID_lookup_cust_value_1_input_2",
      ANY_VALUE("_fb_internal_CUSTOMER_ID_lookup_cust_value_2_input_2") AS "_fb_internal_CUSTOMER_ID_lookup_cust_value_2_input_2"
    FROM (
      SELECT
        "cust_id" AS "CUSTOMER_ID",
        "cust_value_1" AS "_fb_internal_CUSTOMER_ID_lookup_cust_value_1_input_2",
        "cust_value_2" AS "_fb_internal_CUSTOMER_ID_lookup_cust_value_2_input_2"
      FROM (
        SELECT
          "cust_id" AS "cust_id",
          "cust_value_1" AS "cust_value_1",
          "cust_value_2" AS "cust_value_2"
        FROM "db"."public"."dimension_table"
      )
    )
    GROUP BY
      "CUSTOMER_ID"
  ) AS T0
    ON REQ."CUSTOMER_ID" = T0."CUSTOMER_ID"
)
SELECT
  AGG."__FB_TABLE_ROW_INDEX",
  AGG."POINT_IN_TIME",
  AGG."CUSTOMER_ID",
  "_fb_internal_CUSTOMER_ID_BUSINESS_ID_latest_b4a6546e024f3a059bd67f454028e56c5a37826e" AS "a_latest_value",
  (
    "_fb_internal_CUSTOMER_ID_lookup_cust_value_1_input_2" + "_fb_internal_CUSTOMER_ID_lookup_cust_value_2_input_2"
  ) AS "MY FEATURE"
FROM _FB_AGGREGATED AS AGG
