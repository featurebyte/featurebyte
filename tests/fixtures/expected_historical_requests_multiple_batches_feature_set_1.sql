CREATE TABLE "__TEMP_646f1b781d1e7970788b32ec_1" AS
WITH _FB_AGGREGATED AS (
  SELECT
    REQ."__FB_ROW_INDEX_FOR_JOIN" AS "__FB_ROW_INDEX_FOR_JOIN",
    REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
    REQ."CUSTOMER_ID" AS "CUSTOMER_ID",
    REQ."_fb_internal_latest_3b3c2a8389d7720826731fefb7060b6578050e04" AS "_fb_internal_latest_3b3c2a8389d7720826731fefb7060b6578050e04",
    REQ."_fb_internal_lookup_membership_status_input_2" AS "_fb_internal_lookup_membership_status_input_2"
  FROM (
    SELECT
      L."__FB_ROW_INDEX_FOR_JOIN" AS "__FB_ROW_INDEX_FOR_JOIN",
      L."POINT_IN_TIME" AS "POINT_IN_TIME",
      L."CUSTOMER_ID" AS "CUSTOMER_ID",
      L."_fb_internal_latest_3b3c2a8389d7720826731fefb7060b6578050e04" AS "_fb_internal_latest_3b3c2a8389d7720826731fefb7060b6578050e04",
      R."membership_status" AS "_fb_internal_lookup_membership_status_input_2"
    FROM (
      SELECT
        "__FB_KEY_COL_0",
        "__FB_LAST_TS",
        "__FB_ROW_INDEX_FOR_JOIN",
        "POINT_IN_TIME",
        "CUSTOMER_ID",
        "_fb_internal_latest_3b3c2a8389d7720826731fefb7060b6578050e04"
      FROM (
        SELECT
          "__FB_KEY_COL_0",
          LAG("__FB_EFFECTIVE_TS_COL") IGNORE NULLS OVER (PARTITION BY "__FB_KEY_COL_0" ORDER BY "__FB_TS_COL", "__FB_TS_TIE_BREAKER_COL") AS "__FB_LAST_TS",
          "__FB_ROW_INDEX_FOR_JOIN",
          "POINT_IN_TIME",
          "CUSTOMER_ID",
          "_fb_internal_latest_3b3c2a8389d7720826731fefb7060b6578050e04",
          "__FB_EFFECTIVE_TS_COL"
        FROM (
          SELECT
            CAST(CONVERT_TIMEZONE('UTC', "POINT_IN_TIME") AS TIMESTAMP) AS "__FB_TS_COL",
            "CUSTOMER_ID" AS "__FB_KEY_COL_0",
            NULL AS "__FB_EFFECTIVE_TS_COL",
            2 AS "__FB_TS_TIE_BREAKER_COL",
            "__FB_ROW_INDEX_FOR_JOIN" AS "__FB_ROW_INDEX_FOR_JOIN",
            "POINT_IN_TIME" AS "POINT_IN_TIME",
            "CUSTOMER_ID" AS "CUSTOMER_ID",
            "_fb_internal_latest_3b3c2a8389d7720826731fefb7060b6578050e04" AS "_fb_internal_latest_3b3c2a8389d7720826731fefb7060b6578050e04"
          FROM (
            SELECT
              REQ."__FB_ROW_INDEX_FOR_JOIN" AS "__FB_ROW_INDEX_FOR_JOIN",
              REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
              REQ."CUSTOMER_ID" AS "CUSTOMER_ID",
              REQ."_fb_internal_latest_3b3c2a8389d7720826731fefb7060b6578050e04" AS "_fb_internal_latest_3b3c2a8389d7720826731fefb7060b6578050e04"
            FROM (
              SELECT
                L."__FB_ROW_INDEX_FOR_JOIN" AS "__FB_ROW_INDEX_FOR_JOIN",
                L."POINT_IN_TIME" AS "POINT_IN_TIME",
                L."CUSTOMER_ID" AS "CUSTOMER_ID",
                R.value_latest_3b3c2a8389d7720826731fefb7060b6578050e04 AS "_fb_internal_latest_3b3c2a8389d7720826731fefb7060b6578050e04"
              FROM (
                SELECT
                  "__FB_KEY_COL_0",
                  "__FB_KEY_COL_1",
                  "__FB_LAST_TS",
                  "__FB_ROW_INDEX_FOR_JOIN",
                  "POINT_IN_TIME",
                  "CUSTOMER_ID"
                FROM (
                  SELECT
                    "__FB_KEY_COL_0",
                    "__FB_KEY_COL_1",
                    LAG("__FB_EFFECTIVE_TS_COL") IGNORE NULLS OVER (PARTITION BY "__FB_KEY_COL_0", "__FB_KEY_COL_1" ORDER BY "__FB_TS_COL", "__FB_TS_TIE_BREAKER_COL") AS "__FB_LAST_TS",
                    "__FB_ROW_INDEX_FOR_JOIN",
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
                      "__FB_ROW_INDEX_FOR_JOIN" AS "__FB_ROW_INDEX_FOR_JOIN",
                      "POINT_IN_TIME" AS "POINT_IN_TIME",
                      "CUSTOMER_ID" AS "CUSTOMER_ID"
                    FROM (
                      SELECT
                        REQ."__FB_ROW_INDEX_FOR_JOIN",
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
                      NULL AS "__FB_ROW_INDEX_FOR_JOIN",
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
          )
          UNION ALL
          SELECT
            CAST(CONVERT_TIMEZONE('UTC', "event_timestamp") AS TIMESTAMP) AS "__FB_TS_COL",
            "cust_id" AS "__FB_KEY_COL_0",
            "event_timestamp" AS "__FB_EFFECTIVE_TS_COL",
            1 AS "__FB_TS_TIE_BREAKER_COL",
            NULL AS "__FB_ROW_INDEX_FOR_JOIN",
            NULL AS "POINT_IN_TIME",
            NULL AS "CUSTOMER_ID",
            NULL AS "_fb_internal_latest_3b3c2a8389d7720826731fefb7060b6578050e04"
          FROM (
            SELECT
              "effective_ts" AS "effective_ts",
              "cust_id" AS "cust_id",
              "membership_status" AS "membership_status"
            FROM "db"."public"."customer_profile_table"
          )
        )
      )
      WHERE
        "__FB_EFFECTIVE_TS_COL" IS NULL
    ) AS L
    LEFT JOIN (
      SELECT
        "effective_ts" AS "effective_ts",
        "cust_id" AS "cust_id",
        "membership_status" AS "membership_status"
      FROM "db"."public"."customer_profile_table"
    ) AS R
      ON L."__FB_LAST_TS" = R."event_timestamp" AND L."__FB_KEY_COL_0" = R."cust_id"
  ) AS REQ
)
SELECT
  AGG."__FB_ROW_INDEX_FOR_JOIN",
  AGG."POINT_IN_TIME",
  AGG."CUSTOMER_ID",
  "_fb_internal_latest_3b3c2a8389d7720826731fefb7060b6578050e04" AS "a_latest_value",
  "_fb_internal_lookup_membership_status_input_2" AS "Current Membership Status"
FROM _FB_AGGREGATED AS AGG
