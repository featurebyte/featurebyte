WITH TILE_F3600_M1800_B900_AF1FD0AEE34EC80A96A6D5A486CE40F5A2267B4E AS (
  SELECT
    latest_6ba5affa84771d1b3e01284bc5301186a8828c7a.INDEX,
    latest_6ba5affa84771d1b3e01284bc5301186a8828c7a."cust_id",
    latest_6ba5affa84771d1b3e01284bc5301186a8828c7a."biz_id",
    value_latest_6ba5affa84771d1b3e01284bc5301186a8828c7a
  FROM (
    SELECT
      *,
      F_TIMESTAMP_TO_INDEX(__FB_TILE_START_DATE_COLUMN, 1800, 900, 60) AS "INDEX"
    FROM (
      SELECT
        __FB_TILE_START_DATE_COLUMN,
        "cust_id",
        "biz_id",
        value_latest_6ba5affa84771d1b3e01284bc5301186a8828c7a
      FROM (
        SELECT
          TO_TIMESTAMP(
            DATE_PART(EPOCH_SECOND, CAST('1970-01-01 00:15:00' AS TIMESTAMPNTZ)) + tile_index * 3600
          ) AS __FB_TILE_START_DATE_COLUMN,
          "cust_id",
          "biz_id",
          ROW_NUMBER() OVER (PARTITION BY tile_index, "cust_id", "biz_id" ORDER BY "ts" DESC NULLS LAST) AS "__FB_ROW_NUMBER",
          FIRST_VALUE("a") OVER (PARTITION BY tile_index, "cust_id", "biz_id" ORDER BY "ts" DESC NULLS LAST) AS value_latest_6ba5affa84771d1b3e01284bc5301186a8828c7a
        FROM (
          SELECT
            *,
            FLOOR(
              (
                DATE_PART(EPOCH_SECOND, "ts") - DATE_PART(EPOCH_SECOND, CAST('1970-01-01 00:15:00' AS TIMESTAMPNTZ))
              ) / 3600
            ) AS tile_index
          FROM (
            SELECT
              *
            FROM (
              SELECT
                "ts" AS "ts",
                "cust_id" AS "cust_id",
                "a" AS "a",
                "b" AS "b"
              FROM "db"."public"."event_table"
            )
            WHERE
              "ts" >= CAST('1970-01-01 00:15:00' AS TIMESTAMPNTZ)
              AND "ts" < CAST('2022-04-20 09:15:00' AS TIMESTAMPNTZ)
          )
        )
      )
      WHERE
        "__FB_ROW_NUMBER" = 1
    )
  ) AS latest_6ba5affa84771d1b3e01284bc5301186a8828c7a
), REQUEST_TABLE AS (
  SELECT
    CAST('2022-04-20 10:00:00' AS TIMESTAMPNTZ) AS "POINT_IN_TIME",
    'C1' AS "CUSTOMER_ID"
), _FB_AGGREGATED AS (
  SELECT
    REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
    REQ."CUSTOMER_ID" AS "CUSTOMER_ID",
    REQ."_fb_internal_latest_6ba5affa84771d1b3e01284bc5301186a8828c7a" AS "_fb_internal_latest_6ba5affa84771d1b3e01284bc5301186a8828c7a"
  FROM (
    SELECT
      L."POINT_IN_TIME" AS "POINT_IN_TIME",
      L."CUSTOMER_ID" AS "CUSTOMER_ID",
      R.value_latest_6ba5affa84771d1b3e01284bc5301186a8828c7a AS "_fb_internal_latest_6ba5affa84771d1b3e01284bc5301186a8828c7a"
    FROM (
      SELECT
        "__FB_KEY_COL_0",
        "__FB_KEY_COL_1",
        "__FB_LAST_TS",
        "POINT_IN_TIME",
        "CUSTOMER_ID"
      FROM (
        SELECT
          "__FB_KEY_COL_0",
          "__FB_KEY_COL_1",
          LAG("__FB_EFFECTIVE_TS_COL") IGNORE NULLS OVER (PARTITION BY "__FB_KEY_COL_0", "__FB_KEY_COL_1" ORDER BY "__FB_TS_COL", "__FB_TS_TIE_BREAKER_COL") AS "__FB_LAST_TS",
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
            "POINT_IN_TIME" AS "POINT_IN_TIME",
            "CUSTOMER_ID" AS "CUSTOMER_ID"
          FROM (
            SELECT
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
SELECT
  AGG."POINT_IN_TIME",
  AGG."CUSTOMER_ID",
  "_fb_internal_latest_6ba5affa84771d1b3e01284bc5301186a8828c7a" AS "a_latest_value"
FROM _FB_AGGREGATED AS AGG
