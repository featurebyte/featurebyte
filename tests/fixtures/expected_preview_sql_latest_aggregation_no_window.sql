WITH REQUEST_TABLE AS (
  SELECT
    CAST('2022-04-20 10:00:00' AS TIMESTAMPNTZ) AS "POINT_IN_TIME",
    'C1' AS "CUSTOMER_ID"
), TILE_F3600_M1800_B900_AF1FD0AEE34EC80A96A6D5A486CE40F5A2267B4E AS (
  SELECT
    latest_3b3c2a8389d7720826731fefb7060b6578050e04.INDEX,
    latest_3b3c2a8389d7720826731fefb7060b6578050e04."cust_id",
    latest_3b3c2a8389d7720826731fefb7060b6578050e04."biz_id",
    value_latest_3b3c2a8389d7720826731fefb7060b6578050e04
  FROM (
    SELECT
      index,
      "cust_id",
      "biz_id",
      value_latest_3b3c2a8389d7720826731fefb7060b6578050e04
    FROM (
      SELECT
        index,
        "cust_id",
        "biz_id",
        ROW_NUMBER() OVER (PARTITION BY index, "cust_id", "biz_id" ORDER BY "ts" DESC NULLS LAST) AS "__FB_ROW_NUMBER",
        FIRST_VALUE("a") OVER (PARTITION BY index, "cust_id", "biz_id" ORDER BY "ts" DESC NULLS LAST) AS value_latest_3b3c2a8389d7720826731fefb7060b6578050e04
      FROM (
        SELECT
          *,
          F_TIMESTAMP_TO_INDEX(CONVERT_TIMEZONE('UTC', "ts"), 1800, 900, 60) AS index
        FROM (
          WITH __FB_ENTITY_TABLE_NAME AS (
            (
              SELECT
                "CUSTOMER_ID" AS "cust_id",
                "BUSINESS_ID" AS "biz_id",
                TO_TIMESTAMP(
                  FLOOR((
                    DATE_PART(EPOCH_SECOND, MAX(POINT_IN_TIME)) - 1800
                  ) / 3600) * 3600 + 1800 - 900
                ) AS "__FB_ENTITY_TABLE_END_DATE",
                DATEADD(
                  microsecond,
                  (
                    (
                      1800 - 900
                    ) * CAST(1000000 AS BIGINT) / CAST(1 AS BIGINT)
                  ),
                  CAST('1970-01-01' AS TIMESTAMPNTZ)
                ) AS "__FB_ENTITY_TABLE_START_DATE"
              FROM "REQUEST_TABLE"
              GROUP BY
                "CUSTOMER_ID",
                "BUSINESS_ID"
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
              "b" AS "b"
            FROM "db"."public"."event_table"
          ) AS R
            ON R."cust_id" = __FB_ENTITY_TABLE_NAME."cust_id"
            AND R."biz_id" = __FB_ENTITY_TABLE_NAME."biz_id"
            AND R."ts" >= __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_START_DATE
            AND R."ts" < __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_END_DATE
        )
      )
    )
    WHERE
      "__FB_ROW_NUMBER" = 1
  ) AS latest_3b3c2a8389d7720826731fefb7060b6578050e04
), _FB_AGGREGATED AS (
  SELECT
    REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
    REQ."CUSTOMER_ID" AS "CUSTOMER_ID",
    REQ."_fb_internal_latest_3b3c2a8389d7720826731fefb7060b6578050e04" AS "_fb_internal_latest_3b3c2a8389d7720826731fefb7060b6578050e04"
  FROM (
    SELECT
      L."POINT_IN_TIME" AS "POINT_IN_TIME",
      L."CUSTOMER_ID" AS "CUSTOMER_ID",
      R.value_latest_3b3c2a8389d7720826731fefb7060b6578050e04 AS "_fb_internal_latest_3b3c2a8389d7720826731fefb7060b6578050e04"
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
  "_fb_internal_latest_3b3c2a8389d7720826731fefb7060b6578050e04" AS "a_latest_value"
FROM _FB_AGGREGATED AS AGG
