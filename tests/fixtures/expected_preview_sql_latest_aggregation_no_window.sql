WITH REQUEST_TABLE AS (
  SELECT
    CAST('2022-04-20 10:00:00' AS TIMESTAMP) AS "POINT_IN_TIME",
    'C1' AS "CUSTOMER_ID"
), TILE_F3600_M1800_B900_AF1FD0AEE34EC80A96A6D5A486CE40F5A2267B4E AS (
  SELECT
    latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78.INDEX,
    latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78."cust_id",
    latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78."biz_id",
    value_latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78
  FROM (
    WITH __FB_ENTITY_TABLE_NAME AS (
      SELECT
        "CUSTOMER_ID" AS "cust_id",
        "BUSINESS_ID" AS "biz_id",
        CAST(FLOOR((
          DATE_PART(EPOCH_SECOND, MAX(POINT_IN_TIME)) - 1800
        ) / 3600) * 3600 + 1800 - 900 AS TIMESTAMP) AS __FB_ENTITY_TABLE_END_DATE,
        DATEADD(
          MICROSECOND,
          (
            (
              1800 - 900
            ) * CAST(1000000 AS BIGINT) / CAST(1 AS BIGINT)
          ),
          CAST('1970-01-01' AS TIMESTAMP)
        ) AS __FB_ENTITY_TABLE_START_DATE
      FROM "REQUEST_TABLE"
      GROUP BY
        "CUSTOMER_ID",
        "BUSINESS_ID"
    ), __FB_TILE_COMPUTE_INPUT_TABLE_NAME AS (
      SELECT
        R.*
      FROM __FB_ENTITY_TABLE_NAME
      INNER JOIN (
        SELECT
          "ts" AS "ts",
          "cust_id" AS "cust_id",
          "biz_id" AS "biz_id",
          "a" AS "input_col_latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78"
        FROM "db"."public"."event_table"
      ) AS R
        ON R."cust_id" = __FB_ENTITY_TABLE_NAME."cust_id"
        AND R."biz_id" = __FB_ENTITY_TABLE_NAME."biz_id"
        AND R."ts" >= __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_START_DATE
        AND R."ts" < __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_END_DATE
    )
    SELECT
      index,
      "cust_id",
      "biz_id",
      value_latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78
    FROM (
      SELECT
        index,
        "cust_id",
        "biz_id",
        ROW_NUMBER() OVER (PARTITION BY index, "cust_id", "biz_id" ORDER BY "ts" DESC NULLS LAST) AS "__FB_ROW_NUMBER",
        FIRST_VALUE("input_col_latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78") OVER (PARTITION BY index, "cust_id", "biz_id" ORDER BY "ts" DESC NULLS LAST) AS value_latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78
      FROM (
        SELECT
          *,
          F_TIMESTAMP_TO_INDEX(CONVERT_TIMEZONE('UTC', "ts"), 1800, 900, 60) AS index
        FROM __FB_TILE_COMPUTE_INPUT_TABLE_NAME
      )
    )
    WHERE
      "__FB_ROW_NUMBER" = 1
  ) AS latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78
), _FB_AGGREGATED AS (
  SELECT
    REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
    REQ."CUSTOMER_ID" AS "CUSTOMER_ID",
    REQ."_fb_internal_CUSTOMER_ID_BUSINESS_ID_latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78" AS "_fb_internal_CUSTOMER_ID_BUSINESS_ID_latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78"
  FROM (
    SELECT
      L."POINT_IN_TIME" AS "POINT_IN_TIME",
      L."CUSTOMER_ID" AS "CUSTOMER_ID",
      R.value_latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78 AS "_fb_internal_CUSTOMER_ID_BUSINESS_ID_latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78"
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
          LAG("__FB_EFFECTIVE_TS_COL") IGNORE NULLS OVER (PARTITION BY "__FB_KEY_COL_0", "__FB_KEY_COL_1" ORDER BY "__FB_TS_COL" NULLS FIRST, "__FB_TS_TIE_BREAKER_COL") AS "__FB_LAST_TS",
          "POINT_IN_TIME",
          "CUSTOMER_ID",
          "__FB_EFFECTIVE_TS_COL"
        FROM (
          SELECT
            CAST(FLOOR((
              DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
            ) / 3600) AS BIGINT) AS "__FB_TS_COL",
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
  CAST("_fb_internal_CUSTOMER_ID_BUSINESS_ID_latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78" AS DOUBLE) AS "a_latest_value"
FROM _FB_AGGREGATED AS AGG
