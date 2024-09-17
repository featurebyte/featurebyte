SELECT
  REQ."a" AS "a",
  REQ."b" AS "b",
  REQ."c" AS "c",
  REQ."_fb_internal_CUSTOMER_ID_BUSINESS_ID_latest_b4a6546e024f3a059bd67f454028e56c5a37826e" AS "_fb_internal_CUSTOMER_ID_BUSINESS_ID_latest_b4a6546e024f3a059bd67f454028e56c5a37826e"
FROM (
  SELECT
    L."a" AS "a",
    L."b" AS "b",
    L."c" AS "c",
    R.value_latest_b4a6546e024f3a059bd67f454028e56c5a37826e AS "_fb_internal_CUSTOMER_ID_BUSINESS_ID_latest_b4a6546e024f3a059bd67f454028e56c5a37826e"
  FROM (
    SELECT
      "__FB_KEY_COL_0",
      "__FB_KEY_COL_1",
      "__FB_LAST_TS",
      "a",
      "b",
      "c"
    FROM (
      SELECT
        "__FB_KEY_COL_0",
        "__FB_KEY_COL_1",
        LAG("__FB_EFFECTIVE_TS_COL") IGNORE NULLS OVER (PARTITION BY "__FB_KEY_COL_0", "__FB_KEY_COL_1" ORDER BY "__FB_TS_COL", "__FB_TS_TIE_BREAKER_COL" NULLS LAST) AS "__FB_LAST_TS",
        "a",
        "b",
        "c",
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
          "a" AS "a",
          "b" AS "b",
          "c" AS "c"
        FROM (
          SELECT
            a,
            b,
            c
          FROM my_table
        )
        UNION ALL
        SELECT
          "INDEX" AS "__FB_TS_COL",
          "cust_id" AS "__FB_KEY_COL_0",
          "biz_id" AS "__FB_KEY_COL_1",
          "INDEX" AS "__FB_EFFECTIVE_TS_COL",
          1 AS "__FB_TS_TIE_BREAKER_COL",
          NULL AS "a",
          NULL AS "b",
          NULL AS "c"
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
