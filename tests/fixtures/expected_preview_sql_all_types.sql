WITH TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS (
  SELECT
    avg_833762b783166cd0980c65b9e3f3c7c6b9dcd489.INDEX,
    avg_833762b783166cd0980c65b9e3f3c7c6b9dcd489."cust_id",
    sum_value_avg_833762b783166cd0980c65b9e3f3c7c6b9dcd489,
    count_value_avg_833762b783166cd0980c65b9e3f3c7c6b9dcd489,
    value_latest_088635a8a233d93984ceb9acdaa23eaa1460f338
  FROM (
    SELECT
      *,
      F_TIMESTAMP_TO_INDEX(__FB_TILE_START_DATE_COLUMN, 1800, 900, 60) AS "INDEX"
    FROM (
      SELECT
        TO_TIMESTAMP(
          DATE_PART(EPOCH_SECOND, CAST('2022-01-20 09:15:00' AS TIMESTAMPNTZ)) + tile_index * 3600
        ) AS __FB_TILE_START_DATE_COLUMN,
        "cust_id",
        SUM("a") AS sum_value_avg_833762b783166cd0980c65b9e3f3c7c6b9dcd489,
        COUNT("a") AS count_value_avg_833762b783166cd0980c65b9e3f3c7c6b9dcd489
      FROM (
        SELECT
          *,
          FLOOR(
            (
              DATE_PART(EPOCH_SECOND, "ts") - DATE_PART(EPOCH_SECOND, CAST('2022-01-20 09:15:00' AS TIMESTAMPNTZ))
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
              "b" AS "b",
              (
                "a" + "b"
              ) AS "c"
            FROM "db"."public"."event_table"
          )
          WHERE
            "ts" >= CAST('2022-01-20 09:15:00' AS TIMESTAMPNTZ)
            AND "ts" < CAST('2022-04-20 09:15:00' AS TIMESTAMPNTZ)
        )
      )
      GROUP BY
        tile_index,
        "cust_id"
    )
  ) AS avg_833762b783166cd0980c65b9e3f3c7c6b9dcd489
  RIGHT JOIN (
    SELECT
      *,
      F_TIMESTAMP_TO_INDEX(__FB_TILE_START_DATE_COLUMN, 1800, 900, 60) AS "INDEX"
    FROM (
      SELECT
        __FB_TILE_START_DATE_COLUMN,
        "cust_id",
        value_latest_088635a8a233d93984ceb9acdaa23eaa1460f338
      FROM (
        SELECT
          TO_TIMESTAMP(
            DATE_PART(EPOCH_SECOND, CAST('2022-01-20 09:15:00' AS TIMESTAMPNTZ)) + tile_index * 3600
          ) AS __FB_TILE_START_DATE_COLUMN,
          "cust_id",
          ROW_NUMBER() OVER (PARTITION BY tile_index, "cust_id" ORDER BY "ts" DESC NULLS LAST) AS "__FB_ROW_NUMBER",
          FIRST_VALUE("a") OVER (PARTITION BY tile_index, "cust_id" ORDER BY "ts" DESC NULLS LAST) AS value_latest_088635a8a233d93984ceb9acdaa23eaa1460f338
        FROM (
          SELECT
            *,
            FLOOR(
              (
                DATE_PART(EPOCH_SECOND, "ts") - DATE_PART(EPOCH_SECOND, CAST('2022-01-20 09:15:00' AS TIMESTAMPNTZ))
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
              "ts" >= CAST('2022-01-20 09:15:00' AS TIMESTAMPNTZ)
              AND "ts" < CAST('2022-04-20 09:15:00' AS TIMESTAMPNTZ)
          )
        )
      )
      WHERE
        "__FB_ROW_NUMBER" = 1
    )
  ) AS latest_088635a8a233d93984ceb9acdaa23eaa1460f338
    ON avg_833762b783166cd0980c65b9e3f3c7c6b9dcd489.INDEX = latest_088635a8a233d93984ceb9acdaa23eaa1460f338.INDEX
    AND avg_833762b783166cd0980c65b9e3f3c7c6b9dcd489."cust_id" = latest_088635a8a233d93984ceb9acdaa23eaa1460f338."cust_id"
), TILE_F3600_M1800_B900_AF1FD0AEE34EC80A96A6D5A486CE40F5A2267B4E AS (
  SELECT
    latest_bdf76e38d5a0186a5b23c57ce5e4f5d6549d3ab0.INDEX,
    latest_bdf76e38d5a0186a5b23c57ce5e4f5d6549d3ab0."cust_id",
    latest_bdf76e38d5a0186a5b23c57ce5e4f5d6549d3ab0."biz_id",
    value_latest_bdf76e38d5a0186a5b23c57ce5e4f5d6549d3ab0
  FROM (
    SELECT
      *,
      F_TIMESTAMP_TO_INDEX(__FB_TILE_START_DATE_COLUMN, 1800, 900, 60) AS "INDEX"
    FROM (
      SELECT
        __FB_TILE_START_DATE_COLUMN,
        "cust_id",
        "biz_id",
        value_latest_bdf76e38d5a0186a5b23c57ce5e4f5d6549d3ab0
      FROM (
        SELECT
          TO_TIMESTAMP(
            DATE_PART(EPOCH_SECOND, CAST('1970-01-01 00:15:00' AS TIMESTAMPNTZ)) + tile_index * 3600
          ) AS __FB_TILE_START_DATE_COLUMN,
          "cust_id",
          "biz_id",
          ROW_NUMBER() OVER (PARTITION BY tile_index, "cust_id", "biz_id" ORDER BY "ts" DESC NULLS LAST) AS "__FB_ROW_NUMBER",
          FIRST_VALUE("a") OVER (PARTITION BY tile_index, "cust_id", "biz_id" ORDER BY "ts" DESC NULLS LAST) AS value_latest_bdf76e38d5a0186a5b23c57ce5e4f5d6549d3ab0
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
  ) AS latest_bdf76e38d5a0186a5b23c57ce5e4f5d6549d3ab0
), REQUEST_TABLE AS (
  SELECT
    CAST('2022-04-20 10:00:00' AS TIMESTAMPNTZ) AS "POINT_IN_TIME",
    'C1' AS "CUSTOMER_ID"
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
), "REQUEST_TABLE_W7776000_F3600_BS900_M1800_CUSTOMER_ID" AS (
  SELECT
    "POINT_IN_TIME",
    "CUSTOMER_ID",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 3600) AS "__FB_LAST_TILE_INDEX",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 3600) - 2160 AS "__FB_FIRST_TILE_INDEX"
  FROM (
    SELECT DISTINCT
      "POINT_IN_TIME",
      "CUSTOMER_ID"
    FROM REQUEST_TABLE
  )
), "REQUEST_TABLE_order_id" AS (
  SELECT DISTINCT
    "order_id"
  FROM REQUEST_TABLE
), "REQUEST_TABLE_POINT_IN_TIME_MEMBERSHIP_STATUS" AS (
  SELECT DISTINCT
    "POINT_IN_TIME",
    "MEMBERSHIP_STATUS"
  FROM REQUEST_TABLE
), _FB_AGGREGATED AS (
  SELECT
    REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
    REQ."CUSTOMER_ID" AS "CUSTOMER_ID",
    REQ."agg_latest_bdf76e38d5a0186a5b23c57ce5e4f5d6549d3ab0" AS "agg_latest_bdf76e38d5a0186a5b23c57ce5e4f5d6549d3ab0",
    REQ."membership_status_fbfdb013880b3a67" AS "membership_status_fbfdb013880b3a67",
    "T0"."cust_value_1_9b8bee3acf7d5bc7" AS "cust_value_1_9b8bee3acf7d5bc7",
    "T0"."cust_value_2_9b8bee3acf7d5bc7" AS "cust_value_2_9b8bee3acf7d5bc7",
    "T1"."agg_w7200_avg_833762b783166cd0980c65b9e3f3c7c6b9dcd489" AS "agg_w7200_avg_833762b783166cd0980c65b9e3f3c7c6b9dcd489",
    "T2"."agg_w172800_avg_833762b783166cd0980c65b9e3f3c7c6b9dcd489" AS "agg_w172800_avg_833762b783166cd0980c65b9e3f3c7c6b9dcd489",
    "T3"."agg_w7776000_latest_088635a8a233d93984ceb9acdaa23eaa1460f338" AS "agg_w7776000_latest_088635a8a233d93984ceb9acdaa23eaa1460f338",
    "T4"."order_size" AS "order_size",
    "T5"."count_None_73d3277c62d87bcc" AS "count_None_73d3277c62d87bcc"
  FROM (
    SELECT
      L."POINT_IN_TIME" AS "POINT_IN_TIME",
      L."CUSTOMER_ID" AS "CUSTOMER_ID",
      L."agg_latest_bdf76e38d5a0186a5b23c57ce5e4f5d6549d3ab0" AS "agg_latest_bdf76e38d5a0186a5b23c57ce5e4f5d6549d3ab0",
      R."membership_status" AS "membership_status_fbfdb013880b3a67"
    FROM (
      SELECT
        "__FB_KEY_COL_0",
        "__FB_LAST_TS",
        "POINT_IN_TIME",
        "CUSTOMER_ID",
        "agg_latest_bdf76e38d5a0186a5b23c57ce5e4f5d6549d3ab0"
      FROM (
        SELECT
          "__FB_KEY_COL_0",
          LAG("__FB_EFFECTIVE_TS_COL") IGNORE NULLS OVER (PARTITION BY "__FB_KEY_COL_0" ORDER BY "__FB_TS_COL", "__FB_TS_TIE_BREAKER_COL") AS "__FB_LAST_TS",
          "POINT_IN_TIME",
          "CUSTOMER_ID",
          "agg_latest_bdf76e38d5a0186a5b23c57ce5e4f5d6549d3ab0",
          "__FB_EFFECTIVE_TS_COL"
        FROM (
          SELECT
            CAST(CONVERT_TIMEZONE('UTC', "POINT_IN_TIME") AS TIMESTAMP) AS "__FB_TS_COL",
            "CUSTOMER_ID" AS "__FB_KEY_COL_0",
            NULL AS "__FB_EFFECTIVE_TS_COL",
            2 AS "__FB_TS_TIE_BREAKER_COL",
            "POINT_IN_TIME" AS "POINT_IN_TIME",
            "CUSTOMER_ID" AS "CUSTOMER_ID",
            "agg_latest_bdf76e38d5a0186a5b23c57ce5e4f5d6549d3ab0" AS "agg_latest_bdf76e38d5a0186a5b23c57ce5e4f5d6549d3ab0"
          FROM (
            SELECT
              REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
              REQ."CUSTOMER_ID" AS "CUSTOMER_ID",
              REQ."agg_latest_bdf76e38d5a0186a5b23c57ce5e4f5d6549d3ab0" AS "agg_latest_bdf76e38d5a0186a5b23c57ce5e4f5d6549d3ab0"
            FROM (
              SELECT
                L."POINT_IN_TIME" AS "POINT_IN_TIME",
                L."CUSTOMER_ID" AS "CUSTOMER_ID",
                R.value_latest_bdf76e38d5a0186a5b23c57ce5e4f5d6549d3ab0 AS "agg_latest_bdf76e38d5a0186a5b23c57ce5e4f5d6549d3ab0"
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
          UNION ALL
          SELECT
            CAST(CONVERT_TIMEZONE('UTC', "event_timestamp") AS TIMESTAMP) AS "__FB_TS_COL",
            "cust_id" AS "__FB_KEY_COL_0",
            "event_timestamp" AS "__FB_EFFECTIVE_TS_COL",
            1 AS "__FB_TS_TIE_BREAKER_COL",
            NULL AS "POINT_IN_TIME",
            NULL AS "CUSTOMER_ID",
            NULL AS "agg_latest_bdf76e38d5a0186a5b23c57ce5e4f5d6549d3ab0"
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
  LEFT JOIN (
    SELECT
      "cust_id" AS "CUSTOMER_ID",
      "cust_value_1" AS "cust_value_1_9b8bee3acf7d5bc7",
      "cust_value_2" AS "cust_value_2_9b8bee3acf7d5bc7"
    FROM (
      SELECT
        "cust_id" AS "cust_id",
        "cust_value_1" AS "cust_value_1",
        "cust_value_2" AS "cust_value_2"
      FROM "db"."public"."dimension_table"
    )
  ) AS T0
    ON REQ."CUSTOMER_ID" = T0."CUSTOMER_ID"
  LEFT JOIN (
    SELECT
      REQ."POINT_IN_TIME",
      REQ."CUSTOMER_ID",
      SUM(sum_value_avg_833762b783166cd0980c65b9e3f3c7c6b9dcd489) / SUM(count_value_avg_833762b783166cd0980c65b9e3f3c7c6b9dcd489) AS "agg_w7200_avg_833762b783166cd0980c65b9e3f3c7c6b9dcd489"
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
  ) AS T1
    ON REQ."POINT_IN_TIME" = T1."POINT_IN_TIME" AND REQ."CUSTOMER_ID" = T1."CUSTOMER_ID"
  LEFT JOIN (
    SELECT
      REQ."POINT_IN_TIME",
      REQ."CUSTOMER_ID",
      SUM(sum_value_avg_833762b783166cd0980c65b9e3f3c7c6b9dcd489) / SUM(count_value_avg_833762b783166cd0980c65b9e3f3c7c6b9dcd489) AS "agg_w172800_avg_833762b783166cd0980c65b9e3f3c7c6b9dcd489"
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
  ) AS T2
    ON REQ."POINT_IN_TIME" = T2."POINT_IN_TIME" AND REQ."CUSTOMER_ID" = T2."CUSTOMER_ID"
  LEFT JOIN (
    SELECT
      *
    FROM (
      SELECT
        REQ."POINT_IN_TIME",
        REQ."CUSTOMER_ID",
        ROW_NUMBER() OVER (PARTITION BY REQ."POINT_IN_TIME", REQ."CUSTOMER_ID" ORDER BY TILE.INDEX DESC NULLS LAST) AS "__FB_ROW_NUMBER",
        FIRST_VALUE(value_latest_088635a8a233d93984ceb9acdaa23eaa1460f338) OVER (PARTITION BY REQ."POINT_IN_TIME", REQ."CUSTOMER_ID" ORDER BY TILE.INDEX DESC NULLS LAST) AS "agg_w7776000_latest_088635a8a233d93984ceb9acdaa23eaa1460f338"
      FROM "REQUEST_TABLE_W7776000_F3600_BS900_M1800_CUSTOMER_ID" AS REQ
      INNER JOIN TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS TILE
        ON (
          FLOOR(REQ.__FB_LAST_TILE_INDEX / 2160) = FLOOR(TILE.INDEX / 2160)
          OR FLOOR(REQ.__FB_LAST_TILE_INDEX / 2160) - 1 = FLOOR(TILE.INDEX / 2160)
        )
        AND REQ."CUSTOMER_ID" = TILE."cust_id"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
    )
    WHERE
      "__FB_ROW_NUMBER" = 1
  ) AS T3
    ON REQ."POINT_IN_TIME" = T3."POINT_IN_TIME" AND REQ."CUSTOMER_ID" = T3."CUSTOMER_ID"
  LEFT JOIN (
    SELECT
      ITEM_AGG."order_size" AS "order_size",
      REQ."order_id" AS "order_id"
    FROM "REQUEST_TABLE_order_id" AS REQ
    INNER JOIN (
      SELECT
        "order_id",
        COUNT(*) AS "order_size"
      FROM (
        SELECT
          "order_id" AS "order_id",
          "item_id" AS "item_id",
          "item_name" AS "item_name",
          "item_type" AS "item_type"
        FROM "db"."public"."item_table"
      )
      GROUP BY
        "order_id"
    ) AS ITEM_AGG
      ON REQ."order_id" = ITEM_AGG."order_id"
  ) AS T4
    ON REQ."order_id" = T4."order_id"
  LEFT JOIN (
    SELECT
      REQ."POINT_IN_TIME",
      REQ."MEMBERSHIP_STATUS",
      COUNT(*) AS "count_None_73d3277c62d87bcc"
    FROM "REQUEST_TABLE_POINT_IN_TIME_MEMBERSHIP_STATUS" AS REQ
    INNER JOIN (
      SELECT
        *,
        LEAD("effective_ts") OVER (PARTITION BY "cust_id" ORDER BY "effective_ts") AS "__FB_END_TS"
      FROM (
        SELECT
          "effective_ts" AS "effective_ts",
          "cust_id" AS "cust_id",
          "membership_status" AS "membership_status"
        FROM "db"."public"."customer_profile_table"
      )
    ) AS SCD
      ON REQ."MEMBERSHIP_STATUS" = SCD."membership_status"
      AND (
        SCD."effective_ts" <= REQ."POINT_IN_TIME"
        AND (
          SCD."__FB_END_TS" > REQ."POINT_IN_TIME" OR SCD."__FB_END_TS" IS NULL
        )
      )
    GROUP BY
      REQ."POINT_IN_TIME",
      REQ."MEMBERSHIP_STATUS"
  ) AS T5
    ON REQ."POINT_IN_TIME" = T5."POINT_IN_TIME"
    AND REQ."MEMBERSHIP_STATUS" = T5."MEMBERSHIP_STATUS"
)
SELECT
  AGG."POINT_IN_TIME",
  AGG."CUSTOMER_ID",
  "agg_w7200_avg_833762b783166cd0980c65b9e3f3c7c6b9dcd489" AS "a_2h_average",
  "agg_w172800_avg_833762b783166cd0980c65b9e3f3c7c6b9dcd489" AS "a_48h_average",
  "order_size" AS "order_size",
  (
    "cust_value_1_9b8bee3acf7d5bc7" + "cust_value_2_9b8bee3acf7d5bc7"
  ) AS "MY FEATURE",
  "membership_status_fbfdb013880b3a67" AS "Current Membership Status",
  "agg_w7776000_latest_088635a8a233d93984ceb9acdaa23eaa1460f338" AS "a_latest_value_past_90d",
  "agg_latest_bdf76e38d5a0186a5b23c57ce5e4f5d6549d3ab0" AS "a_latest_value",
  "count_None_73d3277c62d87bcc" AS "asat_feature"
FROM _FB_AGGREGATED AS AGG
