WITH __FB_ENTITY_TABLE_NAME AS (
  __FB_ENTITY_TABLE_SQL_PLACEHOLDER
), __FB_TILE_COMPUTE_INPUT_TABLE_NAME AS (
  SELECT
    R.*
  FROM __FB_ENTITY_TABLE_NAME
  INNER JOIN (
    SELECT
      L."event_timestamp" AS "event_timestamp",
      L."cust_id" AS "cust_id",
      (
        L."col_float" + R."col_float"
      ) AS "input_col_sum_93a5151941c8e5a6d2c8102928d939b8622f6320"
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
      FROM (
        SELECT
          R.*
        FROM __FB_ENTITY_TABLE_NAME
        INNER JOIN (
          SELECT
            "col_int",
            "col_float",
            "col_char",
            "col_text",
            "col_binary",
            "col_boolean",
            "event_timestamp",
            "created_at",
            "cust_id"
          FROM "sf_database"."sf_schema"."sf_table"
          WHERE
            "col_text" >= TO_CHAR(CAST('2023-01-01 00:00:00' AS TIMESTAMP), '%Y-%m-%d %H:%M:%S')
            AND "col_text" <= TO_CHAR(CAST('2023-06-01 00:00:00' AS TIMESTAMP), '%Y-%m-%d %H:%M:%S')
        ) AS R
          ON R."cust_id" = __FB_ENTITY_TABLE_NAME."cust_id"
      )
    ) AS L
    LEFT JOIN (
      SELECT
        "col_int",
        ANY_VALUE("col_float") AS "col_float",
        ANY_VALUE("col_char") AS "col_char",
        ANY_VALUE("col_text") AS "col_text",
        ANY_VALUE("col_binary") AS "col_binary",
        ANY_VALUE("col_boolean") AS "col_boolean",
        ANY_VALUE("event_timestamp") AS "event_timestamp",
        ANY_VALUE("cust_id") AS "cust_id"
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
      )
      GROUP BY
        "col_int"
    ) AS R
      ON L."col_int" = R."col_int"
  ) AS R
    ON R."cust_id" = __FB_ENTITY_TABLE_NAME."cust_id"
    AND R."event_timestamp" >= __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_START_DATE
    AND R."event_timestamp" < __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_END_DATE
)
SELECT
  index,
  "cust_id",
  SUM("input_col_sum_93a5151941c8e5a6d2c8102928d939b8622f6320") AS value_sum_93a5151941c8e5a6d2c8102928d939b8622f6320
FROM (
  SELECT
    *,
    F_TIMESTAMP_TO_INDEX(CAST(CONVERT_TIMEZONE('UTC', "event_timestamp") AS TIMESTAMP), 180, 90, 15) AS index
  FROM __FB_TILE_COMPUTE_INPUT_TABLE_NAME
)
GROUP BY
  index,
  "cust_id"
