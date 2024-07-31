SELECT
  COUNT(*) AS "count"
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
);

CREATE TABLE "__FB_CACHED_TABLE_000000000000000000000000" AS
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
FROM (
  SELECT
    CAST(BITAND(RANDOM(1234), 2147483647) AS DOUBLE) / 2147483647.0 AS "prob",
    "col_int",
    "col_float",
    "col_char",
    "col_text",
    "col_binary",
    "col_boolean",
    "event_timestamp",
    "created_at",
    "cust_id"
  FROM (
    SELECT
      "col_int" AS "col_int",
      "col_float" AS "col_float",
      "col_char" AS "col_char",
      "col_text" AS "col_text",
      "col_binary" AS "col_binary",
      "col_boolean" AS "col_boolean",
      IFF(
        "event_timestamp" < CAST('1900-01-01' AS TIMESTAMPNTZ)
        OR "event_timestamp" > CAST('2200-01-01' AS TIMESTAMPNTZ),
        NULL,
        "event_timestamp"
      ) AS "event_timestamp",
      IFF(
        "created_at" < CAST('1900-01-01' AS TIMESTAMPNTZ)
        OR "created_at" > CAST('2200-01-01' AS TIMESTAMPNTZ),
        NULL,
        "created_at"
      ) AS "created_at",
      "cust_id" AS "cust_id"
    FROM "sf_database"."sf_schema"."sf_table"
  )
)
WHERE
  "prob" <= 0.07500000000000001
ORDER BY
  "prob"
LIMIT 50000;

SELECT
  *
FROM "__FB_CACHED_TABLE_000000000000000000000000"
LIMIT 5;
