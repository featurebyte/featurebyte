WITH REQUEST_TABLE AS (
  SELECT
    CAST('2022-04-20 10:00:00' AS TIMESTAMP) AS "POINT_IN_TIME",
    'C1' AS "CUSTOMER_ID"
), _FB_AGGREGATED AS (
  SELECT
    REQ."POINT_IN_TIME",
    REQ."CUSTOMER_ID",
    "T0"."cust_value_1_9b8bee3acf7d5bc7" AS "cust_value_1_9b8bee3acf7d5bc7",
    "T0"."cust_value_2_9b8bee3acf7d5bc7" AS "cust_value_2_9b8bee3acf7d5bc7"
  FROM REQUEST_TABLE AS REQ
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
)
SELECT
  AGG."POINT_IN_TIME",
  AGG."CUSTOMER_ID",
  (
    "cust_value_1_9b8bee3acf7d5bc7" + "cust_value_2_9b8bee3acf7d5bc7"
  ) AS "MY FEATURE"
FROM _FB_AGGREGATED AS AGG
