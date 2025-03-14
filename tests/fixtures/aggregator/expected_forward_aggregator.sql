SELECT
  a,
  b,
  c,
  "T0"."_fb_internal_serving_cust_id_other_serving_key_forward_sum_value_cust_id_other_key_col_float_input_1" AS "_fb_internal_serving_cust_id_other_serving_key_forward_sum_value_cust_id_other_key_col_float_input_1"
FROM REQUEST_TABLE
LEFT JOIN (
  SELECT
    INNER_."POINT_IN_TIME",
    INNER_."serving_cust_id",
    INNER_."other_serving_key",
    OBJECT_AGG(
      CASE
        WHEN INNER_."col_float" IS NULL
        THEN '__MISSING__'
        ELSE CAST(INNER_."col_float" AS TEXT)
      END,
      TO_VARIANT(
        INNER_."_fb_internal_serving_cust_id_other_serving_key_forward_sum_value_cust_id_other_key_col_float_input_1_inner"
      )
    ) AS "_fb_internal_serving_cust_id_other_serving_key_forward_sum_value_cust_id_other_key_col_float_input_1"
  FROM (
    SELECT
      "POINT_IN_TIME",
      "serving_cust_id",
      "other_serving_key",
      "col_float",
      "_fb_internal_serving_cust_id_other_serving_key_forward_sum_value_cust_id_other_key_col_float_input_1_inner"
    FROM (
      SELECT
        "POINT_IN_TIME",
        "serving_cust_id",
        "other_serving_key",
        "col_float",
        "_fb_internal_serving_cust_id_other_serving_key_forward_sum_value_cust_id_other_key_col_float_input_1_inner",
        ROW_NUMBER() OVER (PARTITION BY "POINT_IN_TIME", "serving_cust_id", "other_serving_key" ORDER BY "_fb_internal_serving_cust_id_other_serving_key_forward_sum_value_cust_id_other_key_col_float_input_1_inner" DESC) AS "__fb_object_agg_row_number"
      FROM (
        SELECT
          REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
          REQ."serving_cust_id" AS "serving_cust_id",
          REQ."other_serving_key" AS "other_serving_key",
          SOURCE_TABLE."col_float" AS "col_float",
          SUM(SOURCE_TABLE."value") AS "_fb_internal_serving_cust_id_other_serving_key_forward_sum_value_cust_id_other_key_col_float_input_1_inner"
        FROM "REQUEST_TABLE_POINT_IN_TIME_serving_cust_id_other_serving_key" AS REQ
        INNER JOIN (
          SELECT
            *
          FROM tab
        ) AS SOURCE_TABLE
          ON (
            DATE_PART(EPOCH_SECOND, SOURCE_TABLE."timestamp_col") > DATE_PART(EPOCH_SECOND, REQ."POINT_IN_TIME")
            AND DATE_PART(EPOCH_SECOND, SOURCE_TABLE."timestamp_col") <= DATE_PART(EPOCH_SECOND, REQ."POINT_IN_TIME") + 604800
          )
          AND REQ."serving_cust_id" = SOURCE_TABLE."cust_id"
          AND REQ."other_serving_key" = SOURCE_TABLE."other_key"
        GROUP BY
          REQ."POINT_IN_TIME",
          REQ."serving_cust_id",
          REQ."other_serving_key",
          SOURCE_TABLE."col_float"
      )
    )
    WHERE
      "__fb_object_agg_row_number" <= 50000
  ) AS INNER_
  GROUP BY
    INNER_."POINT_IN_TIME",
    INNER_."serving_cust_id",
    INNER_."other_serving_key"
) AS T0
  ON REQ."POINT_IN_TIME" = T0."POINT_IN_TIME"
  AND REQ."serving_cust_id" = T0."serving_cust_id"
  AND REQ."other_serving_key" = T0."other_serving_key"
