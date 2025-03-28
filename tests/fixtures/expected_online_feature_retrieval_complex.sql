WITH ONLINE_REQUEST_TABLE AS (
  SELECT
    REQ."CUSTOMER_ID",
    SYSDATE() AS POINT_IN_TIME
  FROM "MY_REQUEST_TABLE" AS REQ
), _FB_AGGREGATED AS (
  SELECT
    REQ."CUSTOMER_ID",
    REQ."POINT_IN_TIME",
    "T0"."_fb_internal_CUSTOMER_ID_window_w7200_avg_13c45b8622761dd28afb4640ac3ed355d57d789f" AS "_fb_internal_CUSTOMER_ID_window_w7200_avg_13c45b8622761dd28afb4640ac3ed355d57d789f",
    "T1"."_fb_internal_BUSINESS_ID_window_w604800_sum_8c11e770ad5121aec588693662ac607b4fba0528" AS "_fb_internal_BUSINESS_ID_window_w604800_sum_8c11e770ad5121aec588693662ac607b4fba0528"
  FROM ONLINE_REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      "CUSTOMER_ID" AS "CUSTOMER_ID",
      "_fb_internal_CUSTOMER_ID_window_w7200_avg_13c45b8622761dd28afb4640ac3ed355d57d789f" AS "_fb_internal_CUSTOMER_ID_window_w7200_avg_13c45b8622761dd28afb4640ac3ed355d57d789f"
    FROM (
      SELECT
        "CUSTOMER_ID",
        "'_fb_internal_CUSTOMER_ID_window_w7200_avg_13c45b8622761dd28afb4640ac3ed355d57d789f'" AS "_fb_internal_CUSTOMER_ID_window_w7200_avg_13c45b8622761dd28afb4640ac3ed355d57d789f"
      FROM (
        SELECT
          "CUSTOMER_ID",
          "AGGREGATION_RESULT_NAME",
          "VALUE"
        FROM (
          SELECT
            R.*
          FROM (
            SELECT
              "AGGREGATION_RESULT_NAME",
              "LATEST_VERSION"
            FROM (VALUES
              (
                '_fb_internal_CUSTOMER_ID_window_w7200_avg_13c45b8622761dd28afb4640ac3ed355d57d789f',
                _fb_internal_CUSTOMER_ID_window_w7200_avg_13c45b8622761dd28afb4640ac3ed355d57d789f_VERSION_PLACEHOLDER
              )) AS version_table("AGGREGATION_RESULT_NAME", "LATEST_VERSION")
          ) AS L
          INNER JOIN ONLINE_STORE_B3BAD6F0A450E950306704A0EF7BD384756A05CC AS R
            ON R."AGGREGATION_RESULT_NAME" = L."AGGREGATION_RESULT_NAME"
            AND R."VERSION" = L."LATEST_VERSION"
        )
        WHERE
          "AGGREGATION_RESULT_NAME" IN ('_fb_internal_CUSTOMER_ID_window_w7200_avg_13c45b8622761dd28afb4640ac3ed355d57d789f')
      )
      PIVOT(MAX("VALUE") FOR "AGGREGATION_RESULT_NAME" IN ('_fb_internal_CUSTOMER_ID_window_w7200_avg_13c45b8622761dd28afb4640ac3ed355d57d789f'))
    )
  ) AS T0
    ON REQ."CUSTOMER_ID" = T0."CUSTOMER_ID"
  LEFT JOIN (
    SELECT
      "BUSINESS_ID" AS "BUSINESS_ID",
      "_fb_internal_BUSINESS_ID_window_w604800_sum_8c11e770ad5121aec588693662ac607b4fba0528" AS "_fb_internal_BUSINESS_ID_window_w604800_sum_8c11e770ad5121aec588693662ac607b4fba0528"
    FROM (
      SELECT
        "BUSINESS_ID",
        "'_fb_internal_BUSINESS_ID_window_w604800_sum_8c11e770ad5121aec588693662ac607b4fba0528'" AS "_fb_internal_BUSINESS_ID_window_w604800_sum_8c11e770ad5121aec588693662ac607b4fba0528"
      FROM (
        SELECT
          "BUSINESS_ID",
          "AGGREGATION_RESULT_NAME",
          "VALUE"
        FROM (
          SELECT
            R.*
          FROM (
            SELECT
              "AGGREGATION_RESULT_NAME",
              "LATEST_VERSION"
            FROM (VALUES
              (
                '_fb_internal_BUSINESS_ID_window_w604800_sum_8c11e770ad5121aec588693662ac607b4fba0528',
                _fb_internal_BUSINESS_ID_window_w604800_sum_8c11e770ad5121aec588693662ac607b4fba0528_VERSION_PLACEHOLDER
              )) AS version_table("AGGREGATION_RESULT_NAME", "LATEST_VERSION")
          ) AS L
          INNER JOIN ONLINE_STORE_51064268424BF868A2EA2DC2F5789E7CB4DF29BF AS R
            ON R."AGGREGATION_RESULT_NAME" = L."AGGREGATION_RESULT_NAME"
            AND R."VERSION" = L."LATEST_VERSION"
        )
        WHERE
          "AGGREGATION_RESULT_NAME" IN ('_fb_internal_BUSINESS_ID_window_w604800_sum_8c11e770ad5121aec588693662ac607b4fba0528')
      )
      PIVOT(MAX("VALUE") FOR "AGGREGATION_RESULT_NAME" IN ('_fb_internal_BUSINESS_ID_window_w604800_sum_8c11e770ad5121aec588693662ac607b4fba0528'))
    )
  ) AS T1
    ON REQ."BUSINESS_ID" = T1."BUSINESS_ID"
)
SELECT
  AGG."CUSTOMER_ID",
  CAST((
    "_fb_internal_CUSTOMER_ID_window_w7200_avg_13c45b8622761dd28afb4640ac3ed355d57d789f" / NULLIF(
      "_fb_internal_BUSINESS_ID_window_w604800_sum_8c11e770ad5121aec588693662ac607b4fba0528",
      0
    )
  ) AS DOUBLE) AS "a_2h_avg_by_user_div_7d_by_biz"
FROM _FB_AGGREGATED AS AGG
