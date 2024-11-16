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
    "T0"."_fb_internal_CUSTOMER_ID_window_w172800_avg_13c45b8622761dd28afb4640ac3ed355d57d789f" AS "_fb_internal_CUSTOMER_ID_window_w172800_avg_13c45b8622761dd28afb4640ac3ed355d57d789f"
  FROM ONLINE_REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      "CUSTOMER_ID" AS "CUSTOMER_ID",
      "_fb_internal_CUSTOMER_ID_window_w7200_avg_13c45b8622761dd28afb4640ac3ed355d57d789f" AS "_fb_internal_CUSTOMER_ID_window_w7200_avg_13c45b8622761dd28afb4640ac3ed355d57d789f",
      "_fb_internal_CUSTOMER_ID_window_w172800_avg_13c45b8622761dd28afb4640ac3ed355d57d789f" AS "_fb_internal_CUSTOMER_ID_window_w172800_avg_13c45b8622761dd28afb4640ac3ed355d57d789f"
    FROM (
      SELECT
        "CUSTOMER_ID",
        "'_fb_internal_CUSTOMER_ID_window_w7200_avg_13c45b8622761dd28afb4640ac3ed355d57d789f'" AS "_fb_internal_CUSTOMER_ID_window_w7200_avg_13c45b8622761dd28afb4640ac3ed355d57d789f",
        "'_fb_internal_CUSTOMER_ID_window_w172800_avg_13c45b8622761dd28afb4640ac3ed355d57d789f'" AS "_fb_internal_CUSTOMER_ID_window_w172800_avg_13c45b8622761dd28afb4640ac3ed355d57d789f"
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
              ),
              (
                '_fb_internal_CUSTOMER_ID_window_w172800_avg_13c45b8622761dd28afb4640ac3ed355d57d789f',
                _fb_internal_CUSTOMER_ID_window_w172800_avg_13c45b8622761dd28afb4640ac3ed355d57d789f_VERSION_PLACEHOLDER
              )) AS version_table("AGGREGATION_RESULT_NAME", "LATEST_VERSION")
          ) AS L
          INNER JOIN ONLINE_STORE_B3BAD6F0A450E950306704A0EF7BD384756A05CC AS R
            ON R."AGGREGATION_RESULT_NAME" = L."AGGREGATION_RESULT_NAME"
            AND R."VERSION" = L."LATEST_VERSION"
        )
        WHERE
          "AGGREGATION_RESULT_NAME" IN ('_fb_internal_CUSTOMER_ID_window_w7200_avg_13c45b8622761dd28afb4640ac3ed355d57d789f', '_fb_internal_CUSTOMER_ID_window_w172800_avg_13c45b8622761dd28afb4640ac3ed355d57d789f')
      )
      PIVOT(MAX("VALUE") FOR "AGGREGATION_RESULT_NAME" IN ('_fb_internal_CUSTOMER_ID_window_w7200_avg_13c45b8622761dd28afb4640ac3ed355d57d789f', '_fb_internal_CUSTOMER_ID_window_w172800_avg_13c45b8622761dd28afb4640ac3ed355d57d789f'))
    )
  ) AS T0
    ON REQ."CUSTOMER_ID" = T0."CUSTOMER_ID"
)
SELECT
  AGG."CUSTOMER_ID",
  CAST("_fb_internal_CUSTOMER_ID_window_w7200_avg_13c45b8622761dd28afb4640ac3ed355d57d789f" AS DOUBLE) AS "a_2h_average",
  CAST((
    "_fb_internal_CUSTOMER_ID_window_w172800_avg_13c45b8622761dd28afb4640ac3ed355d57d789f" + 123
  ) AS DOUBLE) AS "a_48h_average plus 123"
FROM _FB_AGGREGATED AS AGG
