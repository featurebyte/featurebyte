WITH "REQUEST_TABLE_W1800_F1800_BS600_M300_NEW_CUST_ID" AS (
  SELECT
    "POINT_IN_TIME",
    "NEW_CUST_ID",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 300
    ) / 1800) AS "__FB_LAST_TILE_INDEX",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 300
    ) / 1800) - 1 AS "__FB_FIRST_TILE_INDEX"
  FROM (
    SELECT DISTINCT
      "POINT_IN_TIME",
      "NEW_CUST_ID"
    FROM REQUEST_TABLE
  )
), "REQUEST_TABLE_W7200_F1800_BS600_M300_NEW_CUST_ID" AS (
  SELECT
    "POINT_IN_TIME",
    "NEW_CUST_ID",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 300
    ) / 1800) AS "__FB_LAST_TILE_INDEX",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 300
    ) / 1800) - 4 AS "__FB_FIRST_TILE_INDEX"
  FROM (
    SELECT DISTINCT
      "POINT_IN_TIME",
      "NEW_CUST_ID"
    FROM REQUEST_TABLE
  )
), "REQUEST_TABLE_W86400_F1800_BS600_M300_NEW_CUST_ID" AS (
  SELECT
    "POINT_IN_TIME",
    "NEW_CUST_ID",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 300
    ) / 1800) AS "__FB_LAST_TILE_INDEX",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 300
    ) / 1800) - 48 AS "__FB_FIRST_TILE_INDEX"
  FROM (
    SELECT DISTINCT
      "POINT_IN_TIME",
      "NEW_CUST_ID"
    FROM REQUEST_TABLE
  )
), _FB_AGGREGATED AS (
  SELECT
    REQ."POINT_IN_TIME",
    REQ."NEW_CUST_ID",
    REQ."A",
    REQ."B",
    REQ."C",
    "T0"."_fb_internal_window_w1800_sum_aed233b0e8a6e1c1e0d5427b126b03c949609481" AS "_fb_internal_window_w1800_sum_aed233b0e8a6e1c1e0d5427b126b03c949609481",
    "T1"."_fb_internal_window_w7200_sum_aed233b0e8a6e1c1e0d5427b126b03c949609481" AS "_fb_internal_window_w7200_sum_aed233b0e8a6e1c1e0d5427b126b03c949609481",
    "T2"."_fb_internal_window_w86400_sum_aed233b0e8a6e1c1e0d5427b126b03c949609481" AS "_fb_internal_window_w86400_sum_aed233b0e8a6e1c1e0d5427b126b03c949609481"
  FROM REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      "POINT_IN_TIME",
      "NEW_CUST_ID",
      SUM(value_sum_aed233b0e8a6e1c1e0d5427b126b03c949609481) AS "_fb_internal_window_w1800_sum_aed233b0e8a6e1c1e0d5427b126b03c949609481"
    FROM (
      SELECT
        REQ."POINT_IN_TIME",
        REQ."NEW_CUST_ID",
        TILE.INDEX,
        TILE.value_sum_aed233b0e8a6e1c1e0d5427b126b03c949609481
      FROM "REQUEST_TABLE_W1800_F1800_BS600_M300_NEW_CUST_ID" AS REQ
      INNER JOIN TILE_F1800_M300_B600_B5CAF33CCFEDA76C257EC2CB7F66C4AD22009B0F AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 1) = FLOOR(TILE.INDEX / 1)
        AND REQ."NEW_CUST_ID" = TILE."cust_id"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
      UNION ALL
      SELECT
        REQ."POINT_IN_TIME",
        REQ."NEW_CUST_ID",
        TILE.INDEX,
        TILE.value_sum_aed233b0e8a6e1c1e0d5427b126b03c949609481
      FROM "REQUEST_TABLE_W1800_F1800_BS600_M300_NEW_CUST_ID" AS REQ
      INNER JOIN TILE_F1800_M300_B600_B5CAF33CCFEDA76C257EC2CB7F66C4AD22009B0F AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 1) - 1 = FLOOR(TILE.INDEX / 1)
        AND REQ."NEW_CUST_ID" = TILE."cust_id"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
    )
    GROUP BY
      "POINT_IN_TIME",
      "NEW_CUST_ID"
  ) AS T0
    ON REQ."POINT_IN_TIME" = T0."POINT_IN_TIME" AND REQ."NEW_CUST_ID" = T0."NEW_CUST_ID"
  LEFT JOIN (
    SELECT
      "POINT_IN_TIME",
      "NEW_CUST_ID",
      SUM(value_sum_aed233b0e8a6e1c1e0d5427b126b03c949609481) AS "_fb_internal_window_w7200_sum_aed233b0e8a6e1c1e0d5427b126b03c949609481"
    FROM (
      SELECT
        REQ."POINT_IN_TIME",
        REQ."NEW_CUST_ID",
        TILE.INDEX,
        TILE.value_sum_aed233b0e8a6e1c1e0d5427b126b03c949609481
      FROM "REQUEST_TABLE_W7200_F1800_BS600_M300_NEW_CUST_ID" AS REQ
      INNER JOIN TILE_F1800_M300_B600_B5CAF33CCFEDA76C257EC2CB7F66C4AD22009B0F AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 4) = FLOOR(TILE.INDEX / 4)
        AND REQ."NEW_CUST_ID" = TILE."cust_id"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
      UNION ALL
      SELECT
        REQ."POINT_IN_TIME",
        REQ."NEW_CUST_ID",
        TILE.INDEX,
        TILE.value_sum_aed233b0e8a6e1c1e0d5427b126b03c949609481
      FROM "REQUEST_TABLE_W7200_F1800_BS600_M300_NEW_CUST_ID" AS REQ
      INNER JOIN TILE_F1800_M300_B600_B5CAF33CCFEDA76C257EC2CB7F66C4AD22009B0F AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 4) - 1 = FLOOR(TILE.INDEX / 4)
        AND REQ."NEW_CUST_ID" = TILE."cust_id"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
    )
    GROUP BY
      "POINT_IN_TIME",
      "NEW_CUST_ID"
  ) AS T1
    ON REQ."POINT_IN_TIME" = T1."POINT_IN_TIME" AND REQ."NEW_CUST_ID" = T1."NEW_CUST_ID"
  LEFT JOIN (
    SELECT
      "POINT_IN_TIME",
      "NEW_CUST_ID",
      SUM(value_sum_aed233b0e8a6e1c1e0d5427b126b03c949609481) AS "_fb_internal_window_w86400_sum_aed233b0e8a6e1c1e0d5427b126b03c949609481"
    FROM (
      SELECT
        REQ."POINT_IN_TIME",
        REQ."NEW_CUST_ID",
        TILE.INDEX,
        TILE.value_sum_aed233b0e8a6e1c1e0d5427b126b03c949609481
      FROM "REQUEST_TABLE_W86400_F1800_BS600_M300_NEW_CUST_ID" AS REQ
      INNER JOIN TILE_F1800_M300_B600_B5CAF33CCFEDA76C257EC2CB7F66C4AD22009B0F AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 48) = FLOOR(TILE.INDEX / 48)
        AND REQ."NEW_CUST_ID" = TILE."cust_id"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
      UNION ALL
      SELECT
        REQ."POINT_IN_TIME",
        REQ."NEW_CUST_ID",
        TILE.INDEX,
        TILE.value_sum_aed233b0e8a6e1c1e0d5427b126b03c949609481
      FROM "REQUEST_TABLE_W86400_F1800_BS600_M300_NEW_CUST_ID" AS REQ
      INNER JOIN TILE_F1800_M300_B600_B5CAF33CCFEDA76C257EC2CB7F66C4AD22009B0F AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 48) - 1 = FLOOR(TILE.INDEX / 48)
        AND REQ."NEW_CUST_ID" = TILE."cust_id"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
    )
    GROUP BY
      "POINT_IN_TIME",
      "NEW_CUST_ID"
  ) AS T2
    ON REQ."POINT_IN_TIME" = T2."POINT_IN_TIME" AND REQ."NEW_CUST_ID" = T2."NEW_CUST_ID"
)
SELECT
  AGG."POINT_IN_TIME",
  AGG."NEW_CUST_ID",
  AGG."A",
  AGG."B",
  AGG."C",
  "_fb_internal_window_w86400_sum_aed233b0e8a6e1c1e0d5427b126b03c949609481" AS "sum_1d"
FROM _FB_AGGREGATED AS AGG
