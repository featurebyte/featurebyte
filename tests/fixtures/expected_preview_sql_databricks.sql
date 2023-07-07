WITH REQUEST_TABLE AS (
  SELECT
    CAST('2022-04-20 10:00:00' AS TIMESTAMP) AS `POINT_IN_TIME`,
    'C1' AS `CUSTOMER_ID`
), TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS (
  SELECT
    avg_8b1b0ac73d607b2ad9cd1bd83be10878d3137e5b.INDEX,
    avg_8b1b0ac73d607b2ad9cd1bd83be10878d3137e5b.`cust_id`,
    sum_value_avg_8b1b0ac73d607b2ad9cd1bd83be10878d3137e5b,
    count_value_avg_8b1b0ac73d607b2ad9cd1bd83be10878d3137e5b
  FROM (
    SELECT
      index,
      `cust_id`,
      SUM(`a`) AS sum_value_avg_8b1b0ac73d607b2ad9cd1bd83be10878d3137e5b,
      COUNT(`a`) AS count_value_avg_8b1b0ac73d607b2ad9cd1bd83be10878d3137e5b
    FROM (
      SELECT
        *,
        F_TIMESTAMP_TO_INDEX(`ts`, 1800, 900, 60) AS index
      FROM (
        WITH __FB_ENTITY_TABLE_NAME AS (
          (
            SELECT
              `CUSTOMER_ID` AS `cust_id`,
              TO_TIMESTAMP(
                FLOOR((
                  UNIX_TIMESTAMP(MAX(POINT_IN_TIME)) - 1800
                ) / 3600) * 3600 + 1800 - 900
              ) AS `__FB_ENTITY_TABLE_END_DATE`,
              TO_TIMESTAMP(
                FROM_UNIXTIME(
                  CAST(TO_TIMESTAMP(
                    FLOOR((
                      UNIX_TIMESTAMP(MIN(POINT_IN_TIME)) - 1800
                    ) / 3600) * 3600 + 1800 - 900
                  ) AS DOUBLE) + (
                    48 * 3600 * CAST(1000000 AS LONG) / CAST(1 AS LONG)
                  ) * -1 / 1000000.0
                )
              ) AS `__FB_ENTITY_TABLE_START_DATE`
            FROM `REQUEST_TABLE`
            GROUP BY
              `CUSTOMER_ID`
          )
        )
        SELECT
          R.*
        FROM __FB_ENTITY_TABLE_NAME
        INNER JOIN (
          SELECT
            `ts` AS `ts`,
            `cust_id` AS `cust_id`,
            `a` AS `a`,
            `b` AS `b`,
            (
              `a` + `b`
            ) AS `c`
          FROM `db`.`public`.`event_table`
        ) AS R
          ON R.`cust_id` = __FB_ENTITY_TABLE_NAME.`cust_id`
          AND R.`ts` >= __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_START_DATE
          AND R.`ts` < __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_END_DATE
      )
    )
    GROUP BY
      index,
      `cust_id`
  ) AS avg_8b1b0ac73d607b2ad9cd1bd83be10878d3137e5b
), `REQUEST_TABLE_W7200_F3600_BS900_M1800_CUSTOMER_ID` AS (
  SELECT
    `POINT_IN_TIME`,
    `CUSTOMER_ID`,
    FLOOR((
      UNIX_TIMESTAMP(`POINT_IN_TIME`) - 1800
    ) / 3600) AS `__FB_LAST_TILE_INDEX`,
    FLOOR((
      UNIX_TIMESTAMP(`POINT_IN_TIME`) - 1800
    ) / 3600) - 2 AS `__FB_FIRST_TILE_INDEX`
  FROM (
    SELECT DISTINCT
      `POINT_IN_TIME`,
      `CUSTOMER_ID`
    FROM REQUEST_TABLE
  )
), `REQUEST_TABLE_W172800_F3600_BS900_M1800_CUSTOMER_ID` AS (
  SELECT
    `POINT_IN_TIME`,
    `CUSTOMER_ID`,
    FLOOR((
      UNIX_TIMESTAMP(`POINT_IN_TIME`) - 1800
    ) / 3600) AS `__FB_LAST_TILE_INDEX`,
    FLOOR((
      UNIX_TIMESTAMP(`POINT_IN_TIME`) - 1800
    ) / 3600) - 48 AS `__FB_FIRST_TILE_INDEX`
  FROM (
    SELECT DISTINCT
      `POINT_IN_TIME`,
      `CUSTOMER_ID`
    FROM REQUEST_TABLE
  )
), _FB_AGGREGATED AS (
  SELECT
    REQ.`POINT_IN_TIME`,
    REQ.`CUSTOMER_ID`,
    `T0`.`_fb_internal_window_w7200_avg_8b1b0ac73d607b2ad9cd1bd83be10878d3137e5b` AS `_fb_internal_window_w7200_avg_8b1b0ac73d607b2ad9cd1bd83be10878d3137e5b`,
    `T1`.`_fb_internal_window_w172800_avg_8b1b0ac73d607b2ad9cd1bd83be10878d3137e5b` AS `_fb_internal_window_w172800_avg_8b1b0ac73d607b2ad9cd1bd83be10878d3137e5b`
  FROM REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      `POINT_IN_TIME`,
      `CUSTOMER_ID`,
      SUM(sum_value_avg_8b1b0ac73d607b2ad9cd1bd83be10878d3137e5b) / SUM(count_value_avg_8b1b0ac73d607b2ad9cd1bd83be10878d3137e5b) AS `_fb_internal_window_w7200_avg_8b1b0ac73d607b2ad9cd1bd83be10878d3137e5b`
    FROM (
      SELECT
        REQ.`POINT_IN_TIME`,
        REQ.`CUSTOMER_ID`,
        TILE.INDEX,
        TILE.count_value_avg_8b1b0ac73d607b2ad9cd1bd83be10878d3137e5b,
        TILE.sum_value_avg_8b1b0ac73d607b2ad9cd1bd83be10878d3137e5b
      FROM `REQUEST_TABLE_W7200_F3600_BS900_M1800_CUSTOMER_ID` AS REQ
      INNER JOIN TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 2) = FLOOR(TILE.INDEX / 2)
        AND REQ.`CUSTOMER_ID` = TILE.`cust_id`
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
      UNION ALL
      SELECT
        REQ.`POINT_IN_TIME`,
        REQ.`CUSTOMER_ID`,
        TILE.INDEX,
        TILE.count_value_avg_8b1b0ac73d607b2ad9cd1bd83be10878d3137e5b,
        TILE.sum_value_avg_8b1b0ac73d607b2ad9cd1bd83be10878d3137e5b
      FROM `REQUEST_TABLE_W7200_F3600_BS900_M1800_CUSTOMER_ID` AS REQ
      INNER JOIN TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 2) - 1 = FLOOR(TILE.INDEX / 2)
        AND REQ.`CUSTOMER_ID` = TILE.`cust_id`
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
    )
    GROUP BY
      `POINT_IN_TIME`,
      `CUSTOMER_ID`
  ) AS T0
    ON REQ.`POINT_IN_TIME` = T0.`POINT_IN_TIME` AND REQ.`CUSTOMER_ID` = T0.`CUSTOMER_ID`
  LEFT JOIN (
    SELECT
      `POINT_IN_TIME`,
      `CUSTOMER_ID`,
      SUM(sum_value_avg_8b1b0ac73d607b2ad9cd1bd83be10878d3137e5b) / SUM(count_value_avg_8b1b0ac73d607b2ad9cd1bd83be10878d3137e5b) AS `_fb_internal_window_w172800_avg_8b1b0ac73d607b2ad9cd1bd83be10878d3137e5b`
    FROM (
      SELECT
        REQ.`POINT_IN_TIME`,
        REQ.`CUSTOMER_ID`,
        TILE.INDEX,
        TILE.count_value_avg_8b1b0ac73d607b2ad9cd1bd83be10878d3137e5b,
        TILE.sum_value_avg_8b1b0ac73d607b2ad9cd1bd83be10878d3137e5b
      FROM `REQUEST_TABLE_W172800_F3600_BS900_M1800_CUSTOMER_ID` AS REQ
      INNER JOIN TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 48) = FLOOR(TILE.INDEX / 48)
        AND REQ.`CUSTOMER_ID` = TILE.`cust_id`
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
      UNION ALL
      SELECT
        REQ.`POINT_IN_TIME`,
        REQ.`CUSTOMER_ID`,
        TILE.INDEX,
        TILE.count_value_avg_8b1b0ac73d607b2ad9cd1bd83be10878d3137e5b,
        TILE.sum_value_avg_8b1b0ac73d607b2ad9cd1bd83be10878d3137e5b
      FROM `REQUEST_TABLE_W172800_F3600_BS900_M1800_CUSTOMER_ID` AS REQ
      INNER JOIN TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 48) - 1 = FLOOR(TILE.INDEX / 48)
        AND REQ.`CUSTOMER_ID` = TILE.`cust_id`
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
    )
    GROUP BY
      `POINT_IN_TIME`,
      `CUSTOMER_ID`
  ) AS T1
    ON REQ.`POINT_IN_TIME` = T1.`POINT_IN_TIME` AND REQ.`CUSTOMER_ID` = T1.`CUSTOMER_ID`
)
SELECT
  AGG.`POINT_IN_TIME`,
  AGG.`CUSTOMER_ID`,
  `_fb_internal_window_w7200_avg_8b1b0ac73d607b2ad9cd1bd83be10878d3137e5b` AS `a_2h_average`,
  `_fb_internal_window_w172800_avg_8b1b0ac73d607b2ad9cd1bd83be10878d3137e5b` AS `a_48h_average`
FROM _FB_AGGREGATED AS AGG
