WITH REQUEST_TABLE AS (
  SELECT
    CAST('2022-04-20 10:00:00' AS TIMESTAMP) AS `POINT_IN_TIME`,
    'C1' AS `CUSTOMER_ID`
), __FB_ENTITY_TABLE_NAME AS (
  (
    SELECT
      `CUSTOMER_ID` AS `cust_id`,
      CAST(FLOOR((
        UNIX_TIMESTAMP(MAX(POINT_IN_TIME)) - 1800
      ) / 3600) * 3600 + 1800 - 900 AS TIMESTAMP) AS __FB_ENTITY_TABLE_END_DATE,
      CAST(CAST(CAST(CAST(FLOOR((
        UNIX_TIMESTAMP(MIN(POINT_IN_TIME)) - 1800
      ) / 3600) * 3600 + 1800 - 900 AS TIMESTAMP) AS TIMESTAMP) AS DOUBLE) + (
        48 * 3600 * CAST(1000000 AS BIGINT) / CAST(1 AS BIGINT)
      ) * -1 / 1000000.0 AS TIMESTAMP) AS __FB_ENTITY_TABLE_START_DATE
    FROM `REQUEST_TABLE`
    GROUP BY
      `CUSTOMER_ID`
  )
), TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS (
  SELECT
    avg_f777ffee22ceceebb438d61cb44de17eb593fd38.INDEX,
    avg_f777ffee22ceceebb438d61cb44de17eb593fd38.`cust_id`,
    sum_value_avg_f777ffee22ceceebb438d61cb44de17eb593fd38,
    count_value_avg_f777ffee22ceceebb438d61cb44de17eb593fd38
  FROM (
    SELECT
      index,
      `cust_id`,
      SUM(`a`) AS sum_value_avg_f777ffee22ceceebb438d61cb44de17eb593fd38,
      COUNT(`a`) AS count_value_avg_f777ffee22ceceebb438d61cb44de17eb593fd38
    FROM (
      SELECT
        *,
        F_TIMESTAMP_TO_INDEX(CAST(`ts` AS TIMESTAMP), 1800, 900, 60) AS index
      FROM (
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
  ) AS avg_f777ffee22ceceebb438d61cb44de17eb593fd38
), `REQUEST_TABLE_W7200_F3600_BS900_M1800_CUSTOMER_ID` AS (
  SELECT
    `POINT_IN_TIME`,
    `CUSTOMER_ID`,
    FLOOR((
      UNIX_TIMESTAMP(`POINT_IN_TIME`) - 1800
    ) / 3600) AS __FB_LAST_TILE_INDEX,
    FLOOR((
      UNIX_TIMESTAMP(`POINT_IN_TIME`) - 1800
    ) / 3600) - 2 AS __FB_FIRST_TILE_INDEX
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
    ) / 3600) AS __FB_LAST_TILE_INDEX,
    FLOOR((
      UNIX_TIMESTAMP(`POINT_IN_TIME`) - 1800
    ) / 3600) - 48 AS __FB_FIRST_TILE_INDEX
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
    `T0`.`_fb_internal_CUSTOMER_ID_window_w7200_avg_f777ffee22ceceebb438d61cb44de17eb593fd38` AS `_fb_internal_CUSTOMER_ID_window_w7200_avg_f777ffee22ceceebb438d61cb44de17eb593fd38`,
    `T1`.`_fb_internal_CUSTOMER_ID_window_w172800_avg_f777ffee22ceceebb438d61cb44de17eb593fd38` AS `_fb_internal_CUSTOMER_ID_window_w172800_avg_f777ffee22ceceebb438d61cb44de17eb593fd38`
  FROM REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      `POINT_IN_TIME`,
      `CUSTOMER_ID`,
      SUM(sum_value_avg_f777ffee22ceceebb438d61cb44de17eb593fd38) / SUM(count_value_avg_f777ffee22ceceebb438d61cb44de17eb593fd38) AS `_fb_internal_CUSTOMER_ID_window_w7200_avg_f777ffee22ceceebb438d61cb44de17eb593fd38`
    FROM (
      SELECT
        REQ.`POINT_IN_TIME`,
        REQ.`CUSTOMER_ID`,
        TILE.INDEX,
        TILE.count_value_avg_f777ffee22ceceebb438d61cb44de17eb593fd38,
        TILE.sum_value_avg_f777ffee22ceceebb438d61cb44de17eb593fd38
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
        TILE.count_value_avg_f777ffee22ceceebb438d61cb44de17eb593fd38,
        TILE.sum_value_avg_f777ffee22ceceebb438d61cb44de17eb593fd38
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
      SUM(sum_value_avg_f777ffee22ceceebb438d61cb44de17eb593fd38) / SUM(count_value_avg_f777ffee22ceceebb438d61cb44de17eb593fd38) AS `_fb_internal_CUSTOMER_ID_window_w172800_avg_f777ffee22ceceebb438d61cb44de17eb593fd38`
    FROM (
      SELECT
        REQ.`POINT_IN_TIME`,
        REQ.`CUSTOMER_ID`,
        TILE.INDEX,
        TILE.count_value_avg_f777ffee22ceceebb438d61cb44de17eb593fd38,
        TILE.sum_value_avg_f777ffee22ceceebb438d61cb44de17eb593fd38
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
        TILE.count_value_avg_f777ffee22ceceebb438d61cb44de17eb593fd38,
        TILE.sum_value_avg_f777ffee22ceceebb438d61cb44de17eb593fd38
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
  CAST(`_fb_internal_CUSTOMER_ID_window_w7200_avg_f777ffee22ceceebb438d61cb44de17eb593fd38` AS DOUBLE) AS `a_2h_average`,
  CAST(`_fb_internal_CUSTOMER_ID_window_w172800_avg_f777ffee22ceceebb438d61cb44de17eb593fd38` AS DOUBLE) AS `a_48h_average`
FROM _FB_AGGREGATED AS AGG
