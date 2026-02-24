WITH DEPLOYMENT_REQUEST_TABLE AS (
  SELECT
    REQ.`cust_id`,
    {{ CURRENT_TIMESTAMP }} AS POINT_IN_TIME
  FROM (
    SELECT DISTINCT
      CAST(`cust_id` AS BIGINT) AS `cust_id`
    FROM (
      SELECT
        `cust_id` AS `cust_id`
      FROM `sf_database`.`sf_schema`.`sf_table`
      WHERE
        `event_timestamp` >= CAST(FLOOR((
          UNIX_TIMESTAMP(CAST({{ CURRENT_TIMESTAMP }} AS TIMESTAMP)) - 300
        ) / 1800) * 1800 + 300 - 600 - 86400 AS TIMESTAMP)
        AND `event_timestamp` < CAST(FLOOR((
          UNIX_TIMESTAMP(CAST({{ CURRENT_TIMESTAMP }} AS TIMESTAMP)) - 300
        ) / 1800) * 1800 + 300 - 600 AS TIMESTAMP)
    )
    WHERE
      NOT `cust_id` IS NULL
  ) AS REQ
), _FB_AGGREGATED AS (
  SELECT
    REQ.`cust_id`,
    REQ.`POINT_IN_TIME`,
    `T0`.`_fb_internal_cust_id_window_w1800_count_e707f2276dfd37d2ca45a7ffc8ca6569d4c8a2a0` AS `_fb_internal_cust_id_window_w1800_count_e707f2276dfd37d2ca45a7ffc8ca6569d4c8a2a0`,
    `T1`.`_fb_internal_cust_id_window_w86400_count_e707f2276dfd37d2ca45a7ffc8ca6569d4c8a2a0` AS `_fb_internal_cust_id_window_w86400_count_e707f2276dfd37d2ca45a7ffc8ca6569d4c8a2a0`
  FROM DEPLOYMENT_REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      INNER_.`cust_id`,
      OBJECT_AGG(
        CASE
          WHEN INNER_.`col_int` IS NULL
          THEN '__MISSING__'
          ELSE CAST(INNER_.`col_int` AS STRING)
        END,
        TO_VARIANT(
          INNER_.`_fb_internal_cust_id_window_w1800_count_e707f2276dfd37d2ca45a7ffc8ca6569d4c8a2a0_inner`
        )
      ) AS `_fb_internal_cust_id_window_w1800_count_e707f2276dfd37d2ca45a7ffc8ca6569d4c8a2a0`
    FROM (
      SELECT
        `cust_id`,
        `col_int`,
        `_fb_internal_cust_id_window_w1800_count_e707f2276dfd37d2ca45a7ffc8ca6569d4c8a2a0_inner`
      FROM (
        SELECT
          `cust_id`,
          `col_int`,
          `_fb_internal_cust_id_window_w1800_count_e707f2276dfd37d2ca45a7ffc8ca6569d4c8a2a0_inner`,
          ROW_NUMBER() OVER (PARTITION BY `cust_id` ORDER BY `_fb_internal_cust_id_window_w1800_count_e707f2276dfd37d2ca45a7ffc8ca6569d4c8a2a0_inner` DESC) AS `__fb_object_agg_row_number`
        FROM (
          SELECT
            `cust_id` AS `cust_id`,
            `col_int` AS `col_int`,
            COUNT(*) AS `_fb_internal_cust_id_window_w1800_count_e707f2276dfd37d2ca45a7ffc8ca6569d4c8a2a0_inner`
          FROM (
            SELECT
              `col_int` AS `col_int`,
              `col_float` AS `col_float`,
              `col_char` AS `col_char`,
              `col_text` AS `col_text`,
              `col_binary` AS `col_binary`,
              `col_boolean` AS `col_boolean`,
              `event_timestamp` AS `event_timestamp`,
              `cust_id` AS `cust_id`
            FROM `sf_database`.`sf_schema`.`sf_table`
            WHERE
              `event_timestamp` >= CAST(DATE_PART(EPOCH_SECOND, {{ CURRENT_TIMESTAMP }}) - 600 - 1800 AS TIMESTAMP)
              AND `event_timestamp` < CAST(DATE_PART(EPOCH_SECOND, {{ CURRENT_TIMESTAMP }}) - 600 AS TIMESTAMP)
          )
          GROUP BY
            `cust_id`,
            `col_int`
        )
      )
      WHERE
        `__fb_object_agg_row_number` <= 500
    ) AS INNER_
    GROUP BY
      INNER_.`cust_id`
  ) AS T0
    ON REQ.`cust_id` = T0.`cust_id`
  LEFT JOIN (
    SELECT
      INNER_.`cust_id`,
      OBJECT_AGG(
        CASE
          WHEN INNER_.`col_int` IS NULL
          THEN '__MISSING__'
          ELSE CAST(INNER_.`col_int` AS STRING)
        END,
        TO_VARIANT(
          INNER_.`_fb_internal_cust_id_window_w86400_count_e707f2276dfd37d2ca45a7ffc8ca6569d4c8a2a0_inner`
        )
      ) AS `_fb_internal_cust_id_window_w86400_count_e707f2276dfd37d2ca45a7ffc8ca6569d4c8a2a0`
    FROM (
      SELECT
        `cust_id`,
        `col_int`,
        `_fb_internal_cust_id_window_w86400_count_e707f2276dfd37d2ca45a7ffc8ca6569d4c8a2a0_inner`
      FROM (
        SELECT
          `cust_id`,
          `col_int`,
          `_fb_internal_cust_id_window_w86400_count_e707f2276dfd37d2ca45a7ffc8ca6569d4c8a2a0_inner`,
          ROW_NUMBER() OVER (PARTITION BY `cust_id` ORDER BY `_fb_internal_cust_id_window_w86400_count_e707f2276dfd37d2ca45a7ffc8ca6569d4c8a2a0_inner` DESC) AS `__fb_object_agg_row_number`
        FROM (
          SELECT
            `cust_id` AS `cust_id`,
            `col_int` AS `col_int`,
            COUNT(*) AS `_fb_internal_cust_id_window_w86400_count_e707f2276dfd37d2ca45a7ffc8ca6569d4c8a2a0_inner`
          FROM (
            SELECT
              `col_int` AS `col_int`,
              `col_float` AS `col_float`,
              `col_char` AS `col_char`,
              `col_text` AS `col_text`,
              `col_binary` AS `col_binary`,
              `col_boolean` AS `col_boolean`,
              `event_timestamp` AS `event_timestamp`,
              `cust_id` AS `cust_id`
            FROM `sf_database`.`sf_schema`.`sf_table`
            WHERE
              `event_timestamp` >= CAST(DATE_PART(EPOCH_SECOND, {{ CURRENT_TIMESTAMP }}) - 600 - 86400 AS TIMESTAMP)
              AND `event_timestamp` < CAST(DATE_PART(EPOCH_SECOND, {{ CURRENT_TIMESTAMP }}) - 600 AS TIMESTAMP)
          )
          GROUP BY
            `cust_id`,
            `col_int`
        )
      )
      WHERE
        `__fb_object_agg_row_number` <= 500
    ) AS INNER_
    GROUP BY
      INNER_.`cust_id`
  ) AS T1
    ON REQ.`cust_id` = T1.`cust_id`
)
SELECT
  AGG.`cust_id`,
  CAST((
    F_COUNT_DICT_COSINE_SIMILARITY(
      `_fb_internal_cust_id_window_w1800_count_e707f2276dfd37d2ca45a7ffc8ca6569d4c8a2a0`,
      `_fb_internal_cust_id_window_w86400_count_e707f2276dfd37d2ca45a7ffc8ca6569d4c8a2a0`
    )
  ) AS DOUBLE) AS `cosine_similarity_30m_vs_1d`,
  {{ CURRENT_TIMESTAMP }} AS `POINT_IN_TIME`
FROM _FB_AGGREGATED AS AGG
