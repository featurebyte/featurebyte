SELECT DISTINCT
  CAST(`store_id` AS INT64) AS `cust_id`
FROM (
  SELECT
    `store_id` AS `store_id`
  FROM `sf_database`.`sf_schema`.`time_series_table`
  WHERE
    (
      `date` >= FORMAT_DATETIME(
        'YYYY-MM-DD HH24:MI:SS',
        DATETIME_ADD(CAST(DATETIME_ADD(CAST(__fb_current_feature_timestamp AS DATETIME), INTERVAL (
          CAST(-3 AS INT64)
        ) MONTH) AS DATETIME), INTERVAL (
          CAST(-1 AS INT64)
        ) MONTH)
      )
      AND `date` <= FORMAT_DATETIME(
        'YYYY-MM-DD HH24:MI:SS',
        DATETIME_ADD(CAST(__fb_current_feature_timestamp AS DATETIME), INTERVAL (
          CAST(1 AS INT64)
        ) MONTH)
      )
    )
    AND (
      CAST(CAST(PARSE_TIMESTAMP('YYYY-MM-DD HH24:MI:SS', `date`) AS DATETIME) AS DATETIME) >= DATETIME_SUB(CAST(TIMESTAMP_TRUNC(`__fb_current_feature_timestamp`, MONTH) AS DATETIME), INTERVAL '3' MONTH)
      AND CAST(CAST(PARSE_TIMESTAMP('YYYY-MM-DD HH24:MI:SS', `date`) AS DATETIME) AS DATETIME) < TIMESTAMP_TRUNC(`__fb_current_feature_timestamp`, MONTH)
    )
)
WHERE
  NOT `store_id` IS NULL
