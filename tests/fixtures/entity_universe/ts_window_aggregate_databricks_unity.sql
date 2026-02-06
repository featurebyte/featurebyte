SELECT DISTINCT
  CAST(`store_id` AS BIGINT) AS `cust_id`
FROM (
  SELECT
    `store_id` AS `store_id`
  FROM `sf_database`.`sf_schema`.`time_series_table`
  WHERE
    (
      `date` >= DATE_FORMAT(
        CAST(DATEADD(MONTH, -1, DATEADD(MONTH, -3, __fb_current_feature_timestamp)) AS TIMESTAMP),
        'yyyy-MM-DD HH24:MI:SS'
      )
      AND `date` <= DATE_FORMAT(
        CAST(DATEADD(MONTH, 1, __fb_current_feature_timestamp) AS TIMESTAMP),
        'yyyy-MM-DD HH24:MI:SS'
      )
    )
    AND (
      TO_TIMESTAMP(`date`, 'yyyy-MM-DD HH24:MI:SS') >= DATE_ADD(MONTH, -3, DATE_TRUNC('MONTH', `__fb_current_feature_timestamp`))
      AND TO_TIMESTAMP(`date`, 'yyyy-MM-DD HH24:MI:SS') < DATE_TRUNC('MONTH', `__fb_current_feature_timestamp`)
    )
)
WHERE
  NOT `store_id` IS NULL
