SELECT DISTINCT
  CAST(`store_id` AS BIGINT) AS `cust_id`
FROM (
  SELECT
    `store_id` AS `store_id`
  FROM `sf_database`.`sf_schema`.`time_series_table`
  WHERE
    (
      `date` >= DATE_FORMAT(
        CAST(ADD_MONTHS(ADD_MONTHS(__fb_current_feature_timestamp, -3), -1) AS TIMESTAMP),
        'yyyy-MM-DD HH24:MI:SS'
      )
      AND `date` <= DATE_FORMAT(
        CAST(ADD_MONTHS(__fb_current_feature_timestamp, 1) AS TIMESTAMP),
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
