`partition_col` >= DATE_FORMAT(CAST('2022-12-01 00:00:00' AS TIMESTAMP), 'yyyy-MM-dd')
AND `partition_col` <= DATE_FORMAT(CAST('2023-07-01 00:00:00' AS TIMESTAMP), 'yyyy-MM-dd')
