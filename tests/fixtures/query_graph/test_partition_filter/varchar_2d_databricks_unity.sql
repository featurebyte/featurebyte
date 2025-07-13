`partition_col` >= DATE_FORMAT(CAST('2023-01-30 00:00:00' AS TIMESTAMP), 'yyyy-MM-dd')
AND `partition_col` <= DATE_FORMAT(CAST('2023-05-03 00:00:00' AS TIMESTAMP), 'yyyy-MM-dd')
