`partition_col` >= DATE_FORMAT(CAST('2023-02-01 00:00:00' AS TIMESTAMP), 'yyyy-MM-dd')
AND `partition_col` <= DATE_FORMAT(CAST('2023-05-01 00:00:00' AS TIMESTAMP), 'yyyy-MM-dd')
