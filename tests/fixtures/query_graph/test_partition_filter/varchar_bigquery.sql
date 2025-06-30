`partition_col` >= FORMAT_DATETIME('yyyy-MM-dd', CAST('2023-02-01 00:00:00' AS DATETIME))
AND `partition_col` <= FORMAT_DATETIME('yyyy-MM-dd', CAST('2023-05-01 00:00:00' AS DATETIME))
