(
  (
    CAST(CAST(TO_TIMESTAMP(GET_JSON_OBJECT(zipped_column_1, '$.timestamp'), 'yyyy-MM-dd\'T\'HH:mm:ss\'Z\'') AS TIMESTAMP) AS DOUBLE) * 1000000.0 - CAST(CAST(TO_TIMESTAMP(GET_JSON_OBJECT(zipped_column_2, '$.timestamp'), 'yyyy-MM-dd\'T\'HH:mm:ss\'Z\'') AS TIMESTAMP) AS DOUBLE) * 1000000.0
  ) * CAST(1 AS BIGINT) / CAST(1000000 AS BIGINT)
)
