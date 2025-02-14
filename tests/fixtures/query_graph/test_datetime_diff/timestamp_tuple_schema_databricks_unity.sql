(
  DATEDIFF(
    MICROSECOND,
    TO_TIMESTAMP(zipped_column_2.timestamp, 'yyyy-MM-dd\'T\'HH:mm:ss\'Z\''),
    TO_TIMESTAMP(zipped_column_1.timestamp, 'yyyy-MM-dd\'T\'HH:mm:ss\'Z\'')
  ) * CAST(1 AS BIGINT) / CAST(1000000 AS BIGINT)
)
